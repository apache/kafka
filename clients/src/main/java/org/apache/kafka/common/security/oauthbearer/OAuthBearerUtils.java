package org.apache.kafka.common.security.oauthbearer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.HttpsURLConnection;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerUtils {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerUtils.class);

    static String getTokenEndpoint(String issuerUri) throws IOException {
        String openIdConfigurationUrl = String.format("%s/.well-known/openid-configuration", issuerUri);
        JsonNode rootNode = get(openIdConfigurationUrl, null);
        return rootNode.at("/token_endpoint").textValue();
    }

    static String getAccessToken(String tokenEndpoint,
        String clientId,
        String clientSecret,
        String scope) throws IOException {
        Map<String, String> requestParameters = new HashMap<>(2);
        requestParameters.put("grant_type", "client_credentials");

        if (scope != null)
            requestParameters.put("scope", scope);

        log.warn("getAccessToken - requestParameters: {}", requestParameters);

        String authorizationHeader = formatAuthorizationHeader(clientId, clientSecret);
        log.warn("getAccessToken - authorizationHeader: {}", authorizationHeader);
        JsonNode rootNode = post(tokenEndpoint, authorizationHeader, requestParameters);
        return rootNode.at("/access_token").textValue();
    }

    static JsonNode get(String uri) throws IOException {
        return get(uri, null);
    }

    static JsonNode get(String uri, String authorizationHeader) throws IOException {
        return request(uri, "GET", authorizationHeader, null);
    }

    static JsonNode post(String uri, String authorizationHeader, Map<String, String> requestParameters) throws IOException {
        String requestBody = null;

        if (requestParameters != null && requestParameters.size() > 0)
            requestBody = getRequestBody(requestParameters);

        return request(uri, "POST", authorizationHeader, requestBody);
    }

    static JsonNode request(String uri,
        String requestMethod,
        String authorizationHeader,
        String requestBody)
        throws IOException {
        if (requestBody != null && requestBody.length() > 0)
            log.warn("request - requestBody: {}", requestBody);

        HttpURLConnection con = prepareRequest(uri, requestMethod, authorizationHeader, requestBody);
        String responseBody = getResponseBody(con);

        if (responseBody != null && responseBody.length() > 0) {
            log.warn("request - responseBody: {}", responseBody);
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(responseBody);
        } else {
            return null;
        }
    }

    static HttpURLConnection prepareRequest(String uri,
        String requestMethod,
        String authorizationHeader,
        String requestBody)
        throws IOException {
        HttpURLConnection con = (HttpURLConnection)new URL(uri).openConnection();
        con.setRequestMethod(requestMethod.toUpperCase());
        con.setRequestProperty("Accept", "application/json");
        con.setRequestProperty("Authorization", authorizationHeader);
        con.setRequestProperty("Cache-Control", "no-cache");
        con.setUseCaches(false);

        if (requestBody != null && requestBody.length() > 0) {
            con.setRequestProperty("Content-Length", String.valueOf(requestBody.length()));
            con.setDoOutput(true);
        }

        try {
            log.warn("prepareRequest - preparing to connect to {}", uri);
            con.connect();
        } catch (ConnectException e) {
            throw new IOException("Failed to connect to: " + uri, e);
        }

        if (requestBody != null && requestBody.length() > 0) {
            try (OutputStream os = con.getOutputStream()) {
                ByteArrayInputStream is = new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8));
                copy(is, os);
            }
        }

        return con;
    }

    static String getResponseBody(HttpURLConnection con) throws IOException {
        int responseCode = con.getResponseCode();
        log.warn("handleResponse - responseCode: {}", responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
            byte[] responseBody;

            try (InputStream is = con.getInputStream()) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                copy(is, os);
                responseBody = os.toByteArray();
            }

            return new String(responseBody, StandardCharsets.UTF_8);
        } else if (responseCode == HttpsURLConnection.HTTP_NO_CONTENT) {
            return null;
        } else {
            throw new IOException("Don't know how to handle response code " + responseCode);
        }
    }

    static String getRequestBody(Map<String, String> requestParameters) {
        String encoding = StandardCharsets.UTF_8.name();
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, String> entry : requestParameters.entrySet()) {
            if (sb.length() > 0)
                sb.append("&");

            String key = entry.getKey();
            String value;

            try {
                value = URLEncoder.encode(entry.getValue(), encoding);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(String.format("Encoding %s not supported", encoding));
            }

            sb.append(key).append("=").append(value);
        }

        return sb.toString();
    }

    static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int b;

        while ((b = is.read(buf)) != -1)
            os.write(buf, 0, b);
    }

    static String formatAuthorizationHeader(String clientId, String clientSecret) {
        String base64Encoded = base64Encode(String.format("%s:%s", clientId, clientSecret));
        return String.format("Basic %s", base64Encoded);
    }

    static String base64Encode(String s) {
        return Base64.getUrlEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static String base64Decode(String s) {
        return new String(Base64.getUrlDecoder().decode(s), StandardCharsets.UTF_8);
    }

    static BasicOAuthBearerToken parseAndValidateToken(String jwt) {
        if (jwt.endsWith("."))
            jwt += ".";

        String[] splits = jwt.split("\\.");
        log.warn("handle - splits length: {}", splits.length);

        Map<String, Object> payload = OAuthBearerUnsecuredJws.toMap(splits[1]);

        Set<String> scopes = new HashSet<>();

        if (payload.containsKey("scp")) {
            @SuppressWarnings("unchecked")
            Collection<String> scopeList = (Collection<String>)payload.get("scp");
            scopes.addAll(scopeList);
        }

        Long lifetimeMs = null;

        if (payload.containsKey("exp")) {
            Number expiration = (Number)payload.get("exp");
            lifetimeMs = expiration.longValue() * 1000;
        }

        String principalName = null;

        if (payload.containsKey("sub")) {
            principalName = (String)payload.get("sub");
        }

        Long startTimeMs = null;

        if (payload.containsKey("iat")) {
            Number expiration = (Number)payload.get("iat");
            startTimeMs = expiration.longValue() * 1000;
        }

        return new BasicOAuthBearerToken(jwt, scopes, lifetimeMs, principalName, startTimeMs);
    }

}
