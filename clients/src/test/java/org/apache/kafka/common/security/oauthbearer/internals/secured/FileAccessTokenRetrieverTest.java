package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;

import javax.security.auth.callback.Callback;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class FileAccessTokenRetrieverTest extends OAuthBearerTest {

    @Test
    public void testFileTokenRetrieverHandlesNewline() throws IOException {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        long cur = cal.getTimeInMillis() / 1000;
        String exp = "" + (cur + 60 * 60);  // 1 hour in future
        String iat = "" + cur;

        String expected = createAccessKey("{}", String.format("{\"exp\":%s, \"iat\":%s, \"sub\":\"subj\"}", exp, iat), "sign");
        String withNewline = expected + "\n";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", withNewline);

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, accessTokenFile.toURI().toString());

        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());
        FileAccessTokenRetriever accessTokenRetriever = new FileAccessTokenRetriever();

        try (OAuthBearerLoginCallbackHandler handler = createHandler(accessTokenRetriever, configs)) {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[]{callback});
            assertEquals(callback.token().value(), expected);
        } catch (Exception e) {
            fail(e);
        }
    }

}
