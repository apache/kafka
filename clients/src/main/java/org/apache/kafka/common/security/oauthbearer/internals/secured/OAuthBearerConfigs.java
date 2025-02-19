package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * Additional configurations to extend the available ones within {@link SaslConfigs}.
 */
public class OAuthBearerConfigs {

    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPE = "sasl.oauthbearer.token.endpoint.grant.type";
    public static final List<String> SUPPORTED_SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPES = List.of(
        "client_credentials",
        "urn:ietf:params:oauth:grant-type:jwt-bearer"
    );
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPE_DOC = "The grant type used when sending the JWT token to the token endpoint. "
        + "This should be set explicitly to determine which token retriever to use. The supported values are "
        + SUPPORTED_SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPES.toString();

    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_SCOPE = "sasl.oauthbearer.token.endpoint.scope";
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_SCOPE_DOC = "The scope used when sending the JWT token to the token endpoint. "
        + "This is only used when the grant type is 'client_credentials'.";

    // Configs for grant_type = client_credentials
    public static final String SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_ID = "sasl.oauthbearer.token.endpoint.client.id";
    public static final String SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_ID_DOC = "The client ID used to authenticate with the token endpoint when using the "
        + "'client_credentials' grant type.";

    public static final String SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_SECRET = "sasl.oauthbearer.token.endpoint.client.secret";
    public static final String SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_SECRET_DOC = "The client secret used to authenticate with the token endpoint when using the "
        + "'client_credentials' grant type.";

    // Configs for grant_type = urn:ietf:params:oauth:grant-type:jwt-bearer
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO = "sasl.oauthbearer.token.endpoint.signing.algo"; // Default: "RS256";
    public static final String DEFAULT_SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO = "RS256";
    public static final List<String> SUPPORTED_SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGOS = List.of(
        "RS256",
        "ES256"
    );
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO_DOC = "The algorithm used to sign the JWT token sent to the token endpoint. "
        + "The default is RS256, and the supported values are " + SUPPORTED_SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGOS.toString();

    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_ID = "sasl.oauthbearer.token.endpoint.private.key.id";
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_ID_DOC = "The private key ID of the private key used to sign the JWT token sent to the token endpoint. "
        + "This will be added as a header in the JWT token sent to the token endpoint.";

    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_SECRET = "sasl.oauthbearer.token.endpoint.private.key.secret";
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_SECRET_DOC = "The private key used to sign the JWT token sent to the token endpoint. "
        + "This must me in PEM format with the header and footer discluded.";

    public static final String SASL_OAUTHBEARER_TOKEN_SUBJECT = "sasl.oauthbearer.token.subject";
    public static final String SASL_OAUTHBEARER_TOKEN_SUBJECT_DOC = "The subject of the JWT token sent to the token endpoint.";

    public static final String SASL_OAUTHBEARER_TOKEN_ISSUER = "sasl.oauthbearer.token.issuer";
    public static final String SASL_OAUTHBEARER_TOKEN_ISSUER_DOC = "The issuer of the JWT token sent to the token endpoint.";

    public static final String SASL_OAUTHBEARER_TOKEN_AUDIENCE = "sasl.oauthbearer.token.audience";
    public static final String SASL_OAUTHBEARER_TOKEN_AUDIENCE_DOC = "The audience of the JWT token sent to the token endpoint.";

    public static final String SASL_OAUTHBEARER_TOKEN_TARGET_AUDIENCE = "sasl.oauthbearer.token.target.audience";
    public static final String SASL_OAUTHBEARER_TOKEN_TARGET_AUDIENCE_DOC = "The target audience of the JWT token sent to the token endpoint.";

    public static void addClientOAuthBearerSupport(ConfigDef config) {
        config.define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_ID_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_SECRET, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENPOINT_CLIENT_SECRET_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_SCOPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_SCOPE_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_GRANT_TYPE_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO, ConfigDef.Type.STRING, OAuthBearerConfigs.DEFAULT_SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_ID_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_SECRET, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_SECRET_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_SUBJECT, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_SUBJECT_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ISSUER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ISSUER_DOC)
            .define(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_AUDIENCE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_AUDIENCE_DOC);
    }
}