package com.microsoft.azpubsub.security.authenticator;

import java.util.Map;

import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.utils.Utils;

import com.microsoft.azpubsub.security.auth.AzPubSubConfig;
import com.microsoft.azpubsub.security.auth.AzPubSubPrincipal;
import com.microsoft.azpubsub.security.oauthbearer.AzPubSubOAuthBearerToken;

/*
 * AzPubSub Principal builder for the Kafka Authorizer
 */
public class AzPubSubPrincipalBuilder extends DefaultKafkaPrincipalBuilder implements Configurable {
    private boolean configured = false;
    private CertificateIdentifier certificateIdentifier;

    public AzPubSubPrincipalBuilder() {
        super(null, null);
    }

	@Override
	public void configure(Map<String, ?> configs) {
        AzPubSubConfig config = AzPubSubConfig.fromProps(configs);
        String certificateIdentifierClass = config.getString(AzPubSubConfig.CERT_IDENTIFIER_CLASS_CONFIG);

        try {
            this.certificateIdentifier = Utils.newInstance(certificateIdentifierClass, CertificateIdentifier.class);
            this.certificateIdentifier.configure(config);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("Class %s configured by %s is not found!", certificateIdentifierClass, AzPubSubConfig.CERT_IDENTIFIER_CLASS_CONFIG), e);
        }

        this.configured = true;
	}

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        if (!this.configured) {
            throw new IllegalStateException("AzPubSub Principal Builder not configured");
        }

    	if (context instanceof SslAuthenticationContext) {
            SSLSession sslSession = ((SslAuthenticationContext) context).session();
            CertificateIdentity identity = this.certificateIdentifier.getIdentity(sslSession);
            return new AzPubSubPrincipal(
                    AzPubSubPrincipal.USER_TYPE,
                    identity.principalName(),
                    identity.scope()
                );
    	} else if (context instanceof SaslAuthenticationContext) {
            SaslServer saslServer = ((SaslAuthenticationContext) context).server();
            if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslServer.getMechanismName())) {
                AzPubSubOAuthBearerToken token = (AzPubSubOAuthBearerToken)saslServer.getNegotiatedProperty(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM + ".token");
                return new AzPubSubPrincipal(
                        AzPubSubPrincipal.USER_TYPE,
                        token.principalName(),
                        token.scope()
                    );
            }
        }

        return super.build(context);
    }
}
