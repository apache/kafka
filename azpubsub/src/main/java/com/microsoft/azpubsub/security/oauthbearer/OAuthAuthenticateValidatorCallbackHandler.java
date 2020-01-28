package com.microsoft.azpubsub.security.oauthbearer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationResult;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import com.microsoft.azpubsub.security.auth.AzPubSubConfig;

/*
 * Callback handler for SASL-based authentication
 */
public class OAuthAuthenticateValidatorCallbackHandler implements AuthenticateCallbackHandler {
    private boolean configured = false;
    private Time time = Time.SYSTEM;
    private OAuthAuthenticateValidator oAuthAuthenticateValidator;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));
        }

        AzPubSubConfig config = AzPubSubConfig.fromProps(configs);
        String validatorClass = config.getString(AzPubSubConfig.TOKEN_VALIDATOR_CLASS_CONFIG);

        try {
            this.oAuthAuthenticateValidator = Utils.newInstance(validatorClass, OAuthAuthenticateValidator.class);
            this.oAuthAuthenticateValidator.configure(config);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("Class %s configured by %s is not found!", validatorClass, AzPubSubConfig.TOKEN_VALIDATOR_CLASS_CONFIG), e);
        }

        this.configured = true;
    }

    @Override
    public void close() {
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (!this.configured) {
            throw new IllegalStateException("Callback handler not configured");
        }

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                try {
                    OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
                    handleCallback(validationCallback);
                } catch (KafkaException e) {
                    throw new IOException(e.getMessage(), e);
                }
            }
            else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleCallback(OAuthBearerValidatorCallback callback){
        String accessToken = callback.tokenValue();
        if (accessToken == null) {
            throw new IllegalArgumentException("Callback missing required token value");
        }

        OAuthBearerToken token = oAuthAuthenticateValidator.introspectBearer(accessToken);

        long now = time.milliseconds();
        if (now > token.lifetimeMs()){
            throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult.newFailure("Token Expired - need re-authentication!"));
        }

        callback.token(token);
    }
}
