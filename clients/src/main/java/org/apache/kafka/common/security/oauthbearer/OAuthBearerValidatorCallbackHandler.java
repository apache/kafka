package org.apache.kafka.common.security.oauthbearer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler.class);

    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism,
        final List<AppConfigurationEntry> jaasConfigEntries) {
    }

    @Override
    public void close() {

    }

    @Override
    public void handle(final Callback[] callbacks)
        throws IOException, UnsupportedCallbackException {
        if (callbacks != null && callbacks.length > 0) {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerValidatorCallback)
                    handle((OAuthBearerValidatorCallback)callback);
            }
        } else {
            log.warn("Nope - no callbacks provided");
        }
    }

    private void handle(OAuthBearerValidatorCallback callback) throws IOException {
        try {
            log.warn("handle - callback.token(): {}", callback.token());
            log.warn("handle - callback.tokenValue(): {}", callback.tokenValue());
        } catch (Exception e) {
            callback.error("nyi", e.getMessage(), "https://www.example.com");
        }
    }

}
