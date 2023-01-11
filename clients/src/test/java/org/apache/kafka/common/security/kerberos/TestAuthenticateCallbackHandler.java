package org.apache.kafka.common.security.kerberos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;

public class TestAuthenticateCallbackHandler implements AuthenticateCallbackHandler {
	@Override
	public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
	}

	@Override
	public void configure(Map<String, ?> configs, String saslMechanism,
			List<AppConfigurationEntry> jaasConfigEntries) {
	}

	@Override
	public void close() {
	}
}