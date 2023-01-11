package org.apache.kafka.common.security.kerberos;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

public class TestConfiguration extends Configuration {
	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
		return new AppConfigurationEntry[] {};
	}
}