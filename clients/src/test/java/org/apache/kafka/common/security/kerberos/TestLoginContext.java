package org.apache.kafka.common.security.kerberos;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class TestLoginContext extends LoginContext {
    private static Configuration EMPTY_WILDCARD_CONFIGURATION = new Configuration() {
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return new AppConfigurationEntry[0]; // match any name
        }
    };
	
	
	public TestLoginContext(String name) throws LoginException {
		super(name,null,null,EMPTY_WILDCARD_CONFIGURATION);
	}
}