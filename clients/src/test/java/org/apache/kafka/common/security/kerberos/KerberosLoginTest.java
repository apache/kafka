/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.kerberos;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Field;
import java.util.HashMap;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.junit.jupiter.api.Test;

public class KerberosLoginTest {
	/*
	 * KerberosLogin class should reuse the same callbackHandler for reLogin() as was used for login().
	 */
    @Test
    public void testReuseCallbackDuringRelogin() throws Exception {
    	KerberosLogin kl = new KerberosLogin();
    	
    	HashMap<String, Object> configs = new HashMap<>();
    	configs.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME,"test");
    	configs.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,0.5);
    	configs.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,0.5);
    	configs.put(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,100l);
    	
    	Configuration configuration = new TestConfiguration();
    	AuthenticateCallbackHandler callbackHandler = new TestAuthenticateCallbackHandler();
    	
    	kl.configure(configs, "dummy", configuration, callbackHandler);
    	
    	AuthenticateCallbackHandler storedCallbackHandler = (AuthenticateCallbackHandler) getFieldViaReflection(kl,"loginCallbackHandler");
    	assertSame(callbackHandler,storedCallbackHandler);
    	
    	setFieldViaReflection(kl,"isKrbTicket",true);
    	setFieldViaReflection(kl,"loginContext",new TestLoginContext("dummy"));
    	
    	LoginContext lctxBeforeRelogin = (LoginContext) getFieldViaReflection(kl,"loginContext");
    	
    	try {
    		kl.reLogin();
    	} catch( Exception ex ) {
    		// as expected -- all modules ignored
    		// but the new login context should have been created with the callbackHandler at this point already
    	}
    	
    	// login context must change
    	LoginContext lctxAfterRelogin = (LoginContext) getFieldViaReflection(kl,"loginContext");
    	assertNotSame(lctxAfterRelogin, lctxBeforeRelogin);
    	
    	// but the callbackHandler should be the same
    	CallbackHandler ch = (CallbackHandler) getFieldViaReflection(lctxAfterRelogin,"callbackHandler");
    	assertSame(callbackHandler,ch);
    }

	private void setFieldViaReflection(Object kl, String name, Object val) throws Exception {
		Class clz = kl.getClass();
		
		Field field = clz.getDeclaredField(name);
		field.setAccessible(true);
		field.set(kl, val);
	}
	
	private Object getFieldViaReflection(Object kl, String name) throws Exception {
		Class clz = kl.getClass();
		
		Field field = null;
		while(field == null && clz != null) {
			try {
				field = clz.getDeclaredField(name);
			} catch(NoSuchFieldException ex) {
				clz = clz.getSuperclass();
			}
		}
		
		field.setAccessible(true);
		return field.get(kl);
	}
}
