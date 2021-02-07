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

package org.apache.kafka.connect.rest.basic.auth.extension;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.security.auth.login.Configuration;
import javax.ws.rs.core.Configurable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BasicAuthSecurityRestExtensionTest {

    Configuration priorConfiguration;

    @BeforeEach
    public void setup() {
        priorConfiguration = Configuration.getConfiguration();
    }

    @AfterEach
    public void tearDown() {
        Configuration.setConfiguration(priorConfiguration);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJaasConfigurationNotOverwritten() {
        Capture<JaasBasicAuthFilter> jaasFilter = EasyMock.newCapture();
        Configurable<? extends Configurable<?>> configurable = EasyMock.mock(Configurable.class);
        EasyMock.expect(configurable.register(EasyMock.capture(jaasFilter))).andReturn(null);
  
        ConnectRestExtensionContext context = EasyMock.mock(ConnectRestExtensionContext.class);
        EasyMock.expect(context.configurable()).andReturn((Configurable) configurable);

        EasyMock.replay(configurable, context);
  
        BasicAuthSecurityRestExtension extension = new BasicAuthSecurityRestExtension();
        Configuration overwrittenConfiguration = EasyMock.mock(Configuration.class);
        Configuration.setConfiguration(overwrittenConfiguration);
        extension.register(context);
  
        assertNotEquals(overwrittenConfiguration, jaasFilter.getValue().configuration,
            "Overwritten JAAS configuration should not be used by basic auth REST extension");
    }

    @Test
    public void testBadJaasConfigInitialization() {
        SecurityException jaasConfigurationException = new SecurityException(new IOException("Bad JAAS config is bad"));
        Supplier<Configuration> configuration = BasicAuthSecurityRestExtension.initializeConfiguration(() -> {
            throw jaasConfigurationException;
        });

        ConnectException thrownException = assertThrows(ConnectException.class, configuration::get);
        assertEquals(jaasConfigurationException, thrownException.getCause());
    }

    @Test
    public void testGoodJaasConfigInitialization() {
        AtomicBoolean configurationInitializerEvaluated = new AtomicBoolean(false);
        Configuration mockConfiguration = EasyMock.mock(Configuration.class);
        Supplier<Configuration> configuration = BasicAuthSecurityRestExtension.initializeConfiguration(() -> {
            configurationInitializerEvaluated.set(true);
            return mockConfiguration;
        });

        assertTrue(configurationInitializerEvaluated.get());
        assertEquals(mockConfiguration, configuration.get());
    }

    @Test
    public void testBadJaasConfigExtensionSetup() {
        SecurityException jaasConfigurationException = new SecurityException(new IOException("Bad JAAS config is bad"));
        Supplier<Configuration> configuration = () -> {
            throw jaasConfigurationException;
        };

        BasicAuthSecurityRestExtension extension = new BasicAuthSecurityRestExtension(configuration);

        Exception thrownException = assertThrows(Exception.class, () -> extension.configure(Collections.emptyMap()));
        assertEquals(jaasConfigurationException, thrownException);

        thrownException = assertThrows(Exception.class, () -> extension.register(EasyMock.mock(ConnectRestExtensionContext.class)));
        assertEquals(jaasConfigurationException, thrownException);
    }
}
