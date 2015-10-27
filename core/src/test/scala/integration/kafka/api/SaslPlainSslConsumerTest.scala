/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.File
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.JaasUtils
import org.junit.After
import org.junit.Before
import javax.security.auth.login.Configuration
import org.apache.kafka.common.config.SaslConfigs
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.authenticator.SaslMechanism

class SaslPlainSslConsumerTest extends BaseConsumerTest with ZooKeeperTestHarness {
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))    
  override protected def saslMechanism = Some(SaslMechanism.PLAIN)
  
  @Before
  override def setUp() {
    // Clean-up global configuration set by other tests
    Configuration.setConfiguration(null)
    val jaasFileUrl = Thread.currentThread().getContextClassLoader.getResource("kafka_saslplain_jaas.conf")
    if (jaasFileUrl == null)
      throw new IllegalStateException("Could not load `kafka_saslplain_jaas.conf`, make sure it is in the classpath")
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFileUrl.getPath)
    super.setUp
  }

  @After
  override def tearDown() {
    super.tearDown
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    Configuration.setConfiguration(null)
  }
}
