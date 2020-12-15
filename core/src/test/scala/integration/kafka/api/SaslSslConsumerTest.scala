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

import kafka.server.KafkaConfig
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Before}

class SaslSslConsumerTest extends BaseConsumerTest with SaslSetup {
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }
  
}
