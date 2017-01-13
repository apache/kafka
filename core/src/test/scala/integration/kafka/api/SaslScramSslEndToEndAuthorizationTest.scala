/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.api

import org.apache.kafka.common.security.scram.ScramMechanism
import kafka.utils.JaasTestUtils
import kafka.admin.ConfigCommand
import kafka.utils.ZkUtils
import scala.collection.JavaConverters._

class SaslScramSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  override protected def kafkaClientSaslMechanism = "SCRAM-SHA-256"
  override protected def kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override val clientPrincipal = JaasTestUtils.KafkaScramUser
  override val kafkaPrincipal = JaasTestUtils.KafkaScramAdmin
  private val clientPassword = JaasTestUtils.KafkaScramPassword
  private val kafkaPassword = JaasTestUtils.KafkaScramAdminPassword

  override def configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart()
    zkUtils.makeSurePersistentPathExists(ZkUtils.EntityConfigChangesPath)

    def configCommandArgs(username: String, password: String) : Array[String] = {
      val credentials = kafkaServerSaslMechanisms.map(m => s"$m=[iterations=4096,password=$password]")
      Array("--zookeeper", zkConnect,
            "--alter", "--add-config", credentials.mkString(","),
            "--entity-type", "users",
            "--entity-name", username)
    }
    ConfigCommand.main(configCommandArgs(kafkaPrincipal, kafkaPassword))
    ConfigCommand.main(configCommandArgs(clientPrincipal, clientPassword))
    ConfigCommand.main(configCommandArgs(JaasTestUtils.KafkaScramUser2, JaasTestUtils.KafkaScramPassword2))
  }
}
