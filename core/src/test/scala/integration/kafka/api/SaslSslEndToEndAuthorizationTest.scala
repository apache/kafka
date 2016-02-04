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

import kafka.server.KafkaConfig
import org.apache.kafka.common.protocol.SecurityProtocol

class SaslSslEndToEndAuthorizationTest extends EndToEndAuthorizationTest {
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override val clientPrincipal = "client"
  override val kafkaPrincipal = "kafka"

  // Configure brokers to require SSL client authentication in order to verify that SASL_SSL works correctly even if the
  // client doesn't have a keystore. We want to cover the scenario where a broker requires either SSL client
  // authentication or SASL authentication with SSL as the transport layer (but not both).
  serverConfig.put(KafkaConfig.SslClientAuthProp, "required")

}
