/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import java.util.Properties

import kafka.api.CustomKafkaServerSasl
import org.apache.kafka.common.network.ListenerName


class MultipleListenersWithAdditionalJaasContextTest extends MultipleListenersWithSameSecurityProtocolBaseTest{

  import MultipleListenersWithSameSecurityProtocolBaseTest._

  override def setSaslProperties(listenerName: ListenerName): Option[Properties] = {

    val gssapiSaslProperties = kafkaClientSaslProperties(GssApi, dynamicJaasConfig = true)
    val plainSaslProperties = kafkaClientSaslProperties(Plain, dynamicJaasConfig = true)

    listenerName.value match {
      case SecureInternal => Some(plainSaslProperties)
      case SecureExternal => Some(gssapiSaslProperties)
      case _ => None
    }
  }

  override def addJaasSection(): Unit = {
    setJaasConfiguration(CustomKafkaServerSasl, "secure_external.KafkaServer", List(GssApi), None)
    setJaasConfiguration(CustomKafkaServerSasl, "secure_internal.KafkaServer", List(Plain), None)
    removeJaasSection("KafkaServer")
  }
}
