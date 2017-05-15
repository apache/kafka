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

import kafka.utils.JaasTestUtils
import kafka.utils.JaasTestUtils.JaasSection
import org.apache.kafka.common.network.ListenerName

class MultipleListenersWithAdditionalJaasContextTest extends MultipleListenersWithSameSecurityProtocolBaseTest {

  import MultipleListenersWithSameSecurityProtocolBaseTest._

  override def saslProperties(listenerName: ListenerName): Properties = {
    listenerName.value match {
      case SecureInternal => kafkaClientSaslProperties(Plain, dynamicJaasConfig = true)
      case SecureExternal => kafkaClientSaslProperties(GssApi, dynamicJaasConfig = true)
      case _ => throw new IllegalArgumentException(s"Unexpected listener name $listenerName")
    }
  }

  override def jaasSections: Seq[JaasSection] = {
    val (serverKeytabFile, _) = maybeCreateEmptyKeytabFiles()
    JaasTestUtils.zkSections ++ Seq(
      JaasTestUtils.kafkaServerSection("secure_external.KafkaServer", Seq(GssApi), Some(serverKeytabFile)),
      JaasTestUtils.kafkaServerSection("secure_internal.KafkaServer", Seq(Plain), None)
    )
  }
}
