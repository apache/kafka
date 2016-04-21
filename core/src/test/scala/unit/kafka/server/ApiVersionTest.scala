/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package unit.kafka.server

import kafka.server.KafkaApis
import org.apache.kafka.common.requests.ApiVersionResponse.ApiVersion
import org.apache.kafka.common.protocol.{Protocol, ApiKeys}
import org.junit.Test

class ApiVersionTest {

  @Test
  def testApiVersions {
    val apiVersions = KafkaApis.apiKeysToApiVersions.values
    assert(KafkaApis.apiKeysToApiVersions.values.size == ApiKeys.values().length)

    for (key <- ApiKeys.values()) {
      val version: ApiVersion = KafkaApis.apiKeysToApiVersions.getOrElse(key.id, null)
      assert(version != null, "Could not find ApiVersion for API " + key.name)

      for (i <- 0 until version.minVersion) {
        assert(Protocol.REQUESTS(version.apiKey)(i) == null)
        assert(Protocol.RESPONSES(version.apiKey)(i) == null)
      }
      for (i <- version.minVersion.asInstanceOf[Int] to version.maxVersion) {
        assert(Protocol.REQUESTS(version.apiKey)(i) != null)
        assert(Protocol.RESPONSES(version.apiKey)(i) != null)
      }
    }
  }
}