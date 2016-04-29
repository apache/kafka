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

import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.protocol.{Protocol, ApiKeys}
import org.junit.Assert._
import org.junit.Test

class ApiVersionsTest {

  @Test
  def testApiVersions {
    val apiVersions = ApiVersionsResponse.apiVersionsResponse.apiVersions
    assertEquals("API versions for all API keys must be maintained.", apiVersions.size, ApiKeys.values().length)

    for (key <- ApiKeys.values) {
      val version = ApiVersionsResponse.apiVersionsResponse.apiVersion(key.id)
      assertNotNull(s"Could not find ApiVersion for API ${key.name}", version)
      assertEquals(s"Incorrect min version for Api ${key.name}.", version.minVersion, Protocol.MIN_VERSIONS(key.id))
      assertEquals(s"Incorrect max version for Api ${key.name}.", version.maxVersion, Protocol.CURR_VERSION(key.id))

      // Check if versions less than min version are indeed set as null, i.e., deprecated.
      for (i <- 0 until version.minVersion) {
        assertNull(s"Request version $i for API ${version.apiKey} must be null.", Protocol.REQUESTS(version.apiKey)(i))
        assertNull(s"Response version $i for API ${version.apiKey} must be null.", Protocol.RESPONSES(version.apiKey)(i))
      }

      // Check if versions between min and max versions are non null, i.e., valid.
      for (i <- version.minVersion.toInt to version.maxVersion) {
        assertNotNull(s"Request version $i for API ${version.apiKey} must not be null.", Protocol.REQUESTS(version.apiKey)(i))
        assertNotNull(s"Response version $i for API ${version.apiKey} must not be null.", Protocol.RESPONSES(version.apiKey)(i))
      }
    }
  }
}