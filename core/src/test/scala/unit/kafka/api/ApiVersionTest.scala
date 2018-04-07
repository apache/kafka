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

package kafka.api

import org.apache.kafka.common.record.RecordVersion
import org.junit.Test
import org.junit.Assert._

class ApiVersionTest {

  @Test
  def testApply(): Unit = {
    assertEquals(KAFKA_0_8_0, ApiVersion("0.8.0"))
    assertEquals(KAFKA_0_8_0, ApiVersion("0.8.0.0"))
    assertEquals(KAFKA_0_8_0, ApiVersion("0.8.0.1"))

    assertEquals(KAFKA_0_8_1, ApiVersion("0.8.1"))
    assertEquals(KAFKA_0_8_1, ApiVersion("0.8.1.0"))
    assertEquals(KAFKA_0_8_1, ApiVersion("0.8.1.1"))

    assertEquals(KAFKA_0_8_2, ApiVersion("0.8.2"))
    assertEquals(KAFKA_0_8_2, ApiVersion("0.8.2.0"))
    assertEquals(KAFKA_0_8_2, ApiVersion("0.8.2.1"))

    assertEquals(KAFKA_0_9_0, ApiVersion("0.9.0"))
    assertEquals(KAFKA_0_9_0, ApiVersion("0.9.0.0"))
    assertEquals(KAFKA_0_9_0, ApiVersion("0.9.0.1"))

    assertEquals(KAFKA_0_10_0_IV0, ApiVersion("0.10.0-IV0"))

    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0"))
    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0.0"))
    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0.0-IV0"))
    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0.1"))

    assertEquals(KAFKA_0_10_1_IV0, ApiVersion("0.10.1-IV0"))
    assertEquals(KAFKA_0_10_1_IV1, ApiVersion("0.10.1-IV1"))

    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1"))
    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1.0"))
    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1-IV2"))
    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1.1"))

    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2"))
    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2.0"))
    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2-IV0"))
    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2.1"))

    assertEquals(KAFKA_0_11_0_IV0, ApiVersion("0.11.0-IV0"))
    assertEquals(KAFKA_0_11_0_IV1, ApiVersion("0.11.0-IV1"))

    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0"))
    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0.0"))
    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0-IV2"))
    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0.1"))

    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0"))
    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0.0"))
    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0.0-IV0"))
    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0.1"))
  }

  @Test
  def testMinSupportedVersionFor(): Unit = {
    assertEquals(KAFKA_0_8_0, ApiVersion.minSupportedFor(RecordVersion.V0))
    assertEquals(KAFKA_0_10_0_IV0, ApiVersion.minSupportedFor(RecordVersion.V1))
    assertEquals(KAFKA_0_11_0_IV0, ApiVersion.minSupportedFor(RecordVersion.V2))

    // Ensure that all record versions have a defined min version so that we remember to update the method
    for (recordVersion <- RecordVersion.values)
      assertNotNull(ApiVersion.minSupportedFor(recordVersion))
  }

  @Test
  def testShortVersion(): Unit = {
    assertEquals("0.8.0", KAFKA_0_8_0.shortVersion)
    assertEquals("0.10.0", KAFKA_0_10_0_IV0.shortVersion)
    assertEquals("0.11.0", KAFKA_0_11_0_IV0.shortVersion)
  }

}
