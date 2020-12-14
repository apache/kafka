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

package kafka.server

import org.apache.kafka.common.Uuid;

import java.io.File
import kafka.server.KafkaServer.BrokerRole
import org.junit.Assert.assertEquals
import org.junit.Test

class BrokerMetadataCheckpointTest {
  @Test
  def testReadWithNonExistentFile(): Unit = {
    assertEquals(None, new BrokerMetadataCheckpoint(new File("path/that/does/not/exist")).read())
  }

  @Test
  def testCreateLegacyMetadataProperties(): Unit = {
    val meta = LegacyMetaProperties("7bc79ca1-9746-42a3-a35a-efb3cde44492", 3)
    val properties = meta.toProperties
    val parsed = RawMetaProperties(properties)
    assertEquals(0, parsed.version)
    assertEquals(Some(meta.clusterId), parsed.clusterId)
    assertEquals(Some(meta.brokerId), parsed.brokerId)
  }

  @Test
  def testCreateMetadataProperties(): Unit = {
    val meta = MetaProperties(
      clusterId = Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"),
      brokerId = Some(5),
      controllerId = None
    )
    val properties = RawMetaProperties(meta.toProperties)
    val meta2 = MetaProperties.parse(properties, Set(BrokerRole))
    assertEquals(meta, meta2)
  }
}
