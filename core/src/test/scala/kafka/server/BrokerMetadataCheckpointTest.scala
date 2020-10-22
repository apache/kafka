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

import java.io.File
import java.util.UUID

import org.junit.Assert.assertEquals
import org.junit.Test

class BrokerMetadataCheckpointTest {
  @Test
  def testReadWithNonExistentFile(): Unit = {
    assertEquals(None, new BrokerMetadataCheckpoint(new File("path/that/does/not/exist")).read())
  }

  @Test
  def testCreateLegacyMetadataProperties(): Unit = {
    val meta = new LegacyMetaProperties(3, Some("7bc79ca1-9746-42a3-a35a-efb3cde44492"))
    val properties = meta.toProperties()
    val meta2 = LegacyMetaProperties(properties)
    assertEquals(meta, meta2)
  }

  @Test
  def testCreateLegacyMetadataPropertiesWithoutClusterId(): Unit = {
    val meta = new LegacyMetaProperties(0, None)
    val properties = meta.toProperties()
    val meta2 = LegacyMetaProperties(properties)
    assertEquals(meta, meta2)
  }

  @Test
  def testCreateMetadataProperties(): Unit = {
    val meta = new MetaProperties(UUID.fromString("3512fe11-f6ae-419e-ae98-59478b1f953d"),
      UUID.fromString("623c048c-787a-41ac-9e86-7d9d4b07c1a3"))
    val properties = meta.toProperties()
    val meta2 = MetaProperties(properties)
    assertEquals(meta, meta2)
  }
}
