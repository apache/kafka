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

package kafka.server.metadata

import java.util
import java.util.{Collections, Optional}

import org.apache.kafka.common.{Endpoint, Uuid}
import org.apache.kafka.common.metadata.RegisterBrokerRecord
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.{BrokerRegistration, RecordTestUtils, VersionRange}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test


class BrokerMetadataListenerTest {
  @Test
  def testCreateAndClose(): Unit = {
    val listener = new BrokerMetadataListener(0, Time.SYSTEM, None)
    listener.close()
  }

  @Test
  def testPublish(): Unit = {
    val listener = new BrokerMetadataListener(0, Time.SYSTEM, None)
    try {
      listener.handleCommit(RecordTestUtils.mockBatchReader(100L,
        util.Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
          setBrokerId(0).
          setBrokerEpoch(100L).
          setFenced(false).
          setRack(null).
          setIncarnationId(Uuid.fromString("GFBwlTcpQUuLYQ2ig05CSg")), 1))));
      val imageRecords = listener.getImageRecords().get()
      assertEquals(0, imageRecords.size())
      assertEquals(100L, listener.highestMetadataOffset())
      listener.handleCommit(RecordTestUtils.mockBatchReader(200L,
        util.Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
          setBrokerId(1).
          setBrokerEpoch(200L).
          setFenced(true).
          setRack(null).
          setIncarnationId(Uuid.fromString("QkOQtNKVTYatADcaJ28xDg")), 1))));
      listener.startPublishing(new MetadataPublisher {
        override def publish(newHighestMetadataOffset: Long,
                             delta: MetadataDelta,
                             newImage: MetadataImage): Unit = {
          assertEquals(200L, newHighestMetadataOffset)
          assertEquals(new BrokerRegistration(0, 100L,
            Uuid.fromString("GFBwlTcpQUuLYQ2ig05CSg"), Collections.emptyList[Endpoint](),
            Collections.emptyMap[String, VersionRange](), Optional.empty[String](), false),
            delta.clusterDelta().broker(0))
          assertEquals(new BrokerRegistration(1, 200L,
            Uuid.fromString("QkOQtNKVTYatADcaJ28xDg"), Collections.emptyList[Endpoint](),
            Collections.emptyMap[String, VersionRange](), Optional.empty[String](), true),
            delta.clusterDelta().broker(1))
        }
      }).get()
    } finally {
      listener.close()
    }
  }
}

