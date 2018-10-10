/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.transaction

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.KafkaException
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Test}
import org.junit.Assert._

class ProducerIdManagerTest {

  private val zkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])

  @After
  def tearDown(): Unit = {
    EasyMock.reset(zkClient)
  }

  @Test
  def testGetProducerId() {
    var zkVersion: Option[Int] = None
    var data: Array[Byte] = null
    EasyMock.expect(zkClient.getDataAndVersion(EasyMock.anyString)).andAnswer(new IAnswer[(Option[Array[Byte]], Int)] {
      override def answer(): (Option[Array[Byte]], Int) = zkVersion.map(Some(data) -> _).getOrElse(None, 0)
    }).anyTimes()

    val capturedVersion: Capture[Int] = EasyMock.newCapture()
    val capturedData: Capture[Array[Byte]] = EasyMock.newCapture()
    EasyMock.expect(zkClient.conditionalUpdatePath(EasyMock.anyString(),
      EasyMock.capture(capturedData),
      EasyMock.capture(capturedVersion),
      EasyMock.anyObject[Option[(KafkaZkClient, String, Array[Byte]) => (Boolean, Int)]])).andAnswer(new IAnswer[(Boolean, Int)] {
        override def answer(): (Boolean, Int) = {
          val newZkVersion = capturedVersion.getValue + 1
          zkVersion = Some(newZkVersion)
          data = capturedData.getValue
          (true, newZkVersion)
        }
      }).anyTimes()

    EasyMock.replay(zkClient)

    val manager1 = new ProducerIdManager(0, zkClient)
    val manager2 = new ProducerIdManager(1, zkClient)

    val pid1 = manager1.generateProducerId()
    val pid2 = manager2.generateProducerId()

    assertEquals(0, pid1)
    assertEquals(ProducerIdManager.PidBlockSize, pid2)

    for (i <- 1L until ProducerIdManager.PidBlockSize)
      assertEquals(pid1 + i, manager1.generateProducerId())

    for (i <- 1L until ProducerIdManager.PidBlockSize)
      assertEquals(pid2 + i, manager2.generateProducerId())

    assertEquals(pid2 + ProducerIdManager.PidBlockSize, manager1.generateProducerId())
    assertEquals(pid2 + ProducerIdManager.PidBlockSize * 2, manager2.generateProducerId())
  }

  @Test(expected = classOf[KafkaException])
  def testExceedProducerIdLimit() {
    EasyMock.expect(zkClient.getDataAndVersion(EasyMock.anyString)).andAnswer(new IAnswer[(Option[Array[Byte]], Int)] {
      override def answer(): (Option[Array[Byte]], Int) = {
        val json = ProducerIdManager.generateProducerIdBlockJson(
          ProducerIdBlock(0, Long.MaxValue - ProducerIdManager.PidBlockSize, Long.MaxValue))
        (Some(json), 0)
      }
    }).anyTimes()
    EasyMock.replay(zkClient)
    new ProducerIdManager(0, zkClient)
  }
}

