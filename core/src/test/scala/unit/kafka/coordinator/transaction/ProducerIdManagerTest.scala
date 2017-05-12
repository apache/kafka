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

import kafka.common.KafkaException
import kafka.utils.ZkUtils
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Test}
import org.junit.Assert._

class ProducerIdManagerTest {

  val zkUtils: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])

  @After
  def tearDown(): Unit = {
    EasyMock.reset(zkUtils)
  }

  @Test
  def testGetPID() {
    var zkVersion: Int = -1
    var data: String = null
    EasyMock.expect(zkUtils.readDataAndVersionMaybeNull(EasyMock.anyString()))
      .andAnswer(new IAnswer[(Option[String], Int)] {
        override def answer(): (Option[String], Int) = {
          if (zkVersion == -1) {
            (None.asInstanceOf[Option[String]], 0)
          } else {
            (Some(data), zkVersion)
          }
        }
      })
      .anyTimes()

    val capturedVersion: Capture[Int] = EasyMock.newCapture()
    val capturedData: Capture[String] = EasyMock.newCapture()
    EasyMock.expect(zkUtils.conditionalUpdatePersistentPath(EasyMock.anyString(),
      EasyMock.capture(capturedData),
      EasyMock.capture(capturedVersion),
      EasyMock.anyObject().asInstanceOf[Option[(ZkUtils, String, String) => (Boolean,Int)]]))
      .andAnswer(new IAnswer[(Boolean, Int)] {
        override def answer(): (Boolean, Int) = {
          zkVersion = capturedVersion.getValue + 1
          data = capturedData.getValue

          (true, zkVersion)
        }
      })
      .anyTimes()

    EasyMock.replay(zkUtils)

    val manager1: ProducerIdManager = new ProducerIdManager(0, zkUtils)
    val manager2: ProducerIdManager = new ProducerIdManager(1, zkUtils)

    val pid1 = manager1.generateProducerId()
    val pid2 = manager2.generateProducerId()

    assertEquals(0, pid1)
    assertEquals(ProducerIdManager.PidBlockSize, pid2)

    for (i <- 1 until ProducerIdManager.PidBlockSize.asInstanceOf[Int]) {
      assertEquals(pid1 + i, manager1.generateProducerId())
    }

    for (i <- 1 until ProducerIdManager.PidBlockSize.asInstanceOf[Int]) {
      assertEquals(pid2 + i, manager2.generateProducerId())
    }

    assertEquals(pid2 + ProducerIdManager.PidBlockSize, manager1.generateProducerId())
    assertEquals(pid2 + ProducerIdManager.PidBlockSize * 2, manager2.generateProducerId())
  }

  @Test(expected = classOf[KafkaException])
  def testExceedPIDLimit() {
    EasyMock.expect(zkUtils.readDataAndVersionMaybeNull(EasyMock.anyString()))
      .andAnswer(new IAnswer[(Option[String], Int)] {
        override def answer(): (Option[String], Int) = {
          (Some(ProducerIdManager.generateProducerIdBlockJson(ProducerIdBlock(0,
            Long.MaxValue - ProducerIdManager.PidBlockSize,
            Long.MaxValue))), 0)
        }
      })
      .anyTimes()
    EasyMock.replay(zkUtils)
    new ProducerIdManager(0, zkUtils)
  }
}

