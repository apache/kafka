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
package kafka.server

import junit.framework.Assert._
import org.junit.Test
import kafka.integration.KafkaServerTestHarness
import kafka.utils._
import kafka.common._
import kafka.log.LogConfig
import kafka.admin.{AdminOperationException, AdminUtils}
import org.scalatest.junit.JUnit3Suite

class DynamicConfigChangeTest extends JUnit3Suite with KafkaServerTestHarness {
  
  override val configs = List(new KafkaConfig(TestUtils.createBrokerConfig(0, TestUtils.choosePort)))

  @Test
  def testConfigChange() {
    val oldVal = 100000
    val newVal = 200000
    val tp = TopicAndPartition("test", 0)
    AdminUtils.createTopic(zkClient, tp.topic, 1, 1, LogConfig(flushInterval = oldVal).toProps)
    TestUtils.retry(10000) {
      val logOpt = this.servers(0).logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    AdminUtils.changeTopicConfig(zkClient, tp.topic, LogConfig(flushInterval = newVal).toProps)
    TestUtils.retry(10000) {
      assertEquals(newVal, this.servers(0).logManager.getLog(tp).get.config.flushInterval)
    }
  }

  @Test
  def testConfigChangeOnNonExistingTopic() {
    val topic = TestUtils.tempTopic
    try {
      AdminUtils.changeTopicConfig(zkClient, topic, LogConfig(flushInterval = 10000).toProps)
      fail("Should fail with AdminOperationException for topic doesn't exist")
    } catch {
      case e: AdminOperationException => // expected
    }
  }

}