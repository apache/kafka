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

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.utils.Utils
import org.junit.{Assert, Test}

class TransactionCoordinatorIntegrationTest extends KafkaServerTestHarness {
  val offsetsTopicCompressionCodec = CompressionType.GZIP
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  overridingProps.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
  overridingProps.put(KafkaConfig.RequestTimeoutMsProp, "100")

  override def generateConfigs = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map {
    KafkaConfig.fromProps(_, overridingProps)
  }

  @Test
  def shouldCommitTransaction(): Unit = {
    TestUtils.createTopic(zkUtils, Topic.TRANSACTION_STATE_TOPIC_NAME, 1, 1, servers, servers.head.groupCoordinator.offsetsTopicConfigs)
    val topic = "foo"
    TestUtils.createTopic(this.zkUtils, topic, 1, 1, servers)

    val tc = servers.head.transactionCoordinator

    var initProducerIdResult: InitProducerIdResult = null
    def callback(result: InitProducerIdResult): Unit = {
      initProducerIdResult = result
    }

    val txnId = "txn"
    tc.handleInitProducerId(txnId, 900000, callback)

    while(initProducerIdResult == null) {
      Utils.sleep(1)
    }

    Assert.assertEquals(Errors.NONE, initProducerIdResult.error)

    @volatile var addPartitionErrors: Errors = null
    def addPartitionsCallback(errors: Errors): Unit = {
        addPartitionErrors = errors
    }

    tc.handleAddPartitionsToTransaction(txnId,
      initProducerIdResult.producerId,
      initProducerIdResult.producerEpoch,
      Set[TopicPartition](new TopicPartition(topic, 0)),
      addPartitionsCallback
    )

    while(addPartitionErrors == null) {
      Utils.sleep(1)
    }

    Assert.assertEquals(Errors.NONE, addPartitionErrors)

    /**
     * TODO: Can't do this until the Broker side changes are done
    @volatile var commitErrors: Errors = null
    def commitCallback(errors: Errors): Unit ={
      commitErrors = errors
    }

    tc.handleEndTransaction(txnId,
      initPidResult.pid,
      initPidResult.epoch,
      TransactionResult.COMMIT,
      commitCallback)

    while(commitErrors == null) {
      Utils.sleep(1)
    }

    Assert.assertEquals(Errors.NONE, commitErrors)
    */
  }
}
