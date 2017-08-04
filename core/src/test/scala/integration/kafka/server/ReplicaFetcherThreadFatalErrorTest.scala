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

import java.util.concurrent.atomic.AtomicBoolean

import kafka.admin.AdminUtils
import kafka.cluster.BrokerEndPoint
import kafka.server.ReplicaFetcherThread.{FetchRequest, PartitionData}
import kafka.utils.{Exit, TestUtils, ZkUtils}
import kafka.utils.TestUtils.createBrokerConfigs
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.utils.Time
import org.junit.{After, Test}

import scala.collection.Map
import scala.collection.JavaConverters._
import scala.concurrent.Future

class ReplicaFetcherThreadFatalErrorTest extends ZooKeeperTestHarness {

  private var brokers: Seq[KafkaServer] = null
  @volatile private var shutdownCompleted = false

  @After
  override def tearDown() {
    Exit.resetExitProcedure()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  /**
    * Verifies that a follower shuts down if the offset for an `added partition` is out of range and if a fatal
    * exception is thrown from `handleOffsetOutOfRange`. It's a bit tricky to ensure that there are no deadlocks
    * when the shutdown hook is invoked and hence this test.
    */
  @Test
  def testFatalErrorInAddPartitions(): Unit = {

    // Unlike `TestUtils.createTopic`, this doesn't wait for metadata propagation as the broker shuts down before
    // the metadata is propagated.
    def createTopic(zkUtils: ZkUtils, topic: String): Unit = {
      AdminUtils.createTopic(zkUtils, topic, partitions = 1, replicationFactor = 2)
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    }

    val props = createBrokerConfigs(2, zkConnect)
    brokers = props.map(KafkaConfig.fromProps).map(config => createServer(config, { params =>
      import params._
      new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, config, replicaManager, metrics, time, quotaManager) {
        override def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = throw new FatalExitError
        override def addPartitions(partitionAndOffsets: Map[TopicPartition, Long]): Unit =
          super.addPartitions(partitionAndOffsets.mapValues(_ => -1))
      }
    }))
    createTopic(zkUtils, "topic")
    TestUtils.waitUntilTrue(() => shutdownCompleted, "Shutdown of follower did not complete")
  }

  /**
    * Verifies that a follower shuts down if the offset of a partition in the fetch response is out of range and if a
    * fatal exception is thrown from `handleOffsetOutOfRange`. It's a bit tricky to ensure that there are no deadlocks
    * when the shutdown hook is invoked and hence this test.
    */
  @Test
  def testFatalErrorInProcessFetchRequest(): Unit = {
    val props = createBrokerConfigs(2, zkConnect)
    brokers = props.map(KafkaConfig.fromProps).map(config => createServer(config, { params =>
      import params._
      new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, config, replicaManager, metrics, time, quotaManager) {
        override def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = throw new FatalExitError
        override protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
          fetchRequest.underlying.fetchData.asScala.keys.toSeq.map { tp =>
            (tp, new PartitionData(new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE,
              FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, null)))
          }
        }
      }
    }))
    TestUtils.createTopic(zkUtils, "topic", numPartitions = 1, replicationFactor = 2, servers = brokers)
    TestUtils.waitUntilTrue(() => shutdownCompleted, "Shutdown of follower did not complete")
  }

  private case class FetcherThreadParams(threadName: String, fetcherId: Int, sourceBroker: BrokerEndPoint,
                                         replicaManager: ReplicaManager, metrics: Metrics, time: Time,
                                         quotaManager: ReplicationQuotaManager)

  private def createServer(config: KafkaConfig, fetcherThread: FetcherThreadParams => ReplicaFetcherThread): KafkaServer = {
    val time = Time.SYSTEM
    val server = new KafkaServer(config, time) {

      override def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
        new ReplicaManager(config, metrics, time, zkUtils, kafkaScheduler, logManager, isShuttingDown,
          quotaManagers.follower, new BrokerTopicStats, metadataCache, logDirFailureChannel) {

          override protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String],
                                                             quotaManager: ReplicationQuotaManager) =
            new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager) {
              override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
                val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
                val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
                fetcherThread(FetcherThreadParams(threadName, fetcherId, sourceBroker, replicaManager, metrics,
                  time, quotaManager))
              }
            }
        }
      }

    }

    Exit.setExitProcedure { (_, _) =>
      import scala.concurrent.ExecutionContext.Implicits._
      // Run in a separate thread like shutdown hooks
      Future {
        server.shutdown()
        shutdownCompleted = true
      }
      // Sleep until interrupted to emulate the fact that `System.exit()` never returns
      Thread.sleep(Long.MaxValue)
      throw new AssertionError
    }
    server.startup()
    server
  }

}
