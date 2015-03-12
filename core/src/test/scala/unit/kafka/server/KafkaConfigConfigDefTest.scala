/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unit.kafka.server

import java.util.Properties

import kafka.message._
import kafka.server.{Defaults, KafkaConfig}
import org.apache.kafka.common.config.ConfigException
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnit3Suite

import scala.collection.Map
import scala.util.Random._

class KafkaConfigConfigDefTest extends JUnit3Suite {

  @Test
  def testFromPropsDefaults() {
    val defaults = new Properties()
    defaults.put(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")

    // some ordinary setting
    defaults.put(KafkaConfig.AdvertisedPortProp, "1818")

    val props = new Properties(defaults)

    val config = KafkaConfig.fromProps(props)

    Assert.assertEquals(1818, config.advertisedPort)
    Assert.assertEquals("KafkaConfig defaults should be retained", Defaults.ConnectionsMaxIdleMs, config.connectionsMaxIdleMs)
  }

  @Test
  def testFromPropsEmpty() {
    // only required
    val p = new Properties()
    p.put(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
    val actualConfig = KafkaConfig.fromProps(p)

    val expectedConfig = new KafkaConfig(zkConnect = "127.0.0.1:2181")

    Assert.assertEquals(expectedConfig.zkConnect, actualConfig.zkConnect)
    Assert.assertEquals(expectedConfig.zkSessionTimeoutMs, actualConfig.zkSessionTimeoutMs)
    Assert.assertEquals(expectedConfig.zkConnectionTimeoutMs, actualConfig.zkConnectionTimeoutMs)
    Assert.assertEquals(expectedConfig.zkSyncTimeMs, actualConfig.zkSyncTimeMs)
    Assert.assertEquals(expectedConfig.maxReservedBrokerId, actualConfig.maxReservedBrokerId)
    Assert.assertEquals(expectedConfig.brokerId, actualConfig.brokerId)
    Assert.assertEquals(expectedConfig.messageMaxBytes, actualConfig.messageMaxBytes)
    Assert.assertEquals(expectedConfig.numNetworkThreads, actualConfig.numNetworkThreads)
    Assert.assertEquals(expectedConfig.numIoThreads, actualConfig.numIoThreads)
    Assert.assertEquals(expectedConfig.backgroundThreads, actualConfig.backgroundThreads)
    Assert.assertEquals(expectedConfig.queuedMaxRequests, actualConfig.queuedMaxRequests)

    Assert.assertEquals(expectedConfig.port, actualConfig.port)
    Assert.assertEquals(expectedConfig.hostName, actualConfig.hostName)
    Assert.assertEquals(expectedConfig.advertisedHostName, actualConfig.advertisedHostName)
    Assert.assertEquals(expectedConfig.advertisedPort, actualConfig.advertisedPort)
    Assert.assertEquals(expectedConfig.socketSendBufferBytes, actualConfig.socketSendBufferBytes)
    Assert.assertEquals(expectedConfig.socketReceiveBufferBytes, actualConfig.socketReceiveBufferBytes)
    Assert.assertEquals(expectedConfig.socketRequestMaxBytes, actualConfig.socketRequestMaxBytes)
    Assert.assertEquals(expectedConfig.maxConnectionsPerIp, actualConfig.maxConnectionsPerIp)
    Assert.assertEquals(expectedConfig.maxConnectionsPerIpOverrides, actualConfig.maxConnectionsPerIpOverrides)
    Assert.assertEquals(expectedConfig.connectionsMaxIdleMs, actualConfig.connectionsMaxIdleMs)

    Assert.assertEquals(expectedConfig.numPartitions, actualConfig.numPartitions)
    Assert.assertEquals(expectedConfig.logDirs, actualConfig.logDirs)

    Assert.assertEquals(expectedConfig.logSegmentBytes, actualConfig.logSegmentBytes)

    Assert.assertEquals(expectedConfig.logRollTimeMillis, actualConfig.logRollTimeMillis)
    Assert.assertEquals(expectedConfig.logRollTimeJitterMillis, actualConfig.logRollTimeJitterMillis)
    Assert.assertEquals(expectedConfig.logRetentionTimeMillis, actualConfig.logRetentionTimeMillis)

    Assert.assertEquals(expectedConfig.logRetentionBytes, actualConfig.logRetentionBytes)
    Assert.assertEquals(expectedConfig.logCleanupIntervalMs, actualConfig.logCleanupIntervalMs)
    Assert.assertEquals(expectedConfig.logCleanupPolicy, actualConfig.logCleanupPolicy)
    Assert.assertEquals(expectedConfig.logCleanerThreads, actualConfig.logCleanerThreads)
    Assert.assertEquals(expectedConfig.logCleanerIoMaxBytesPerSecond, actualConfig.logCleanerIoMaxBytesPerSecond, 0.0)
    Assert.assertEquals(expectedConfig.logCleanerDedupeBufferSize, actualConfig.logCleanerDedupeBufferSize)
    Assert.assertEquals(expectedConfig.logCleanerIoBufferSize, actualConfig.logCleanerIoBufferSize)
    Assert.assertEquals(expectedConfig.logCleanerDedupeBufferLoadFactor, actualConfig.logCleanerDedupeBufferLoadFactor, 0.0)
    Assert.assertEquals(expectedConfig.logCleanerBackoffMs, actualConfig.logCleanerBackoffMs)
    Assert.assertEquals(expectedConfig.logCleanerMinCleanRatio, actualConfig.logCleanerMinCleanRatio, 0.0)
    Assert.assertEquals(expectedConfig.logCleanerEnable, actualConfig.logCleanerEnable)
    Assert.assertEquals(expectedConfig.logCleanerDeleteRetentionMs, actualConfig.logCleanerDeleteRetentionMs)
    Assert.assertEquals(expectedConfig.logIndexSizeMaxBytes, actualConfig.logIndexSizeMaxBytes)
    Assert.assertEquals(expectedConfig.logIndexIntervalBytes, actualConfig.logIndexIntervalBytes)
    Assert.assertEquals(expectedConfig.logFlushIntervalMessages, actualConfig.logFlushIntervalMessages)
    Assert.assertEquals(expectedConfig.logDeleteDelayMs, actualConfig.logDeleteDelayMs)
    Assert.assertEquals(expectedConfig.logFlushSchedulerIntervalMs, actualConfig.logFlushSchedulerIntervalMs)
    Assert.assertEquals(expectedConfig.logFlushIntervalMs, actualConfig.logFlushIntervalMs)
    Assert.assertEquals(expectedConfig.logFlushOffsetCheckpointIntervalMs, actualConfig.logFlushOffsetCheckpointIntervalMs)
    Assert.assertEquals(expectedConfig.numRecoveryThreadsPerDataDir, actualConfig.numRecoveryThreadsPerDataDir)
    Assert.assertEquals(expectedConfig.autoCreateTopicsEnable, actualConfig.autoCreateTopicsEnable)

    Assert.assertEquals(expectedConfig.minInSyncReplicas, actualConfig.minInSyncReplicas)

    Assert.assertEquals(expectedConfig.controllerSocketTimeoutMs, actualConfig.controllerSocketTimeoutMs)
    Assert.assertEquals(expectedConfig.controllerMessageQueueSize, actualConfig.controllerMessageQueueSize)
    Assert.assertEquals(expectedConfig.defaultReplicationFactor, actualConfig.defaultReplicationFactor)
    Assert.assertEquals(expectedConfig.replicaLagTimeMaxMs, actualConfig.replicaLagTimeMaxMs)
    Assert.assertEquals(expectedConfig.replicaLagMaxMessages, actualConfig.replicaLagMaxMessages)
    Assert.assertEquals(expectedConfig.replicaSocketTimeoutMs, actualConfig.replicaSocketTimeoutMs)
    Assert.assertEquals(expectedConfig.replicaSocketReceiveBufferBytes, actualConfig.replicaSocketReceiveBufferBytes)
    Assert.assertEquals(expectedConfig.replicaFetchMaxBytes, actualConfig.replicaFetchMaxBytes)
    Assert.assertEquals(expectedConfig.replicaFetchWaitMaxMs, actualConfig.replicaFetchWaitMaxMs)
    Assert.assertEquals(expectedConfig.replicaFetchMinBytes, actualConfig.replicaFetchMinBytes)
    Assert.assertEquals(expectedConfig.replicaFetchBackoffMs, actualConfig.replicaFetchBackoffMs)
    Assert.assertEquals(expectedConfig.numReplicaFetchers, actualConfig.numReplicaFetchers)
    Assert.assertEquals(expectedConfig.replicaHighWatermarkCheckpointIntervalMs, actualConfig.replicaHighWatermarkCheckpointIntervalMs)
    Assert.assertEquals(expectedConfig.fetchPurgatoryPurgeIntervalRequests, actualConfig.fetchPurgatoryPurgeIntervalRequests)
    Assert.assertEquals(expectedConfig.producerPurgatoryPurgeIntervalRequests, actualConfig.producerPurgatoryPurgeIntervalRequests)
    Assert.assertEquals(expectedConfig.autoLeaderRebalanceEnable, actualConfig.autoLeaderRebalanceEnable)
    Assert.assertEquals(expectedConfig.leaderImbalancePerBrokerPercentage, actualConfig.leaderImbalancePerBrokerPercentage)
    Assert.assertEquals(expectedConfig.leaderImbalanceCheckIntervalSeconds, actualConfig.leaderImbalanceCheckIntervalSeconds)
    Assert.assertEquals(expectedConfig.uncleanLeaderElectionEnable, actualConfig.uncleanLeaderElectionEnable)

    Assert.assertEquals(expectedConfig.controlledShutdownMaxRetries, actualConfig.controlledShutdownMaxRetries)
    Assert.assertEquals(expectedConfig.controlledShutdownRetryBackoffMs, actualConfig.controlledShutdownRetryBackoffMs)
    Assert.assertEquals(expectedConfig.controlledShutdownEnable, actualConfig.controlledShutdownEnable)

    Assert.assertEquals(expectedConfig.offsetMetadataMaxSize, actualConfig.offsetMetadataMaxSize)
    Assert.assertEquals(expectedConfig.offsetsLoadBufferSize, actualConfig.offsetsLoadBufferSize)
    Assert.assertEquals(expectedConfig.offsetsTopicReplicationFactor, actualConfig.offsetsTopicReplicationFactor)
    Assert.assertEquals(expectedConfig.offsetsTopicPartitions, actualConfig.offsetsTopicPartitions)
    Assert.assertEquals(expectedConfig.offsetsTopicSegmentBytes, actualConfig.offsetsTopicSegmentBytes)
    Assert.assertEquals(expectedConfig.offsetsTopicCompressionCodec, actualConfig.offsetsTopicCompressionCodec)
    Assert.assertEquals(expectedConfig.offsetsRetentionMinutes, actualConfig.offsetsRetentionMinutes)
    Assert.assertEquals(expectedConfig.offsetsRetentionCheckIntervalMs, actualConfig.offsetsRetentionCheckIntervalMs)
    Assert.assertEquals(expectedConfig.offsetCommitTimeoutMs, actualConfig.offsetCommitTimeoutMs)
    Assert.assertEquals(expectedConfig.offsetCommitRequiredAcks, actualConfig.offsetCommitRequiredAcks)

    Assert.assertEquals(expectedConfig.deleteTopicEnable, actualConfig.deleteTopicEnable)
    Assert.assertEquals(expectedConfig.compressionType, actualConfig.compressionType)
  }

  private def atLeastXIntProp(x: Int): String = (nextInt(Int.MaxValue - 1) + x).toString

  private def atLeastOneIntProp: String = atLeastXIntProp(1)

  private def inRangeIntProp(fromInc: Int, toInc: Int): String = (nextInt(toInc + 1 - fromInc) + fromInc).toString

  @Test
  def testFromPropsToProps() {
    import scala.util.Random._
    val expected = new Properties()
    KafkaConfig.configNames().foreach(name => {
      name match {
        case KafkaConfig.ZkConnectProp => expected.setProperty(name, "127.0.0.1:2181")
        case KafkaConfig.ZkSessionTimeoutMsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.ZkConnectionTimeoutMsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.ZkSyncTimeMsProp => expected.setProperty(name, atLeastOneIntProp)

        case KafkaConfig.NumNetworkThreadsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.NumIoThreadsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.BackgroundThreadsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.QueuedMaxRequestsProp => expected.setProperty(name, atLeastOneIntProp)

        case KafkaConfig.PortProp => expected.setProperty(name, "1234")
        case KafkaConfig.HostNameProp => expected.setProperty(name, nextString(10))
        case KafkaConfig.AdvertisedHostNameProp => expected.setProperty(name, nextString(10))
        case KafkaConfig.AdvertisedPortProp => expected.setProperty(name, "4321")
        case KafkaConfig.SocketRequestMaxBytesProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.MaxConnectionsPerIpProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.MaxConnectionsPerIpOverridesProp => expected.setProperty(name, "127.0.0.1:2, 127.0.0.2:3")

        case KafkaConfig.NumPartitionsProp => expected.setProperty(name, "2")
        case KafkaConfig.LogDirsProp => expected.setProperty(name, "/tmp/logs,/tmp/logs2")
        case KafkaConfig.LogDirProp => expected.setProperty(name, "/tmp/log")
        case KafkaConfig.LogSegmentBytesProp => expected.setProperty(name, atLeastXIntProp(Message.MinHeaderSize))

        case KafkaConfig.LogRollTimeMillisProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.LogRollTimeHoursProp => expected.setProperty(name, atLeastOneIntProp)

        case KafkaConfig.LogRetentionTimeMillisProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.LogRetentionTimeMinutesProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.LogRetentionTimeHoursProp => expected.setProperty(name, atLeastOneIntProp)

        case KafkaConfig.LogCleanupIntervalMsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.LogCleanupPolicyProp => expected.setProperty(name, randFrom(Defaults.Compact, Defaults.Delete))
        case KafkaConfig.LogCleanerIoMaxBytesPerSecondProp => expected.setProperty(name, "%.1f".format(nextDouble * .9 + .1))
        case KafkaConfig.LogCleanerDedupeBufferLoadFactorProp => expected.setProperty(name, "%.1f".format(nextDouble * .9 + .1))
        case KafkaConfig.LogCleanerMinCleanRatioProp => expected.setProperty(name, "%.1f".format(nextDouble * .9 + .1))
        case KafkaConfig.LogCleanerEnableProp => expected.setProperty(name, randFrom("true", "false"))
        case KafkaConfig.LogIndexSizeMaxBytesProp => expected.setProperty(name, atLeastXIntProp(4))
        case KafkaConfig.LogFlushIntervalMessagesProp => expected.setProperty(name, atLeastOneIntProp)

        case KafkaConfig.NumRecoveryThreadsPerDataDirProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.AutoCreateTopicsEnableProp => expected.setProperty(name, randFrom("true", "false"))
        case KafkaConfig.MinInSyncReplicasProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.AutoLeaderRebalanceEnableProp => expected.setProperty(name, randFrom("true", "false"))
        case KafkaConfig.UncleanLeaderElectionEnableProp => expected.setProperty(name, randFrom("true", "false"))
        case KafkaConfig.ControlledShutdownEnableProp => expected.setProperty(name, randFrom("true", "false"))
        case KafkaConfig.OffsetsLoadBufferSizeProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.OffsetsTopicPartitionsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.OffsetsTopicSegmentBytesProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.OffsetsTopicCompressionCodecProp => expected.setProperty(name, randFrom(GZIPCompressionCodec.codec.toString,
          SnappyCompressionCodec.codec.toString, LZ4CompressionCodec.codec.toString, NoCompressionCodec.codec.toString))
        case KafkaConfig.OffsetsRetentionMinutesProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.OffsetsRetentionCheckIntervalMsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.OffsetCommitTimeoutMsProp => expected.setProperty(name, atLeastOneIntProp)
        case KafkaConfig.DeleteTopicEnableProp => expected.setProperty(name, randFrom("true", "false"))

        // explicit, non trivial validations or with transient dependencies

        // require(brokerId >= -1 && brokerId <= maxReservedBrokerId)
        case KafkaConfig.MaxReservedBrokerIdProp => expected.setProperty(name, "100")
        case KafkaConfig.BrokerIdProp => expected.setProperty(name, inRangeIntProp(0, 100))
        // require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024)
        case KafkaConfig.LogCleanerThreadsProp =>  expected.setProperty(name, "2")
        case KafkaConfig.LogCleanerDedupeBufferSizeProp => expected.setProperty(name, (1024 * 1024 * 3 + 1).toString)
        // require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs)
        case KafkaConfig.ReplicaFetchWaitMaxMsProp => expected.setProperty(name, "321")
        case KafkaConfig.ReplicaSocketTimeoutMsProp => expected.setProperty(name, atLeastXIntProp(321))
        // require(replicaFetchMaxBytes >= messageMaxBytes)
        case KafkaConfig.MessageMaxBytesProp => expected.setProperty(name, "1234")
        case KafkaConfig.ReplicaFetchMaxBytesProp => expected.setProperty(name, atLeastXIntProp(1234))
        // require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs)
        case KafkaConfig.ReplicaLagTimeMaxMsProp => expected.setProperty(name, atLeastXIntProp(321))
        //require(offsetCommitRequiredAcks >= -1 && offsetCommitRequiredAcks <= offsetsTopicReplicationFactor)
        case KafkaConfig.OffsetCommitRequiredAcksProp => expected.setProperty(name, "-1")
        case KafkaConfig.OffsetsTopicReplicationFactorProp => expected.setProperty(name, inRangeIntProp(-1, Short.MaxValue))
        //BrokerCompressionCodec.isValid(compressionType)
        case KafkaConfig.CompressionTypeProp => expected.setProperty(name, randFrom(BrokerCompressionCodec.brokerCompressionOptions))

        case nonNegativeIntProperty => expected.setProperty(name, nextInt(Int.MaxValue).toString)
      }
    })

    val actual = KafkaConfig.fromProps(expected).toProps
    Assert.assertEquals(expected, actual)
  }

  @Test
  def testFromPropsInvalid() {
    def getBaseProperties(): Properties = {
      val validRequiredProperties = new Properties()
      validRequiredProperties.put(KafkaConfig.ZkConnectProp, "127.0.0.1")
      validRequiredProperties
    }
    // to ensure a basis is valid - bootstraps all needed validation
    KafkaConfig.fromProps(getBaseProperties())

    KafkaConfig.configNames().foreach(name => {
      name match {
        case KafkaConfig.ZkConnectProp => // ignore string
        case KafkaConfig.ZkSessionTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ZkConnectionTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ZkSyncTimeMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")

        case KafkaConfig.BrokerIdProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.NumNetworkThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.NumIoThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.BackgroundThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.QueuedMaxRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        case KafkaConfig.PortProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.HostNameProp => // ignore string
        case KafkaConfig.AdvertisedHostNameProp => //ignore string
        case KafkaConfig.AdvertisedPortProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.SocketSendBufferBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.SocketReceiveBufferBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.MaxConnectionsPerIpOverridesProp =>
          assertPropertyInvalid(getBaseProperties(), name, "127.0.0.1:not_a_number")
        case KafkaConfig.ConnectionsMaxIdleMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")

        case KafkaConfig.NumPartitionsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogDirsProp => // ignore string
        case KafkaConfig.LogDirProp => // ignore string
        case KafkaConfig.LogSegmentBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", Message.MinHeaderSize - 1)

        case KafkaConfig.LogRollTimeMillisProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogRollTimeHoursProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        case KafkaConfig.LogRetentionTimeMillisProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogRetentionTimeMinutesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogRetentionTimeHoursProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        case KafkaConfig.LogRetentionBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanupIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogCleanupPolicyProp => assertPropertyInvalid(getBaseProperties(), name, "unknown_policy", "0")
        case KafkaConfig.LogCleanerIoMaxBytesPerSecondProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerDedupeBufferSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "1024")
        case KafkaConfig.LogCleanerDedupeBufferLoadFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean")
        case KafkaConfig.LogCleanerDeleteRetentionMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerMinCleanRatioProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogIndexSizeMaxBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "3")
        case KafkaConfig.LogFlushIntervalMessagesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogFlushSchedulerIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogFlushIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.NumRecoveryThreadsPerDataDirProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.AutoCreateTopicsEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.MinInSyncReplicasProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.ControllerSocketTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ControllerMessageQueueSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.DefaultReplicationFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaLagTimeMaxMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaLagMaxMessagesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaSocketTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-2")
        case KafkaConfig.ReplicaSocketReceiveBufferBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchMaxBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchWaitMaxMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchMinBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.NumReplicaFetchersProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.AutoLeaderRebalanceEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.LeaderImbalancePerBrokerPercentageProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.UncleanLeaderElectionEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.ControlledShutdownMaxRetriesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ControlledShutdownRetryBackoffMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ControlledShutdownEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.OffsetMetadataMaxSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.OffsetsLoadBufferSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicReplicationFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicPartitionsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicSegmentBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicCompressionCodecProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-1")
        case KafkaConfig.OffsetsRetentionMinutesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsRetentionCheckIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetCommitTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetCommitRequiredAcksProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-2")

        case KafkaConfig.DeleteTopicEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")

        case nonNegativeIntProperty => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-1")
      }
    })
  }

  @Test
  def testSpecificProperties(): Unit = {
    val defaults = new Properties()
    defaults.put(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
    // For ZkConnectionTimeoutMs
    defaults.put(KafkaConfig.ZkSessionTimeoutMsProp, "1234")
    defaults.put(KafkaConfig.MaxReservedBrokerIdProp, "1")
    defaults.put(KafkaConfig.BrokerIdProp, "1")
    defaults.put(KafkaConfig.HostNameProp, "127.0.0.1")
    defaults.put(KafkaConfig.PortProp, "1122")
    defaults.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.1:2, 127.0.0.2:3")
    defaults.put(KafkaConfig.LogDirProp, "/tmp1,/tmp2")
    defaults.put(KafkaConfig.LogRollTimeHoursProp, "12")
    defaults.put(KafkaConfig.LogRollTimeJitterHoursProp, "11")
    defaults.put(KafkaConfig.LogRetentionTimeHoursProp, "10")
    //For LogFlushIntervalMsProp
    defaults.put(KafkaConfig.LogFlushSchedulerIntervalMsProp, "123")
    defaults.put(KafkaConfig.OffsetsTopicCompressionCodecProp, SnappyCompressionCodec.codec.toString)

    val config = KafkaConfig.fromProps(defaults)
    Assert.assertEquals("127.0.0.1:2181", config.zkConnect)
    Assert.assertEquals(1234, config.zkConnectionTimeoutMs)
    Assert.assertEquals(1, config.maxReservedBrokerId)
    Assert.assertEquals(1, config.brokerId)
    Assert.assertEquals("127.0.0.1", config.hostName)
    Assert.assertEquals(1122, config.advertisedPort)
    Assert.assertEquals("127.0.0.1", config.advertisedHostName)
    Assert.assertEquals(Map("127.0.0.1" -> 2, "127.0.0.2" -> 3), config.maxConnectionsPerIpOverrides)
    Assert.assertEquals(List("/tmp1", "/tmp2"), config.logDirs)
    Assert.assertEquals(12 * 60L * 1000L * 60, config.logRollTimeMillis)
    Assert.assertEquals(11 * 60L * 1000L * 60, config.logRollTimeJitterMillis)
    Assert.assertEquals(10 * 60L * 1000L * 60, config.logRetentionTimeMillis)
    Assert.assertEquals(123L, config.logFlushIntervalMs)
    Assert.assertEquals(SnappyCompressionCodec, config.offsetsTopicCompressionCodec)
  }

  private def assertPropertyInvalid(validRequiredProps: => Properties, name: String, values: Any*) {
    values.foreach((value) => {
      val props = validRequiredProps
      props.setProperty(name, value.toString)
      intercept[Exception] {
        KafkaConfig.fromProps(props)
      }
    })
  }

  private def randFrom[T](choices: T*): T = {
    import scala.util.Random
    choices(Random.nextInt(choices.size))
  }

  private def randFrom[T](choices: List[T]): T = {
    import scala.util.Random
    choices(Random.nextInt(choices.size))
  }
}
