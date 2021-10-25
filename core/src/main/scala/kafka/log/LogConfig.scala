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

package kafka.log

import kafka.api.{ApiVersion, ApiVersionValidator, KAFKA_3_0_IV1}
import kafka.log.LogConfig.configDef
import kafka.message.{BrokerCompressionCodec, ProducerCompressionCodec}
import kafka.server.{KafkaConfig, ThrottledReplicaListValidator}
import kafka.utils.Implicits._
import org.apache.kafka.common.compress.{GzipConfig, LZ4Config, SnappyConfig, ZstdConfig}
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList, Validator}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, TopicConfig}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.record.{CompressionConfig, CompressionType, LegacyRecord, RecordVersion, TimestampType}
import org.apache.kafka.common.utils.{ConfigUtils, Utils}

import java.util.{Collections, Locale, Properties}
import scala.annotation.nowarn
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

object Defaults {
  val SegmentSize = kafka.server.Defaults.LogSegmentBytes
  val SegmentMs = kafka.server.Defaults.LogRollHours * 60 * 60 * 1000L
  val SegmentJitterMs = kafka.server.Defaults.LogRollJitterHours * 60 * 60 * 1000L
  val FlushInterval = kafka.server.Defaults.LogFlushIntervalMessages
  val FlushMs = kafka.server.Defaults.LogFlushSchedulerIntervalMs
  val RetentionSize = kafka.server.Defaults.LogRetentionBytes
  val RetentionMs = kafka.server.Defaults.LogRetentionHours * 60 * 60 * 1000L
  val RemoteLogStorageEnable = false
  val LocalRetentionBytes = -2 // It indicates the value to be derived from RetentionSize
  val LocalRetentionMs = -2 // It indicates the value to be derived from RetentionMs
  val MaxMessageSize = kafka.server.Defaults.MessageMaxBytes
  val MaxIndexSize = kafka.server.Defaults.LogIndexSizeMaxBytes
  val IndexInterval = kafka.server.Defaults.LogIndexIntervalBytes
  val FileDeleteDelayMs = kafka.server.Defaults.LogDeleteDelayMs
  val DeleteRetentionMs = kafka.server.Defaults.LogCleanerDeleteRetentionMs
  val MinCompactionLagMs = kafka.server.Defaults.LogCleanerMinCompactionLagMs
  val MaxCompactionLagMs = kafka.server.Defaults.LogCleanerMaxCompactionLagMs
  val MinCleanableDirtyRatio = kafka.server.Defaults.LogCleanerMinCleanRatio
  val CleanupPolicy = kafka.server.Defaults.LogCleanupPolicy
  val UncleanLeaderElectionEnable = kafka.server.Defaults.UncleanLeaderElectionEnable
  val MinInSyncReplicas = kafka.server.Defaults.MinInSyncReplicas
  val CompressionType = kafka.server.Defaults.CompressionType
  val CompressionGzipBuffer = kafka.server.Defaults.CompressionGzipBuffer
  val CompressionSnappyBlock = kafka.server.Defaults.CompressionSnappyBlock
  val CompressionLZ4Block = kafka.server.Defaults.CompressionLZ4Block
  val CompressionZstdWindow = kafka.server.Defaults.CompressionZstdWindow
  val PreAllocateEnable = kafka.server.Defaults.LogPreAllocateEnable

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  val MessageFormatVersion = kafka.server.Defaults.LogMessageFormatVersion

  val MessageTimestampType = kafka.server.Defaults.LogMessageTimestampType
  val MessageTimestampDifferenceMaxMs = kafka.server.Defaults.LogMessageTimestampDifferenceMaxMs
  val LeaderReplicationThrottledReplicas = Collections.emptyList[String]()
  val FollowerReplicationThrottledReplicas = Collections.emptyList[String]()
  val MaxIdMapSnapshots = kafka.server.Defaults.MaxIdMapSnapshots
  val MessageDownConversionEnable = kafka.server.Defaults.MessageDownConversionEnable
}

case class LogConfig(props: java.util.Map[_, _], overriddenConfigs: Set[String] = Set.empty)
  extends AbstractConfig(LogConfig.configDef, props, false) {
  /**
   * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
   * should also go in [[LogConfig.extractLogConfigMap()]].
   */
  val segmentSize = getInt(LogConfig.SegmentBytesProp)
  val segmentMs = getLong(LogConfig.SegmentMsProp)
  val segmentJitterMs = getLong(LogConfig.SegmentJitterMsProp)
  val maxIndexSize = getInt(LogConfig.SegmentIndexBytesProp)
  val flushInterval = getLong(LogConfig.FlushMessagesProp)
  val flushMs = getLong(LogConfig.FlushMsProp)
  val retentionSize = getLong(LogConfig.RetentionBytesProp)
  val retentionMs = getLong(LogConfig.RetentionMsProp)
  val maxMessageSize = getInt(LogConfig.MaxMessageBytesProp)
  val indexInterval = getInt(LogConfig.IndexIntervalBytesProp)
  val fileDeleteDelayMs = getLong(LogConfig.FileDeleteDelayMsProp)
  val deleteRetentionMs = getLong(LogConfig.DeleteRetentionMsProp)
  val compactionLagMs = getLong(LogConfig.MinCompactionLagMsProp)
  val maxCompactionLagMs = getLong(LogConfig.MaxCompactionLagMsProp)
  val minCleanableRatio = getDouble(LogConfig.MinCleanableDirtyRatioProp)
  val compact = getList(LogConfig.CleanupPolicyProp).asScala.map(_.toLowerCase(Locale.ROOT)).contains(LogConfig.Compact)
  val delete = getList(LogConfig.CleanupPolicyProp).asScala.map(_.toLowerCase(Locale.ROOT)).contains(LogConfig.Delete)
  val uncleanLeaderElectionEnable = getBoolean(LogConfig.UncleanLeaderElectionEnableProp)
  val minInSyncReplicas = getInt(LogConfig.MinInSyncReplicasProp)
  val compressionType = getString(LogConfig.CompressionTypeProp).toLowerCase(Locale.ROOT)
  val compressionGzipBuffer = getInt(LogConfig.CompressionGzipBufferProp)
  val compressionSnappyBlock = getInt(LogConfig.CompressionSnappyBlockProp)
  val compressionLZ4Block = getInt(LogConfig.CompressionLZ4BlockProp)
  val compressionZstdWindow = getInt(LogConfig.CompressionZstdWindowProp)
  val preallocate = getBoolean(LogConfig.PreAllocateEnableProp)

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  val messageFormatVersion = ApiVersion(getString(LogConfig.MessageFormatVersionProp))

  val messageTimestampType = TimestampType.forName(getString(LogConfig.MessageTimestampTypeProp))
  val messageTimestampDifferenceMaxMs = getLong(LogConfig.MessageTimestampDifferenceMaxMsProp).longValue
  val LeaderReplicationThrottledReplicas = getList(LogConfig.LeaderReplicationThrottledReplicasProp)
  val FollowerReplicationThrottledReplicas = getList(LogConfig.FollowerReplicationThrottledReplicasProp)
  val messageDownConversionEnable = getBoolean(LogConfig.MessageDownConversionEnableProp)

  class RemoteLogConfig {
    val remoteStorageEnable = getBoolean(LogConfig.RemoteLogStorageEnableProp)

    val localRetentionMs: Long = {
      val localLogRetentionMs = getLong(LogConfig.LocalLogRetentionMsProp)

      // -2 indicates to derive value from retentionMs property.
      if(localLogRetentionMs == -2) retentionMs
      else {
        // Added validation here to check the effective value should not be more than RetentionMs.
        if(localLogRetentionMs == -1 && retentionMs != -1) {
          throw new ConfigException(LogConfig.LocalLogRetentionMsProp, localLogRetentionMs, s"Value must not be -1 as ${LogConfig.RetentionMsProp} value is set as $retentionMs.")
        }

        if (localLogRetentionMs > retentionMs) {
          throw new ConfigException(LogConfig.LocalLogRetentionMsProp, localLogRetentionMs, s"Value must not be more than property: ${LogConfig.RetentionMsProp} value.")
        }

        localLogRetentionMs
      }
    }

    val localRetentionBytes: Long = {
      val localLogRetentionBytes = getLong(LogConfig.LocalLogRetentionBytesProp)

      // -2 indicates to derive value from retentionSize property.
      if(localLogRetentionBytes == -2) retentionSize
      else {
        // Added validation here to check the effective value should not be more than RetentionBytes.
        if(localLogRetentionBytes == -1 && retentionSize != -1) {
          throw new ConfigException(LogConfig.LocalLogRetentionBytesProp, localLogRetentionBytes, s"Value must not be -1 as ${LogConfig.RetentionBytesProp} value is set as $retentionSize.")
        }

        if (localLogRetentionBytes > retentionSize) {
          throw new ConfigException(LogConfig.LocalLogRetentionBytesProp, localLogRetentionBytes, s"Value must not be more than property: ${LogConfig.RetentionBytesProp} value.");
        }

        localLogRetentionBytes
      }
    }
  }

  private val _remoteLogConfig = new RemoteLogConfig()
  def remoteLogConfig = _remoteLogConfig

  @nowarn("cat=deprecation")
  def recordVersion = messageFormatVersion.recordVersion

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)

  def maxSegmentMs: Long = {
    if (compact && maxCompactionLagMs > 0) math.min(maxCompactionLagMs, segmentMs)
    else segmentMs
  }

  def initFileSize: Int = {
    if (preallocate)
      segmentSize
    else
      0
  }

  def overriddenConfigsAsLoggableString: String = {
    val overriddenTopicProps = props.asScala.collect {
      case (k: String, v) if overriddenConfigs.contains(k) => (k, v.asInstanceOf[AnyRef])
    }
    ConfigUtils.configMapToRedactedString(overriddenTopicProps.asJava, configDef)
  }

  /**
   * Returns per-codec [[CompressionConfig]] for given `compressionType`.
   */
  private def getCompressionConfig(compressionType: CompressionType): CompressionConfig = {
    compressionType match {
      case CompressionType.NONE => CompressionConfig.NONE
      case CompressionType.GZIP => CompressionConfig.gzip.setBufferSize(compressionGzipBuffer).build
      case CompressionType.SNAPPY => CompressionConfig.snappy.setBlockSize(compressionSnappyBlock).build
      case CompressionType.LZ4 => CompressionConfig.lz4.setBlockSize(compressionLZ4Block).build
      case CompressionType.ZSTD => CompressionConfig.zstd.setWindowLog(compressionZstdWindow).build
    }
  }

  /**
   * Returns appropriate [[CompressionConfig]] object for given [[compressionType]] and `producerCompressionType` argument.
   *
   * - If [[compressionType]] is "producer", returns `producerCompressionType`'s config.
   * - If [[compressionType]] is "none" or "uncompressed", returns [[org.apache.kafka.common.compress.NoneConfig]].
   * - In other cases, returns [[org.apache.kafka.common.compress.GzipConfig]], [[org.apache.kafka.common.compress.SnappyConfig]], [[org.apache.kafka.common.compress.LZ4Config]], or [[org.apache.kafka.common.compress.ZstdConfig]].
   */
  def compressionConfig(producerCompressionType: CompressionType): CompressionConfig = {
    compressionType match {
      case ProducerCompressionCodec.name => getCompressionConfig(producerCompressionType)
      case "uncompressed" => CompressionConfig.NONE
      case _ => getCompressionConfig(CompressionType.forName(compressionType))
    }
  }
}

object LogConfig {

  def main(args: Array[String]): Unit = {
    println(configDef.toHtml(4, (config: String) => "topicconfigs_" + config))
  }

  val SegmentBytesProp = TopicConfig.SEGMENT_BYTES_CONFIG
  val SegmentMsProp = TopicConfig.SEGMENT_MS_CONFIG
  val SegmentJitterMsProp = TopicConfig.SEGMENT_JITTER_MS_CONFIG
  val SegmentIndexBytesProp = TopicConfig.SEGMENT_INDEX_BYTES_CONFIG
  val FlushMessagesProp = TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG
  val FlushMsProp = TopicConfig.FLUSH_MS_CONFIG
  val RetentionBytesProp = TopicConfig.RETENTION_BYTES_CONFIG
  val RetentionMsProp = TopicConfig.RETENTION_MS_CONFIG
  val RemoteLogStorageEnableProp = TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG
  val LocalLogRetentionMsProp = TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG
  val LocalLogRetentionBytesProp = TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG
  val MaxMessageBytesProp = TopicConfig.MAX_MESSAGE_BYTES_CONFIG
  val IndexIntervalBytesProp = TopicConfig.INDEX_INTERVAL_BYTES_CONFIG
  val DeleteRetentionMsProp = TopicConfig.DELETE_RETENTION_MS_CONFIG
  val MinCompactionLagMsProp = TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG
  val MaxCompactionLagMsProp = TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG
  val FileDeleteDelayMsProp = TopicConfig.FILE_DELETE_DELAY_MS_CONFIG
  val MinCleanableDirtyRatioProp = TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG
  val CleanupPolicyProp = TopicConfig.CLEANUP_POLICY_CONFIG
  val Delete = TopicConfig.CLEANUP_POLICY_DELETE
  val Compact = TopicConfig.CLEANUP_POLICY_COMPACT
  val UncleanLeaderElectionEnableProp = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG
  val MinInSyncReplicasProp = TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG
  val CompressionTypeProp = TopicConfig.COMPRESSION_TYPE_CONFIG
  val CompressionGzipBufferProp = TopicConfig.COMPRESSION_GZIP_BUFFER_CONFIG
  val CompressionSnappyBlockProp = TopicConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG
  val CompressionLZ4BlockProp = TopicConfig.COMPRESSION_LZ4_BLOCK_CONFIG
  val CompressionZstdWindowProp = TopicConfig.COMPRESSION_ZSTD_WINDOW_CONFIG
  val PreAllocateEnableProp = TopicConfig.PREALLOCATE_CONFIG

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0") @nowarn("cat=deprecation")
  val MessageFormatVersionProp = TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG
  val MessageTimestampTypeProp = TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG
  val MessageTimestampDifferenceMaxMsProp = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG
  val MessageDownConversionEnableProp = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG

  // Leave these out of TopicConfig for now as they are replication quota configs
  val LeaderReplicationThrottledReplicasProp = "leader.replication.throttled.replicas"
  val FollowerReplicationThrottledReplicasProp = "follower.replication.throttled.replicas"

  val SegmentSizeDoc = TopicConfig.SEGMENT_BYTES_DOC
  val SegmentMsDoc = TopicConfig.SEGMENT_MS_DOC
  val SegmentJitterMsDoc = TopicConfig.SEGMENT_JITTER_MS_DOC
  val MaxIndexSizeDoc = TopicConfig.SEGMENT_INDEX_BYTES_DOC
  val FlushIntervalDoc = TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC
  val FlushMsDoc = TopicConfig.FLUSH_MS_DOC
  val RetentionSizeDoc = TopicConfig.RETENTION_BYTES_DOC
  val RetentionMsDoc = TopicConfig.RETENTION_MS_DOC
  val RemoteLogStorageEnableDoc = TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC
  val LocalLogRetentionMsDoc = TopicConfig.LOCAL_LOG_RETENTION_MS_DOC
  val LocalLogRetentionBytesDoc = TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC
  val MaxMessageSizeDoc = TopicConfig.MAX_MESSAGE_BYTES_DOC
  val IndexIntervalDoc = TopicConfig.INDEX_INTERVAL_BYTES_DOCS
  val FileDeleteDelayMsDoc = TopicConfig.FILE_DELETE_DELAY_MS_DOC
  val DeleteRetentionMsDoc = TopicConfig.DELETE_RETENTION_MS_DOC
  val MinCompactionLagMsDoc = TopicConfig.MIN_COMPACTION_LAG_MS_DOC
  val MaxCompactionLagMsDoc = TopicConfig.MAX_COMPACTION_LAG_MS_DOC
  val MinCleanableRatioDoc = TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC
  val CompactDoc = TopicConfig.CLEANUP_POLICY_DOC
  val UncleanLeaderElectionEnableDoc = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC
  val MinInSyncReplicasDoc = TopicConfig.MIN_IN_SYNC_REPLICAS_DOC
  val CompressionTypeDoc = TopicConfig.COMPRESSION_TYPE_DOC
  val CompressionGzipBufferDoc = TopicConfig.COMPRESSION_GZIP_BUFFER_DOC
  val CompressionSnappyBlockDoc = TopicConfig.COMPRESSION_SNAPPY_BLOCK_DOC
  val CompressionLZ4BlockDoc = TopicConfig.COMPRESSION_LZ4_BLOCK_DOC
  val CompressionZstdWindowDoc = TopicConfig.COMPRESSION_ZSTD_WINDOW_DOC
  val PreAllocateEnableDoc = TopicConfig.PREALLOCATE_DOC

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0") @nowarn("cat=deprecation")
  val MessageFormatVersionDoc = TopicConfig.MESSAGE_FORMAT_VERSION_DOC

  val MessageTimestampTypeDoc = TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC
  val MessageTimestampDifferenceMaxMsDoc = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC
  val MessageDownConversionEnableDoc = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC

  val LeaderReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on " +
    "the leader side. The list should describe a set of replicas in the form " +
    "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
    "all replicas for this topic."
  val FollowerReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on " +
    "the follower side. The list should describe a set of " + "replicas in the form " +
    "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
    "all replicas for this topic."

  private[log] val ServerDefaultHeaderName = "Server Default Property"

  val configsWithNoServerDefaults: Set[String] = Set(RemoteLogStorageEnableProp, LocalLogRetentionMsProp, LocalLogRetentionBytesProp);

  // Package private for testing
  private[log] class LogConfigDef(base: ConfigDef) extends ConfigDef(base) {
    def this() = this(new ConfigDef)

    private final val serverDefaultConfigNames = mutable.Map[String, String]()
    base match {
      case b: LogConfigDef => serverDefaultConfigNames ++= b.serverDefaultConfigNames
      case _ =>
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Any, validator: Validator,
               importance: ConfigDef.Importance, doc: String, serverDefaultConfigName: String): LogConfigDef = {
      super.define(name, defType, defaultValue, validator, importance, doc)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Any, importance: ConfigDef.Importance,
               documentation: String, serverDefaultConfigName: String): LogConfigDef = {
      super.define(name, defType, defaultValue, importance, documentation)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, importance: ConfigDef.Importance, documentation: String,
               serverDefaultConfigName: String): LogConfigDef = {
      super.define(name, defType, importance, documentation)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    override def headers = List("Name", "Description", "Type", "Default", "Valid Values", ServerDefaultHeaderName,
      "Importance").asJava

    override def getConfigValue(key: ConfigKey, headerName: String): String = {
      headerName match {
        case ServerDefaultHeaderName => serverDefaultConfigNames.getOrElse(key.name, null)
        case _ => super.getConfigValue(key, headerName)
      }
    }

    def serverConfigName(configName: String): Option[String] = serverDefaultConfigNames.get(configName)
  }

  // Package private for testing, return a copy since it's a mutable global variable
  private[kafka] def configDefCopy: LogConfigDef = new LogConfigDef(configDef)

  private val configDef: LogConfigDef = {
    import org.apache.kafka.common.config.ConfigDef.Importance._
    import org.apache.kafka.common.config.ConfigDef.Range._
    import org.apache.kafka.common.config.ConfigDef.Type._
    import org.apache.kafka.common.config.ConfigDef.ValidString._

    @nowarn("cat=deprecation")
    val logConfigDef = new LogConfigDef()
      .define(SegmentBytesProp, INT, Defaults.SegmentSize, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), MEDIUM,
        SegmentSizeDoc, KafkaConfig.LogSegmentBytesProp)
      .define(SegmentMsProp, LONG, Defaults.SegmentMs, atLeast(1), MEDIUM, SegmentMsDoc,
        KafkaConfig.LogRollTimeMillisProp)
      .define(SegmentJitterMsProp, LONG, Defaults.SegmentJitterMs, atLeast(0), MEDIUM, SegmentJitterMsDoc,
        KafkaConfig.LogRollTimeJitterMillisProp)
      .define(SegmentIndexBytesProp, INT, Defaults.MaxIndexSize, atLeast(0), MEDIUM, MaxIndexSizeDoc,
        KafkaConfig.LogIndexSizeMaxBytesProp)
      .define(FlushMessagesProp, LONG, Defaults.FlushInterval, atLeast(0), MEDIUM, FlushIntervalDoc,
        KafkaConfig.LogFlushIntervalMessagesProp)
      .define(FlushMsProp, LONG, Defaults.FlushMs, atLeast(0), MEDIUM, FlushMsDoc,
        KafkaConfig.LogFlushIntervalMsProp)
      // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
      .define(RetentionBytesProp, LONG, Defaults.RetentionSize, MEDIUM, RetentionSizeDoc,
        KafkaConfig.LogRetentionBytesProp)
      // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
      .define(RetentionMsProp, LONG, Defaults.RetentionMs, atLeast(-1), MEDIUM, RetentionMsDoc,
        KafkaConfig.LogRetentionTimeMillisProp)
      .define(MaxMessageBytesProp, INT, Defaults.MaxMessageSize, atLeast(0), MEDIUM, MaxMessageSizeDoc,
        KafkaConfig.MessageMaxBytesProp)
      .define(IndexIntervalBytesProp, INT, Defaults.IndexInterval, atLeast(0), MEDIUM, IndexIntervalDoc,
        KafkaConfig.LogIndexIntervalBytesProp)
      .define(DeleteRetentionMsProp, LONG, Defaults.DeleteRetentionMs, atLeast(0), MEDIUM,
        DeleteRetentionMsDoc, KafkaConfig.LogCleanerDeleteRetentionMsProp)
      .define(MinCompactionLagMsProp, LONG, Defaults.MinCompactionLagMs, atLeast(0), MEDIUM, MinCompactionLagMsDoc,
        KafkaConfig.LogCleanerMinCompactionLagMsProp)
      .define(MaxCompactionLagMsProp, LONG, Defaults.MaxCompactionLagMs, atLeast(1), MEDIUM, MaxCompactionLagMsDoc,
        KafkaConfig.LogCleanerMaxCompactionLagMsProp)
      .define(FileDeleteDelayMsProp, LONG, Defaults.FileDeleteDelayMs, atLeast(0), MEDIUM, FileDeleteDelayMsDoc,
        KafkaConfig.LogDeleteDelayMsProp)
      .define(MinCleanableDirtyRatioProp, DOUBLE, Defaults.MinCleanableDirtyRatio, between(0, 1), MEDIUM,
        MinCleanableRatioDoc, KafkaConfig.LogCleanerMinCleanRatioProp)
      .define(CleanupPolicyProp, LIST, Defaults.CleanupPolicy, ValidList.in(LogConfig.Compact, LogConfig.Delete), MEDIUM, CompactDoc,
        KafkaConfig.LogCleanupPolicyProp)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable,
        MEDIUM, UncleanLeaderElectionEnableDoc, KafkaConfig.UncleanLeaderElectionEnableProp)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), MEDIUM, MinInSyncReplicasDoc,
        KafkaConfig.MinInSyncReplicasProp)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(BrokerCompressionCodec.brokerCompressionOptions:_*),
        MEDIUM, CompressionTypeDoc, KafkaConfig.CompressionTypeProp)
      .define(CompressionGzipBufferProp, INT, Defaults.CompressionGzipBuffer, atLeast(GzipConfig.MIN_BUFFER_SIZE), MEDIUM, CompressionGzipBufferDoc,
        KafkaConfig.CompressionGzipBufferProp)
      .define(CompressionSnappyBlockProp, INT, Defaults.CompressionSnappyBlock, atLeast(SnappyConfig.MIN_BLOCK_SIZE), MEDIUM, CompressionSnappyBlockDoc,
        KafkaConfig.CompressionSnappyBlockProp)
      .define(CompressionLZ4BlockProp, INT, Defaults.CompressionLZ4Block, between(LZ4Config.MIN_BLOCK_SIZE, LZ4Config.MAX_BLOCK_SIZE), MEDIUM, CompressionLZ4BlockDoc,
        KafkaConfig.CompressionLZ4BlockProp)
      .define(CompressionZstdWindowProp, INT, Defaults.CompressionZstdWindow, ConfigDef.LambdaValidator.`with`((name: String, value: Any) => {
        val intValue = value.asInstanceOf[Integer].intValue
        if (!(intValue == ZstdConfig.DEFAULT_WINDOW_LOG || (ZstdConfig.MIN_WINDOW_LOG <= intValue && intValue <= ZstdConfig.MAX_WINDOW_LOG))) throw new ConfigException(name, value, "must be 0 or in [10, 32]")
      }, () => "The window size zstd uses"), MEDIUM, CompressionZstdWindowDoc, KafkaConfig.CompressionZstdWindowProp)
      .define(PreAllocateEnableProp, BOOLEAN, Defaults.PreAllocateEnable, MEDIUM, PreAllocateEnableDoc,
        KafkaConfig.LogPreAllocateProp)
      .define(MessageFormatVersionProp, STRING, Defaults.MessageFormatVersion, ApiVersionValidator, MEDIUM, MessageFormatVersionDoc,
        KafkaConfig.LogMessageFormatVersionProp)
      .define(MessageTimestampTypeProp, STRING, Defaults.MessageTimestampType, in("CreateTime", "LogAppendTime"), MEDIUM, MessageTimestampTypeDoc,
        KafkaConfig.LogMessageTimestampTypeProp)
      .define(MessageTimestampDifferenceMaxMsProp, LONG, Defaults.MessageTimestampDifferenceMaxMs,
        atLeast(0), MEDIUM, MessageTimestampDifferenceMaxMsDoc, KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)
      .define(LeaderReplicationThrottledReplicasProp, LIST, Defaults.LeaderReplicationThrottledReplicas, ThrottledReplicaListValidator, MEDIUM,
        LeaderReplicationThrottledReplicasDoc, LeaderReplicationThrottledReplicasProp)
      .define(FollowerReplicationThrottledReplicasProp, LIST, Defaults.FollowerReplicationThrottledReplicas, ThrottledReplicaListValidator, MEDIUM,
        FollowerReplicationThrottledReplicasDoc, FollowerReplicationThrottledReplicasProp)
      .define(MessageDownConversionEnableProp, BOOLEAN, Defaults.MessageDownConversionEnable, LOW,
        MessageDownConversionEnableDoc, KafkaConfig.LogMessageDownConversionEnableProp)

    // RemoteLogStorageEnableProp, LocalLogRetentionMsProp, LocalLogRetentionBytesProp do not have server default
    // config names.
    logConfigDef
      // This define method is not overridden in LogConfig as these configs do not have server defaults yet.
      .defineInternal(RemoteLogStorageEnableProp, BOOLEAN, Defaults.RemoteLogStorageEnable, null, MEDIUM, RemoteLogStorageEnableDoc)
      .defineInternal(LocalLogRetentionMsProp, LONG, Defaults.LocalRetentionMs, atLeast(-2), MEDIUM, LocalLogRetentionMsDoc)
      .defineInternal(LocalLogRetentionBytesProp, LONG, Defaults.LocalRetentionBytes, atLeast(-2), MEDIUM, LocalLogRetentionBytesDoc)

    logConfigDef
  }

  def apply(): LogConfig = LogConfig(new Properties())

  def configNames: Seq[String] = configDef.names.asScala.toSeq.sorted

  def serverConfigName(configName: String): Option[String] = configDef.serverConfigName(configName)

  def configType(configName: String): Option[ConfigDef.Type] = {
    Option(configDef.configKeys.get(configName)).map(_.`type`)
  }

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: java.util.Map[_ <: Object, _ <: Object], overrides: Properties): LogConfig = {
    val props = new Properties()
    defaults.forEach { (k, v) => props.put(k, v) }
    props ++= overrides
    val overriddenKeys = overrides.keySet.asScala.map(_.asInstanceOf[String]).toSet
    new LogConfig(props, overriddenKeys)
  }

  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties): Unit = {
    val names = configNames
    for(name <- props.asScala.keys)
      if (!names.contains(name))
        throw new InvalidConfigurationException(s"Unknown topic config name: $name")
  }

  private[kafka] def configKeys: Map[String, ConfigKey] = configDef.configKeys.asScala

  def validateValues(props: java.util.Map[_, _]): Unit = {
    val minCompactionLag =  props.get(MinCompactionLagMsProp).asInstanceOf[Long]
    val maxCompactionLag =  props.get(MaxCompactionLagMsProp).asInstanceOf[Long]
    if (minCompactionLag > maxCompactionLag) {
      throw new InvalidConfigurationException(s"conflict topic config setting $MinCompactionLagMsProp " +
        s"($minCompactionLag) > $MaxCompactionLagMsProp ($maxCompactionLag)")
    }
  }

  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties): Unit = {
    validateNames(props)
    val valueMaps = configDef.parse(props)
    validateValues(valueMaps)
  }

  /**
   * Map topic config to the broker config with highest priority. Some of these have additional synonyms
   * that can be obtained using [[kafka.server.DynamicBrokerConfig#brokerConfigSynonyms]]
   */
  @nowarn("cat=deprecation")
  val TopicConfigSynonyms = Map(
    SegmentBytesProp -> KafkaConfig.LogSegmentBytesProp,
    SegmentMsProp -> KafkaConfig.LogRollTimeMillisProp,
    SegmentJitterMsProp -> KafkaConfig.LogRollTimeJitterMillisProp,
    SegmentIndexBytesProp -> KafkaConfig.LogIndexSizeMaxBytesProp,
    FlushMessagesProp -> KafkaConfig.LogFlushIntervalMessagesProp,
    FlushMsProp -> KafkaConfig.LogFlushIntervalMsProp,
    RetentionBytesProp -> KafkaConfig.LogRetentionBytesProp,
    RetentionMsProp -> KafkaConfig.LogRetentionTimeMillisProp,
    MaxMessageBytesProp -> KafkaConfig.MessageMaxBytesProp,
    IndexIntervalBytesProp -> KafkaConfig.LogIndexIntervalBytesProp,
    DeleteRetentionMsProp -> KafkaConfig.LogCleanerDeleteRetentionMsProp,
    MinCompactionLagMsProp -> KafkaConfig.LogCleanerMinCompactionLagMsProp,
    MaxCompactionLagMsProp -> KafkaConfig.LogCleanerMaxCompactionLagMsProp,
    FileDeleteDelayMsProp -> KafkaConfig.LogDeleteDelayMsProp,
    MinCleanableDirtyRatioProp -> KafkaConfig.LogCleanerMinCleanRatioProp,
    CleanupPolicyProp -> KafkaConfig.LogCleanupPolicyProp,
    UncleanLeaderElectionEnableProp -> KafkaConfig.UncleanLeaderElectionEnableProp,
    MinInSyncReplicasProp -> KafkaConfig.MinInSyncReplicasProp,
    CompressionTypeProp -> KafkaConfig.CompressionTypeProp,
    CompressionGzipBufferProp -> KafkaConfig.CompressionGzipBufferProp,
    CompressionSnappyBlockProp -> KafkaConfig.CompressionSnappyBlockProp,
    CompressionLZ4BlockProp -> KafkaConfig.CompressionLZ4BlockProp,
    CompressionZstdWindowProp -> KafkaConfig.CompressionZstdWindowProp,
    PreAllocateEnableProp -> KafkaConfig.LogPreAllocateProp,
    MessageFormatVersionProp -> KafkaConfig.LogMessageFormatVersionProp,
    MessageTimestampTypeProp -> KafkaConfig.LogMessageTimestampTypeProp,
    MessageTimestampDifferenceMaxMsProp -> KafkaConfig.LogMessageTimestampDifferenceMaxMsProp,
    MessageDownConversionEnableProp -> KafkaConfig.LogMessageDownConversionEnableProp
  )


  /**
   * Copy the subset of properties that are relevant to Logs. The individual properties
   * are listed here since the names are slightly different in each Config class...
   */
  @nowarn("cat=deprecation")
  def extractLogConfigMap(
    kafkaConfig: KafkaConfig
  ): java.util.Map[String, Object] = {
    val logProps = new java.util.HashMap[String, Object]()
    logProps.put(SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs)
    logProps.put(FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(CompressionGzipBufferProp, kafkaConfig.compressionGzipBuffer)
    logProps.put(CompressionSnappyBlockProp, kafkaConfig.compressionSnappyBlock)
    logProps.put(CompressionLZ4BlockProp, kafkaConfig.compressionLZ4Block)
    logProps.put(CompressionZstdWindowProp, kafkaConfig.compressionZstdWindow)
    logProps.put(UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(MessageDownConversionEnableProp, kafkaConfig.logMessageDownConversionEnable: java.lang.Boolean)
    logProps
  }

  def shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion: ApiVersion): Boolean =
    interBrokerProtocolVersion >= KAFKA_3_0_IV1

  class MessageFormatVersion(messageFormatVersionString: String, interBrokerProtocolVersionString: String) {
    val messageFormatVersion = ApiVersion(messageFormatVersionString)
    private val interBrokerProtocolVersion = ApiVersion(interBrokerProtocolVersionString)

    def shouldIgnore: Boolean = shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion)

    def shouldWarn: Boolean =
      interBrokerProtocolVersion >= KAFKA_3_0_IV1 && messageFormatVersion.recordVersion.precedes(RecordVersion.V2)

    @nowarn("cat=deprecation")
    def topicWarningMessage(topicName: String): String = {
      s"Topic configuration ${LogConfig.MessageFormatVersionProp} with value `$messageFormatVersionString` is ignored " +
      s"for `$topicName` because the inter-broker protocol version `$interBrokerProtocolVersionString` is " +
      "greater or equal than 3.0. This configuration is deprecated and it will be removed in Apache Kafka 4.0."
    }

    @nowarn("cat=deprecation")
    def brokerWarningMessage: String = {
      s"Broker configuration ${KafkaConfig.LogMessageFormatVersionProp} with value $messageFormatVersionString is ignored " +
      s"because the inter-broker protocol version `$interBrokerProtocolVersionString` is greater or equal than 3.0. " +
      "This configuration is deprecated and it will be removed in Apache Kafka 4.0."
    }
  }

}
