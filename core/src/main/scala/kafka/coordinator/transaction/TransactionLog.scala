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

import kafka.common.{KafkaException, MessageFormatter}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types._
import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.kafka.common.record.CompressionType

import scala.collection.mutable

/*
 * Messages stored for the transaction topic represent the producer id and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [producer_id, producer_epoch, expire_timestamp, status, [topic [partition], timestamp]
 */
object TransactionLog {

  // log-level config default values and enforced values
  val DefaultNumPartitions: Int = 50
  val DefaultSegmentBytes: Int = 100 * 1024 * 1024
  val DefaultReplicationFactor: Short = 3.toShort
  val DefaultMinInSyncReplicas: Int = 2
  val DefaultLoadBufferSize: Int = 5 * 1024 * 1024

  // enforce always using
  //  1. cleanup policy = compact
  //  2. compression = none
  //  3. unclean leader election = disabled
  //  4. required acks = -1 when writing
  val EnforcedCompressionType: CompressionType = CompressionType.NONE
  val EnforcedRequiredAcks: Short = (-1).toShort

  // log message formats

  private object KeySchema {
    private val TXN_ID_KEY = "transactional_id"

    private val V0 = new Schema(new Field(TXN_ID_KEY, STRING))
    private val SCHEMAS = Map(0 -> V0)

    val CURRENT_VERSION = 0.toShort
    val CURRENT = schemaForKey(CURRENT_VERSION)

    val TXN_ID_FIELD = V0.get(TXN_ID_KEY)

    def ofVersion(version: Int): Option[Schema] = SCHEMAS.get(version)
  }

  private object ValueSchema {
    private val ProducerIdKey = "producer_id"
    private val ProducerEpochKey = "producer_epoch"
    private val TxnTimeoutKey = "transaction_timeout"
    private val TxnStatusKey = "transaction_status"
    private val TxnPartitionsKey = "transaction_partitions"
    private val TxnEntryTimestampKey = "transaction_entry_timestamp"
    private val TxnStartTimestampKey = "transaction_start_timestamp"

    private val PartitionIdsKey = "partition_ids"
    private val TopicKey = "topic"
    private val PartitionsSchema = new Schema(new Field(TopicKey, STRING),
      new Field(PartitionIdsKey, new ArrayOf(INT32)))

    private val V0 = new Schema(new Field(ProducerIdKey, INT64, "Producer id in use by the transactional id."),
      new Field(ProducerEpochKey, INT16, "Epoch associated with the producer id"),
      new Field(TxnTimeoutKey, INT32, "Transaction timeout in milliseconds"),
      new Field(TxnStatusKey, INT8,
        "TransactionState the transaction is in"),
      new Field(TxnPartitionsKey, ArrayOf.nullable(PartitionsSchema),
        "Set of partitions involved in the transaction"),
      new Field(TxnEntryTimestampKey, INT64, "Time the transaction was last updated"),
      new Field(TxnStartTimestampKey, INT64, "Time the transaction was started"))

    private val Schemas = Map(0 -> V0)

    val CurrentVersion = 0.toShort
    val Current = schemaForValue(CurrentVersion)

    val ProducerIdField = V0.get(ProducerIdKey)
    val ProducerEpochField = V0.get(ProducerEpochKey)
    val TxnTimeoutField = V0.get(TxnTimeoutKey)
    val TxnStatusField = V0.get(TxnStatusKey)
    val TxnPartitionsField = V0.get(TxnPartitionsKey)
    val TxnEntryTimestampField = V0.get(TxnEntryTimestampKey)
    val TxnStartTimestampField = V0.get(TxnStartTimestampKey)

    val PartitionsTopicField = PartitionsSchema.get(TopicKey)
    val PartitionIdsField = PartitionsSchema.get(PartitionIdsKey)

    def ofVersion(version: Int): Option[Schema] = Schemas.get(version)
  }

  private def schemaForKey(version: Int) = {
    KeySchema.ofVersion(version).getOrElse {
      throw new KafkaException(s"Unknown transaction log message key schema version $version")
    }
  }

  private def schemaForValue(version: Int) = {
    ValueSchema.ofVersion(version).getOrElse {
      throw new KafkaException(s"Unknown transaction log message value schema version $version")
    }
  }

  /**
    * Generates the bytes for transaction log message key
    *
    * @return key bytes
    */
  private[coordinator] def keyToBytes(transactionalId: String): Array[Byte] = {
    import KeySchema._
    val key = new Struct(CURRENT)
    key.set(TXN_ID_FIELD, transactionalId)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload bytes for transaction log message value
    *
    * @return value payload bytes
    */
  private[coordinator] def valueToBytes(txnMetadata: TxnTransitMetadata): Array[Byte] = {
    import ValueSchema._
    val value = new Struct(Current)
    value.set(ProducerIdField, txnMetadata.producerId)
    value.set(ProducerEpochField, txnMetadata.producerEpoch)
    value.set(TxnTimeoutField, txnMetadata.txnTimeoutMs)
    value.set(TxnStatusField, txnMetadata.txnState.byte)
    value.set(TxnEntryTimestampField, txnMetadata.txnLastUpdateTimestamp)
    value.set(TxnStartTimestampField, txnMetadata.txnStartTimestamp)

    if (txnMetadata.txnState == Empty) {
      if (txnMetadata.topicPartitions.nonEmpty)
        throw new IllegalStateException(s"Transaction is not expected to have any partitions since its state is ${txnMetadata.txnState}: $txnMetadata")

      value.set(TxnPartitionsField, null)
    } else {
      // first group the topic partitions by their topic names
      val topicAndPartitions = txnMetadata.topicPartitions.groupBy(_.topic())

      val partitionArray = topicAndPartitions.map { case(topic, partitions) =>
        val topicPartitionsStruct = value.instance(TxnPartitionsField)
        val partitionIds: Array[Integer] = partitions.map(topicPartition => Integer.valueOf(topicPartition.partition())).toArray
        topicPartitionsStruct.set(PartitionsTopicField, topic)
        topicPartitionsStruct.set(PartitionIdsField, partitionIds)
        topicPartitionsStruct
      }
      value.set(TxnPartitionsField, partitionArray.toArray)
    }

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CurrentVersion)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Decodes the transaction log messages' key
    *
    * @return the key
    */
  def readTxnRecordKey(buffer: ByteBuffer): TxnKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version == KeySchema.CURRENT_VERSION) {
      val transactionalId = key.getString(KeySchema.TXN_ID_FIELD)
      TxnKey(version, transactionalId)
    } else {
      throw new IllegalStateException(s"Unknown version $version from the transaction log message")
    }
  }

  /**
    * Decodes the transaction log messages' payload and retrieves the transaction metadata from it
    *
    * @return a transaction metadata object from the message
    */
  def readTxnRecordValue(transactionalId: String, buffer: ByteBuffer): TransactionMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      import ValueSchema._
      val version = buffer.getShort
      val valueSchema = schemaForValue(version)
      val value = valueSchema.read(buffer)

      if (version == CurrentVersion) {
        val producerId = value.getLong(ProducerIdField)
        val epoch = value.getShort(ProducerEpochField)
        val timeout = value.getInt(TxnTimeoutField)

        val stateByte = value.getByte(TxnStatusField)
        val state = TransactionMetadata.byteToState(stateByte)
        val entryTimestamp = value.getLong(TxnEntryTimestampField)
        val startTimestamp = value.getLong(TxnStartTimestampField)

        val transactionMetadata = new TransactionMetadata(transactionalId, producerId, epoch, timeout, state,
          mutable.Set.empty[TopicPartition],startTimestamp, entryTimestamp)

        if (!state.equals(Empty)) {
          val topicPartitionArray = value.getArray(TxnPartitionsField)

          topicPartitionArray.foreach { memberMetadataObj =>
            val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
            val topic = memberMetadata.getString(PartitionsTopicField)
            val partitionIdArray = memberMetadata.getArray(PartitionIdsField)

            val topicPartitions = partitionIdArray.map { partitionIdObj =>
              val partitionId = partitionIdObj.asInstanceOf[Integer]
              new TopicPartition(topic, partitionId)
            }

            transactionMetadata.addPartitions(topicPartitions.toSet)
          }
        }

        transactionMetadata
      } else {
        throw new IllegalStateException(s"Unknown version $version from the transaction log message value")
      }
    }
  }

  // Formatter for use with tools to read transaction log messages
  class TransactionLogMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => readTxnRecordKey(ByteBuffer.wrap(key))).foreach { txnKey =>
        val transactionalId = txnKey.transactionalId
        val value = consumerRecord.value
        val producerIdMetadata =
          if (value == null) "NULL"
          else readTxnRecordValue(transactionalId, ByteBuffer.wrap(value))
        output.write(transactionalId.getBytes(StandardCharsets.UTF_8))
        output.write("::".getBytes(StandardCharsets.UTF_8))
        output.write(producerIdMetadata.toString.getBytes(StandardCharsets.UTF_8))
        output.write("\n".getBytes(StandardCharsets.UTF_8))
      }
    }
  }
}

case class TxnKey(version: Short, transactionalId: String) {
  override def toString: String = transactionalId.toString
}
