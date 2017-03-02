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
package kafka.coordinator

import kafka.common.{KafkaException, MessageFormatter, Topic}
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, ZkUtils}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import java.util.concurrent.locks.ReentrantLock
import java.io.PrintStream
import java.nio.ByteBuffer

import scala.collection.mutable

/*
 * Messages stored for the transaction topic represent the pid and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [pid, epoch, expire_timestamp, status, [topic [partition] ]
 */
object TransactionLogManager {

  private val TXN_ID_KEY = "transactional_id"

  private val PID_KEY = "pid"
  private val EPOCH_KEY = "epoch"
  private val TXN_TIMEOUT_KEY = "transaction_timeout"
  private val TXN_STATUS_KEY = "transaction_status"
  private val TXN_PARTITIONS_KEY = "transaction_partitions"
  private val TOPIC_KEY = "topic"
  private val PARTITION_IDS_KEY = "partition_ids"

  private val KEY_SCHEMA_V0 = new Schema(new Field(TXN_ID_KEY, STRING))
  private val KEY_SCHEMA_TXN_ID_FIELD = KEY_SCHEMA_V0.get(TXN_ID_KEY)

  private val VALUE_PARTITIONS_SCHEMA = new Schema(new Field(TOPIC_KEY, STRING),
                                                   new Field(PARTITION_IDS_KEY, new ArrayOf(INT32)))
  private val PARTITIONS_SCHEMA_TOPIC_FIELD = VALUE_PARTITIONS_SCHEMA.get(TOPIC_KEY)
  private val PARTITIONS_SCHEMA_PARTITION_IDS_FIELD = VALUE_PARTITIONS_SCHEMA.get(PARTITION_IDS_KEY)

  private val VALUE_SCHEMA_V0 = new Schema(new Field(PID_KEY, INT64),
                                           new Field(EPOCH_KEY, INT16),
                                           new Field(TXN_TIMEOUT_KEY, INT32),
                                           new Field(TXN_STATUS_KEY, INT8),
                                           new Field(TXN_PARTITIONS_KEY, ArrayOf.nullable(VALUE_PARTITIONS_SCHEMA)) )
  private val VALUE_SCHEMA_PID_FIELD = VALUE_SCHEMA_V0.get(PID_KEY)
  private val VALUE_SCHEMA_EPOCH_FIELD = VALUE_SCHEMA_V0.get(EPOCH_KEY)
  private val VALUE_SCHEMA_TXN_TIMEOUT_FIELD = VALUE_SCHEMA_V0.get(TXN_TIMEOUT_KEY)
  private val VALUE_SCHEMA_TXN_STATUS_FIELD = VALUE_SCHEMA_V0.get(TXN_STATUS_KEY)
  private val VALUE_SCHEMA_TXN_PARTITIONS_FIELD = VALUE_SCHEMA_V0.get(TXN_PARTITIONS_KEY)

  private val KEY_SCHEMAS = Map(
    0 -> KEY_SCHEMA_V0)

  private val VALUE_SCHEMAS = Map(
    0 -> VALUE_SCHEMA_V0)

  private val CURRENT_KEY_SCHEMA_VERSION = 0.toShort
  private val CURRENT_VALUE_SCHEMA_VERSION = 0.toShort

  private val CURRENT_KEY_SCHEMA = schemaForKey(CURRENT_KEY_SCHEMA_VERSION)

  private val CURRENT_VALUE_SCHEMA = schemaForValue(CURRENT_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = KEY_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown transaction log message key schema version $version")
    }
  }

  private def schemaForValue(version: Int) = {
    val schemaOpt = VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown transaction log message value schema version $version")
    }
  }

  /**
    * Generates the bytes for transaction log message key
    *
    * @return key bytes
    */
  private[coordinator] def keyToBytes(transactionalId: String): Array[Byte] = {
    val key = new Struct(CURRENT_KEY_SCHEMA)
    key.set(KEY_SCHEMA_TXN_ID_FIELD, transactionalId)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload bytes for transaction log message value
    *
    * @return value payload bytes
    */
  private[coordinator] def valueToBytes(pidMetadata: PidMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_VALUE_SCHEMA)
    value.set(VALUE_SCHEMA_PID_FIELD, pidMetadata.pid)
    value.set(VALUE_SCHEMA_EPOCH_FIELD, pidMetadata.epoch)
    value.set(VALUE_SCHEMA_TXN_TIMEOUT_FIELD, pidMetadata.txnTimeoutMs)
    value.set(VALUE_SCHEMA_TXN_STATUS_FIELD, pidMetadata.txnMetadata.state.byte)

    if (pidMetadata.txnMetadata.state.equals(NotExist)) {
      if (pidMetadata.txnMetadata.topicPartitions.nonEmpty)
        throw new IllegalStateException(s"Transaction is not expected to have any partitions since its state is ${pidMetadata.txnMetadata.state}: ${pidMetadata.txnMetadata}")

      value.set(VALUE_SCHEMA_TXN_PARTITIONS_FIELD, null)
    } else {
      // first group the topic partitions by their topic names
      val topicAndPartitions = pidMetadata.txnMetadata.topicPartitions.groupBy(_.topic())

      val partitionArray = topicAndPartitions.map { topicAndPartitionIds =>
        val topicPartitionsStruct = value.instance(VALUE_SCHEMA_TXN_PARTITIONS_FIELD)
        val topic: String = topicAndPartitionIds._1
        val partitionIds: Array[Integer] = topicAndPartitionIds._2.map(topicPartition => Integer.valueOf(topicPartition.partition())).toArray

        topicPartitionsStruct.set(PARTITIONS_SCHEMA_TOPIC_FIELD, topic)
        topicPartitionsStruct.set(PARTITIONS_SCHEMA_PARTITION_IDS_FIELD, partitionIds)

        topicPartitionsStruct
      }
      value.set(VALUE_SCHEMA_TXN_PARTITIONS_FIELD, partitionArray.toArray)
    }

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Decodes the transaction log messages' key
    *
    * @return the key
    */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version == CURRENT_KEY_SCHEMA_VERSION) {
      val transactionalId = key.getString(KEY_SCHEMA_TXN_ID_FIELD)

      TxnKey(version, transactionalId)
    } else {
      throw new IllegalStateException(s"Unknown version $version from the transaction log message")
    }
  }

  /**
    * Decodes the transaction log messages' payload and retrieves pid metadata from it
    *
    * @return a pid metadata object from the message
    */
  def readMessageValue(buffer: ByteBuffer): PidMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForValue(version)
      val value = valueSchema.read(buffer)

      if (version == CURRENT_VALUE_SCHEMA_VERSION) {
        val pid = value.get(VALUE_SCHEMA_PID_FIELD).asInstanceOf[Long]
        val epoch = value.get(VALUE_SCHEMA_EPOCH_FIELD).asInstanceOf[Short]
        val timeout = value.get(VALUE_SCHEMA_TXN_TIMEOUT_FIELD).asInstanceOf[Int]

        val stateByte = value.getByte(VALUE_SCHEMA_TXN_STATUS_FIELD)
        val state = TransactionMetadata.byteToState(stateByte)

        val transactionMetadata = new TransactionMetadata(state)

        if (!state.equals(NotExist)) {
          val topicPartitionArray = value.getArray(VALUE_SCHEMA_TXN_PARTITIONS_FIELD)

          topicPartitionArray.foreach { memberMetadataObj =>
            val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
            val topic = memberMetadata.get(PARTITIONS_SCHEMA_TOPIC_FIELD).asInstanceOf[String]
            val partitionIdArray = memberMetadata.getArray(PARTITIONS_SCHEMA_PARTITION_IDS_FIELD)

            val topicPartitions = partitionIdArray.map { partitionIdObj =>
              val partitionId = partitionIdObj.asInstanceOf[Integer]
              new TopicPartition(topic, partitionId)
            }

            transactionMetadata.addPartitions(topicPartitions.toSet)
          }
        }

        new PidMetadata(pid, epoch, timeout, transactionMetadata)
      } else {
        throw new IllegalStateException(s"Unknown version $version from the pid mapping message value")
      }
    }
  }

  // Formatter for use with tools to read transaction log messages
  class PidMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => readMessageKey(ByteBuffer.wrap(key))).foreach {
        case txnKey: TxnKey =>
          val transactionalId = txnKey.key
          val value = consumerRecord.value
          val pidMetadata =
            if (value == null) "NULL"
            else readMessageValue(ByteBuffer.wrap(value))
          output.write(transactionalId.getBytes)
          output.write("::".getBytes)
          output.write(pidMetadata.toString.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }
}

case class TxnKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key.toString
}

/*
 * Transaction log manager is part of the transaction coordinator that manages the transaction log, which is
 * a special internal topic.
 */
class TransactionLogManager(val brokerId: Int,
                            val zkUtils: ZkUtils) extends Logging {

  this.logIdent = "[Transaction Log Manager " + brokerId + "]: "

  /* number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of transaction topics that are assigned to this manager */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(partitionLock) { ownedPartitions.contains(partitionFor(transactionalId)) }

  /**
    * Gets the partition count of the transaction log topic from ZooKeeper.
    * If the topic does not exist, the default partition count is returned.
    */
  private def getTransactionTopicPartitionCount: Int = {
    zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName).getOrElse(50)  // TODO: need a config for this
  }

  /**
    * Add the partition into the owned list
    *
    * TODO: this is for test only and should be augmented with txn log bootstrapping
    */
  def addPartitionOwnership(partition: Int) {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }

  /**
    * Remove the partition from the owned list
    *
    * TODO: this is for test only and should be augmented with cache cleaning
    */
  def removePartitionOwnership(offsetsPartition: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.remove(offsetsPartition)
    }
  }
}
