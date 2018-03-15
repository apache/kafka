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

package kafka.common

import java.nio.ByteBuffer

import kafka.message.InvalidMessageException
import org.apache.kafka.common.errors.InvalidTopicException

import scala.Predef._

/**
 * A bi-directional mapping between error codes and exceptions
 */
object ErrorMapping {
  val EmptyByteBuffer = ByteBuffer.allocate(0)

  val UnknownCode: Short = -1
  val NoError: Short = 0
  val OffsetOutOfRangeCode: Short = 1
  val InvalidMessageCode: Short = 2
  val UnknownTopicOrPartitionCode: Short = 3
  val InvalidFetchSizeCode: Short = 4
  val LeaderNotAvailableCode: Short = 5
  val NotLeaderForPartitionCode: Short = 6
  val RequestTimedOutCode: Short = 7
  val BrokerNotAvailableCode: Short = 8
  val ReplicaNotAvailableCode: Short = 9
  val MessageSizeTooLargeCode: Short = 10
  val StaleControllerEpochCode: Short = 11
  val OffsetMetadataTooLargeCode: Short = 12
  val StaleLeaderEpochCode: Short = 13
  val OffsetsLoadInProgressCode: Short = 14
  val ConsumerCoordinatorNotAvailableCode: Short = 15
  val NotCoordinatorForConsumerCode: Short = 16
  val InvalidTopicCode: Short = 17
  val MessageSetSizeTooLargeCode: Short = 18
  val NotEnoughReplicasCode: Short = 19
  val NotEnoughReplicasAfterAppendCode: Short = 20
  // 21: InvalidRequiredAcks
  // 22: IllegalConsumerGeneration
  // 23: INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY
  // 24: UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY
  // 25: UNKNOWN_CONSUMER_ID
  // 26: INVALID_SESSION_TIMEOUT
  // 27: REBALANCE_IN_PROGRESS
  // 28: INVALID_COMMIT_OFFSET_SIZE
  val TopicAuthorizationCode: Short = 29
  val GroupAuthorizationCode: Short = 30
  val ClusterAuthorizationCode: Short = 31
  // 32: INVALID_TIMESTAMP
  // 33: UNSUPPORTED_SASL_MECHANISM
  // 34: ILLEGAL_SASL_STATE
  // 35: UNSUPPORTED_VERSION
  // 36: TOPIC_ALREADY_EXISTS
  // 37: INVALID_PARTITIONS
  // 38: INVALID_REPLICATION_FACTOR
  // 39: INVALID_REPLICA_ASSIGNMENT
  // 40: INVALID_CONFIG
  // 41: NOT_CONTROLLER
  // 42: INVALID_REQUEST

  private val exceptionToCode =
    Map[Class[Throwable], Short](
      classOf[OffsetOutOfRangeException].asInstanceOf[Class[Throwable]] -> OffsetOutOfRangeCode,
      classOf[InvalidMessageException].asInstanceOf[Class[Throwable]] -> InvalidMessageCode,
      classOf[UnknownTopicOrPartitionException].asInstanceOf[Class[Throwable]] -> UnknownTopicOrPartitionCode,
      classOf[InvalidMessageSizeException].asInstanceOf[Class[Throwable]] -> InvalidFetchSizeCode,
      classOf[LeaderNotAvailableException].asInstanceOf[Class[Throwable]] -> LeaderNotAvailableCode,
      classOf[NotLeaderForPartitionException].asInstanceOf[Class[Throwable]] -> NotLeaderForPartitionCode,
      classOf[RequestTimedOutException].asInstanceOf[Class[Throwable]] -> RequestTimedOutCode,
      classOf[BrokerNotAvailableException].asInstanceOf[Class[Throwable]] -> BrokerNotAvailableCode,
      classOf[ReplicaNotAvailableException].asInstanceOf[Class[Throwable]] -> ReplicaNotAvailableCode,
      classOf[MessageSizeTooLargeException].asInstanceOf[Class[Throwable]] -> MessageSizeTooLargeCode,
      classOf[ControllerMovedException].asInstanceOf[Class[Throwable]] -> StaleControllerEpochCode,
      classOf[OffsetMetadataTooLargeException].asInstanceOf[Class[Throwable]] -> OffsetMetadataTooLargeCode,
      classOf[OffsetsLoadInProgressException].asInstanceOf[Class[Throwable]] -> OffsetsLoadInProgressCode,
      classOf[ConsumerCoordinatorNotAvailableException].asInstanceOf[Class[Throwable]] -> ConsumerCoordinatorNotAvailableCode,
      classOf[NotCoordinatorForConsumerException].asInstanceOf[Class[Throwable]] -> NotCoordinatorForConsumerCode,
      classOf[InvalidTopicException].asInstanceOf[Class[Throwable]] -> InvalidTopicCode,
      classOf[MessageSetSizeTooLargeException].asInstanceOf[Class[Throwable]] -> MessageSetSizeTooLargeCode,
      classOf[NotEnoughReplicasException].asInstanceOf[Class[Throwable]] -> NotEnoughReplicasCode,
      classOf[NotEnoughReplicasAfterAppendException].asInstanceOf[Class[Throwable]] -> NotEnoughReplicasAfterAppendCode,
      classOf[TopicAuthorizationException].asInstanceOf[Class[Throwable]] -> TopicAuthorizationCode,
      classOf[GroupAuthorizationException].asInstanceOf[Class[Throwable]] -> GroupAuthorizationCode,
      classOf[ClusterAuthorizationException].asInstanceOf[Class[Throwable]] -> ClusterAuthorizationCode
    ).withDefaultValue(UnknownCode)

  /* invert the mapping */
  private val codeToException =
    (Map[Short, Class[Throwable]]() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf[UnknownException])

  def codeFor(exception: Class[Throwable]): Short = exceptionToCode(exception)

  def maybeThrowException(code: Short) =
    if(code != 0)
      throw codeToException(code).newInstance()

  def exceptionFor(code: Short) : Throwable = codeToException(code).newInstance()

  def exceptionNameFor(code: Short) : String = codeToException(code).getName()
}
