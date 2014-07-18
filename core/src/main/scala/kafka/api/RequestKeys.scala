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

package kafka.api

import kafka.common.KafkaException
import java.nio.ByteBuffer

object RequestKeys {
  val ProduceKey: Short = 0
  val FetchKey: Short = 1
  val OffsetsKey: Short = 2
  val MetadataKey: Short = 3
  val LeaderAndIsrKey: Short = 4
  val StopReplicaKey: Short = 5
  val UpdateMetadataKey: Short = 6
  val ControlledShutdownKey: Short = 7
  val OffsetCommitKey: Short = 8
  val OffsetFetchKey: Short = 9
  val ConsumerMetadataKey: Short = 10
  val JoinGroupKey: Short = 11
  val HeartbeatKey: Short = 12

  val keyToNameAndDeserializerMap: Map[Short, (String, (ByteBuffer) => RequestOrResponse)]=
    Map(ProduceKey -> ("Produce", ProducerRequest.readFrom),
        FetchKey -> ("Fetch", FetchRequest.readFrom),
        OffsetsKey -> ("Offsets", OffsetRequest.readFrom),
        MetadataKey -> ("Metadata", TopicMetadataRequest.readFrom),
        LeaderAndIsrKey -> ("LeaderAndIsr", LeaderAndIsrRequest.readFrom),
        StopReplicaKey -> ("StopReplica", StopReplicaRequest.readFrom),
        UpdateMetadataKey -> ("UpdateMetadata", UpdateMetadataRequest.readFrom),
        ControlledShutdownKey -> ("ControlledShutdown", ControlledShutdownRequest.readFrom),
        OffsetCommitKey -> ("OffsetCommit", OffsetCommitRequest.readFrom),
        OffsetFetchKey -> ("OffsetFetch", OffsetFetchRequest.readFrom),
        ConsumerMetadataKey -> ("ConsumerMetadata", ConsumerMetadataRequest.readFrom),
        JoinGroupKey -> ("JoinGroup", JoinGroupRequestAndHeader.readFrom),
        HeartbeatKey -> ("Heartbeat", HeartbeatRequestAndHeader.readFrom)
    )

  def nameForKey(key: Short): String = {
    keyToNameAndDeserializerMap.get(key) match {
      case Some(nameAndSerializer) => nameAndSerializer._1
      case None => throw new KafkaException("Wrong request type %d".format(key))
    }
  }

  def deserializerForKey(key: Short): (ByteBuffer) => RequestOrResponse = {
    keyToNameAndDeserializerMap.get(key) match {
      case Some(nameAndSerializer) => nameAndSerializer._2
      case None => throw new KafkaException("Wrong request type %d".format(key))
    }
  }
}
