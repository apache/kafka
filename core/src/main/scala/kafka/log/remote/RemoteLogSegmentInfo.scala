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
package kafka.log.remote

import org.apache.kafka.common.TopicPartition

/**
 * This class represents information about a remote log segment.
 *
 * @param baseOffset     baseOffset of this segment
 * @param lastOffset     last offset of this segment
 * @param topicPartition topic partition of this segment
 * @param props          any custom props to be stored by RemoteStorageManager, which can be used later when it
 *                       is passed through different methods in RemoteStorageManager.
 */
case class RemoteLogSegmentInfo(baseOffset: Long, lastOffset: Long, topicPartition: TopicPartition, leaderEpoch:Int,
                                props: java.util.Map[String, _]) {
}
