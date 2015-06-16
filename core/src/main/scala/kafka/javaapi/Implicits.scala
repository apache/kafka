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
package kafka.javaapi

import kafka.utils.Logging

private[javaapi] object Implicits extends Logging {

  implicit def scalaMessageSetToJavaMessageSet(messageSet: kafka.message.ByteBufferMessageSet):
     kafka.javaapi.message.ByteBufferMessageSet = {
    new kafka.javaapi.message.ByteBufferMessageSet(messageSet.buffer)
  }

  implicit def toJavaFetchResponse(response: kafka.api.FetchResponse): kafka.javaapi.FetchResponse =
    new kafka.javaapi.FetchResponse(response)

  implicit def toJavaTopicMetadataResponse(response: kafka.api.TopicMetadataResponse): kafka.javaapi.TopicMetadataResponse =
    new kafka.javaapi.TopicMetadataResponse(response)

  implicit def toJavaOffsetResponse(response: kafka.api.OffsetResponse): kafka.javaapi.OffsetResponse =
    new kafka.javaapi.OffsetResponse(response)

  implicit def toJavaOffsetFetchResponse(response: kafka.api.OffsetFetchResponse): kafka.javaapi.OffsetFetchResponse =
    new kafka.javaapi.OffsetFetchResponse(response)

  implicit def toJavaOffsetCommitResponse(response: kafka.api.OffsetCommitResponse): kafka.javaapi.OffsetCommitResponse =
    new kafka.javaapi.OffsetCommitResponse(response)

  implicit def optionToJavaRef[T](opt: Option[T]): T = {
    opt match {
      case Some(obj) => obj
      case None => null.asInstanceOf[T]
    }
  }

}
