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

import kafka.common.{TopicAndPartition, ErrorMapping}
import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import collection.mutable.HashMap
import collection.Map


object UpdateMetadataResponse {
  def readFrom(buffer: ByteBuffer): UpdateMetadataResponse = {
    val correlationId = buffer.getInt
    val errorCode = buffer.getShort
    new UpdateMetadataResponse(correlationId, errorCode)
  }
}

case class UpdateMetadataResponse(correlationId: Int,
                                  errorCode: Short = ErrorMapping.NoError)
  extends RequestOrResponse() {
  def sizeInBytes(): Int = 4 /* correlation id */ + 2 /* error code */

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    buffer.putShort(errorCode)
  }

  override def describe(details: Boolean):String = { toString }
}