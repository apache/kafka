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

import kafka.common.ErrorMapping
import java.nio.ByteBuffer
import kafka.utils.Utils
import collection.mutable.HashMap
import collection.Map


object LeaderAndISRResponse {
  def readFrom(buffer: ByteBuffer): LeaderAndISRResponse = {
    val versionId = buffer.getShort
    val errorCode = buffer.getShort
    val numEntries = buffer.getInt
    val responseMap = new HashMap[(String, Int), Short]()
    for (i<- 0 until numEntries){
      val topic = Utils.readShortString(buffer, "UTF-8")
      val partition = buffer.getInt
      val partitionErrorCode = buffer.getShort
      responseMap.put((topic, partition), partitionErrorCode)
    }
    new LeaderAndISRResponse(versionId, responseMap, errorCode)
  }
}


case class LeaderAndISRResponse(versionId: Short,
                                responseMap: Map[(String, Int), Short],
                                errorCode: Short = ErrorMapping.NoError)
        extends RequestOrResponse{
  def sizeInBytes(): Int ={
    var size = 2 + 2 + 4
    for ((key, value) <- responseMap){
      size += 2 + key._1.length + 4 + 2
    }
    size
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putShort(errorCode)
    buffer.putInt(responseMap.size)
    for ((key:(String, Int), value) <- responseMap){
      Utils.writeShortString(buffer, key._1, "UTF-8")
      buffer.putInt(key._2)
      buffer.putShort(value)
    }
  }
}