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

import java.nio.ByteBuffer
import kafka.network.Request

object MultiProducerRequest {
  def readFrom(buffer: ByteBuffer): MultiProducerRequest = {
    val count = buffer.getShort
    val produces = new Array[ProducerRequest](count)
    for(i <- 0 until produces.length)
      produces(i) = ProducerRequest.readFrom(buffer)
    new MultiProducerRequest(produces)
  }
}

class MultiProducerRequest(val produces: Array[ProducerRequest]) extends Request(RequestKeys.MultiProduce) {
  def writeTo(buffer: ByteBuffer) {
    if(produces.length > Short.MaxValue)
      throw new IllegalArgumentException("Number of requests in MultiProducer exceeds " + Short.MaxValue + ".")    
    buffer.putShort(produces.length.toShort)
    for(produce <- produces)
      produce.writeTo(buffer)
  }

  def sizeInBytes: Int = {
    var size = 2
    for(produce <- produces)
      size += produce.sizeInBytes
    size
  }

  override def toString(): String = {
    val buffer = new StringBuffer
    for(produce <- produces) {
      buffer.append(produce.toString)
      buffer.append(",")
    }
    buffer.toString
  }  
}
