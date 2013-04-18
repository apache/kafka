package kafka.api

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

import java.nio._
import kafka.network.RequestChannel
import kafka.utils.Logging

object Request {
  val OrdinaryConsumerId: Int = -1
  val DebuggingConsumerId: Int = -2
}


private[kafka] abstract class RequestOrResponse(val requestId: Option[Short] = None, val correlationId: Int) extends Logging{

  def sizeInBytes: Int
  
  def writeTo(buffer: ByteBuffer): Unit

  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {}
}

