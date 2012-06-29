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

import kafka.message.InvalidMessageException
import java.nio.ByteBuffer
import java.lang.Throwable
import scala.Predef._

/**
 * A bi-directional mapping between error codes and exceptions x  
 */
object ErrorMapping {
  val EmptyByteBuffer = ByteBuffer.allocate(0)

  val UnknownCode : Short = -1
  val NoError : Short = 0
  val OffsetOutOfRangeCode : Short = 1
  val InvalidMessageCode : Short = 2
  val InvalidPartitionCode : Short = 3
  val InvalidFetchSizeCode  : Short = 4
  val InvalidFetchRequestFormatCode : Short = 5
  val NoLeaderForPartitionCode : Short = 6
  val NotLeaderForPartitionCode : Short = 7
  val UnknownTopicCode : Short = 8
  val RequestTimedOutCode: Short = 9

  private val exceptionToCode = 
    Map[Class[Throwable], Short](
      classOf[OffsetOutOfRangeException].asInstanceOf[Class[Throwable]] -> OffsetOutOfRangeCode,
      classOf[InvalidMessageException].asInstanceOf[Class[Throwable]] -> InvalidMessageCode,
      classOf[InvalidPartitionException].asInstanceOf[Class[Throwable]] -> InvalidPartitionCode,
      classOf[InvalidMessageSizeException].asInstanceOf[Class[Throwable]] -> InvalidFetchSizeCode,
      classOf[FetchRequestFormatException].asInstanceOf[Class[Throwable]] -> InvalidFetchRequestFormatCode,
      classOf[NotLeaderForPartitionException].asInstanceOf[Class[Throwable]] -> NotLeaderForPartitionCode,
      classOf[NoLeaderForPartitionException].asInstanceOf[Class[Throwable]] -> NoLeaderForPartitionCode,
      classOf[RequestTimedOutException].asInstanceOf[Class[Throwable]] -> RequestTimedOutCode,
      classOf[UnknownTopicException].asInstanceOf[Class[Throwable]] -> UnknownTopicCode
    ).withDefaultValue(UnknownCode)
  
  /* invert the mapping */
  private val codeToException = 
    (Map[Short, Class[Throwable]]() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf[UnknownException])
  
  def codeFor(exception: Class[Throwable]): Short = exceptionToCode(exception)
  
  def maybeThrowException(code: Short) =
    if(code != 0)
      throw codeToException(code).newInstance()

  def exceptionFor(code: Short) : Throwable = codeToException(code).newInstance()
}

class InvalidTopicException(message: String) extends RuntimeException(message) {
  def this() = this(null)  
}

class MessageSizeTooLargeException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}
