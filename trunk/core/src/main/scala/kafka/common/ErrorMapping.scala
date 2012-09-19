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

/**
 * A bi-directional mapping between error codes and exceptions x  
 */
object ErrorMapping {
  val EmptyByteBuffer = ByteBuffer.allocate(0)

  val UnknownCode = -1
  val NoError = 0
  val OffsetOutOfRangeCode = 1
  val InvalidMessageCode = 2
  val WrongPartitionCode = 3
  val InvalidFetchSizeCode = 4

  private val exceptionToCode = 
    Map[Class[Throwable], Int](
      classOf[OffsetOutOfRangeException].asInstanceOf[Class[Throwable]] -> OffsetOutOfRangeCode,
      classOf[InvalidMessageException].asInstanceOf[Class[Throwable]] -> InvalidMessageCode,
      classOf[InvalidPartitionException].asInstanceOf[Class[Throwable]] -> WrongPartitionCode,
      classOf[InvalidMessageSizeException].asInstanceOf[Class[Throwable]] -> InvalidFetchSizeCode
    ).withDefaultValue(UnknownCode)
  
  /* invert the mapping */
  private val codeToException = 
    (Map[Int, Class[Throwable]]() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf[UnknownException])
  
  def codeFor(exception: Class[Throwable]): Int = exceptionToCode(exception)
  
  def maybeThrowException(code: Int) =
    if(code != 0)
      throw codeToException(code).newInstance()
}

class InvalidTopicException(message: String) extends RuntimeException(message) {
  def this() = this(null)  
}

class MessageSizeTooLargeException(message: String) extends RuntimeException(message) {
}
