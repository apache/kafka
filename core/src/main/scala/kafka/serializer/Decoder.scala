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

package kafka.serializer

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import kafka.utils.VerifiableProperties

/**
 * A decoder is a method of turning byte arrays into objects.
 * An implementation is required to provide a constructor that
 * takes a VerifiableProperties instance.
 */
trait Decoder[T] {
  def fromBytes(bytes: Array[Byte]): T
}

/**
 * The default implementation does nothing, just returns the same byte array it takes in.
 */
class DefaultDecoder(props: VerifiableProperties = null) extends Decoder[Array[Byte]] {
  def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
}

/**
 * The string decoder translates bytes into strings. It uses UTF8 by default but takes
 * an optional property serializer.encoding to control this.
 */
class StringDecoder(props: VerifiableProperties = null) extends Decoder[String] {
  val encoding: String =
    if (props == null)
      StandardCharsets.UTF_8.name()
    else
      props.getString("serializer.encoding", StandardCharsets.UTF_8.name())

  def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, encoding)
  }
}

/**
  * The long decoder translates bytes into longs.
  */
class LongDecoder(props: VerifiableProperties = null) extends Decoder[Long] {
  def fromBytes(bytes: Array[Byte]): Long = {
    ByteBuffer.wrap(bytes).getLong
  }
}

/**
  * The integer decoder translates bytes into integers.
  */
class IntegerDecoder(props: VerifiableProperties = null) extends Decoder[Integer] {
  def fromBytes(bytes: Array[Byte]): Integer = {
    ByteBuffer.wrap(bytes).getInt()
  }
}
