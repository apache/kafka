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

import kafka.utils.VerifiableProperties

/**
 * An encoder is a method of turning objects into byte arrays.
 * An implementation is required to provide a constructor that
 * takes a VerifiableProperties instance.
 */
trait Encoder[T] {
  def toBytes(t: T): Array[Byte]
}

/**
 * The default implementation is a no-op, it just returns the same array it takes in
 */
class DefaultEncoder(props: VerifiableProperties = null) extends Encoder[Array[Byte]] {
  override def toBytes(value: Array[Byte]): Array[Byte] = value
}

class NullEncoder[T](props: VerifiableProperties = null) extends Encoder[T] {
  override def toBytes(value: T): Array[Byte] = null
}

/**
 * The string encoder takes an optional parameter serializer.encoding which controls
 * the character set used in encoding the string into bytes.
 */
class StringEncoder(props: VerifiableProperties = null) extends Encoder[String] {
  val encoding = 
    if(props == null) 
      "UTF8" 
    else 
      props.getString("serializer.encoding", "UTF8")
  
  override def toBytes(s: String): Array[Byte] = 
    if(s == null)
      null
    else
      s.getBytes(encoding)
}
