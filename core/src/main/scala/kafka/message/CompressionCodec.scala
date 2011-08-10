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

package kafka.message

object CompressionCodec {
  def getCompressionCodec(codec: Int): CompressionCodec = {
    codec match {
      case 0 => NoCompressionCodec
      case 1 => GZIPCompressionCodec
      case _ => throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(codec))
    }
  }
}

sealed trait CompressionCodec { def codec: Int }

case object DefaultCompressionCodec extends CompressionCodec { val codec = 1 }

case object GZIPCompressionCodec extends CompressionCodec { val codec = 1 }

case object NoCompressionCodec extends CompressionCodec { val codec = 0 }
