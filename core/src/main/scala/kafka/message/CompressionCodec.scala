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

import java.util.Locale

import kafka.common.UnknownCodecException

object CompressionCodec {
  def getCompressionCodec(codec: Int): CompressionCodec = {
    codec match {
      case NoCompressionCodec.codec => NoCompressionCodec
      case GZIPCompressionCodec.codec => GZIPCompressionCodec
      case SnappyCompressionCodec.codec => SnappyCompressionCodec
      case LZ4CompressionCodec.codec => LZ4CompressionCodec
      case ZStdCompressionCodec.codec => ZStdCompressionCodec
      case _ => throw new UnknownCodecException("%d is an unknown compression codec".format(codec))
    }
  }
  def getCompressionCodec(name: String): CompressionCodec = {
    name.toLowerCase(Locale.ROOT) match {
      case NoCompressionCodec.name => NoCompressionCodec
      case GZIPCompressionCodec.name => GZIPCompressionCodec
      case SnappyCompressionCodec.name => SnappyCompressionCodec
      case LZ4CompressionCodec.name => LZ4CompressionCodec
      case ZStdCompressionCodec.name => ZStdCompressionCodec
      case _ => throw new kafka.common.UnknownCodecException("%s is an unknown compression codec".format(name))
    }
  }
}

object BrokerCompressionCodec {

  val brokerCompressionCodecs = List(UncompressedCodec, ZStdCompressionCodec, LZ4CompressionCodec, SnappyCompressionCodec, GZIPCompressionCodec, ProducerCompressionCodec)
  val brokerCompressionOptions: List[String] = brokerCompressionCodecs.map(codec => codec.name)

  def isValid(compressionType: String): Boolean = brokerCompressionOptions.contains(compressionType.toLowerCase(Locale.ROOT))

  def getCompressionCodec(compressionType: String): CompressionCodec = {
    compressionType.toLowerCase(Locale.ROOT) match {
      case UncompressedCodec.name => NoCompressionCodec
      case _ => CompressionCodec.getCompressionCodec(compressionType)
    }
  }

  def getTargetCompressionCodec(compressionType: String, producerCompression: CompressionCodec): CompressionCodec = {
    if (ProducerCompressionCodec.name.equals(compressionType))
      producerCompression
    else
      getCompressionCodec(compressionType)
  }
}

sealed trait CompressionCodec { def codec: Int; def name: String }
sealed trait BrokerCompressionCodec { def name: String }

case object DefaultCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec: Int = GZIPCompressionCodec.codec
  val name: String = GZIPCompressionCodec.name
}

case object GZIPCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 1
  val name = "gzip"
}

case object SnappyCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 2
  val name = "snappy"
}

case object LZ4CompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 3
  val name = "lz4"
}

case object ZStdCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 4
  val name = "zstd"
}

case object NoCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 0
  val name = "none"
}

case object UncompressedCodec extends BrokerCompressionCodec {
  val name = "uncompressed"
}

case object ProducerCompressionCodec extends BrokerCompressionCodec {
  val name = "producer"
}
