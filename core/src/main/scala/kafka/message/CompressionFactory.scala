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

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.kafka.common.record.{BufferSupplier, KafkaLZ4BlockInputStream, KafkaLZ4BlockOutputStream}

object CompressionFactory {
  
  def apply(compressionCodec: CompressionCodec, messageVersion: Byte, stream: OutputStream): OutputStream = {
    compressionCodec match {
      case DefaultCompressionCodec => new GZIPOutputStream(stream)
      case GZIPCompressionCodec => new GZIPOutputStream(stream)
      case SnappyCompressionCodec => 
        import org.xerial.snappy.SnappyOutputStream
        new SnappyOutputStream(stream)
      case LZ4CompressionCodec =>
        new KafkaLZ4BlockOutputStream(stream, messageVersion == Message.MagicValue_V0)
      case _ =>
        throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
    }
  }
  
  def apply(compressionCodec: CompressionCodec, messageVersion: Byte, buffer: ByteBuffer): InputStream = {
    compressionCodec match {
      case DefaultCompressionCodec => new GZIPInputStream(new ByteBufferBackedInputStream(buffer))
      case GZIPCompressionCodec => new GZIPInputStream(new ByteBufferBackedInputStream(buffer))
      case SnappyCompressionCodec => 
        import org.xerial.snappy.SnappyInputStream
        new SnappyInputStream(new ByteBufferBackedInputStream(buffer))
      case LZ4CompressionCodec =>
        new KafkaLZ4BlockInputStream(buffer, BufferSupplier.NO_CACHING, messageVersion == Message.MagicValue_V0)
      case _ =>
        throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
    }
  }
}
