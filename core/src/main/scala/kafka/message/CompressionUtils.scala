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

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import kafka.utils._

abstract sealed class CompressionFacade(inputStream: InputStream, outputStream: ByteArrayOutputStream) {
  def close() = {
    if (inputStream != null) inputStream.close()
    if (outputStream != null) outputStream.close()
  }	
  def read(a: Array[Byte]): Int
  def write(a: Array[Byte])
}

class GZIPCompression(inputStream: InputStream, outputStream: ByteArrayOutputStream)  extends CompressionFacade(inputStream,outputStream) {
  import java.util.zip.GZIPInputStream
  import java.util.zip.GZIPOutputStream
  val gzipIn:GZIPInputStream = if (inputStream == null) null else new  GZIPInputStream(inputStream)
  val gzipOut:GZIPOutputStream = if (outputStream == null) null else new  GZIPOutputStream(outputStream)

  override def close() {
    if (gzipIn != null) gzipIn.close()
    if (gzipOut != null) gzipOut.close()
    super.close()	
  }

  override def write(a: Array[Byte]) = {
    gzipOut.write(a)
  }

  override def read(a: Array[Byte]): Int = {
    gzipIn.read(a)
  }
}

class SnappyCompression(inputStream: InputStream,outputStream: ByteArrayOutputStream)  extends CompressionFacade(inputStream,outputStream) {
  import org.xerial.snappy.SnappyInputStream
  import org.xerial.snappy.SnappyOutputStream
  
  val snappyIn:SnappyInputStream = if (inputStream == null) null else new SnappyInputStream(inputStream)
  val snappyOut:SnappyOutputStream = if (outputStream == null) null else new  SnappyOutputStream(outputStream)

  override def close() = {
    if (snappyIn != null) snappyIn.close()
    if (snappyOut != null) snappyOut.close()
    super.close()	
  }

  override def write(a: Array[Byte]) = {
    snappyOut.write(a)
  }

  override def read(a: Array[Byte]): Int = {
    snappyIn.read(a)	
  }

}

object CompressionFactory {
  def apply(compressionCodec: CompressionCodec, stream: ByteArrayOutputStream): CompressionFacade = compressionCodec match {
    case GZIPCompressionCodec => new GZIPCompression(null,stream)
    case SnappyCompressionCodec => new SnappyCompression(null,stream)
    case _ =>
      throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
  }
  def apply(compressionCodec: CompressionCodec, stream: InputStream): CompressionFacade = compressionCodec match {
    case GZIPCompressionCodec => new GZIPCompression(stream,null)
    case SnappyCompressionCodec => new SnappyCompression(stream,null)
    case _ =>
      throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
  }
}

object CompressionUtils extends Logging{

  //specify the codec which is the default when DefaultCompressionCodec is used
  private var defaultCodec: CompressionCodec = GZIPCompressionCodec

  def compress(messages: Iterable[Message], compressionCodec: CompressionCodec = DefaultCompressionCodec):Message = {
	val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream()
	
	debug("Allocating message byte buffer of size = " + MessageSet.messageSetSize(messages))

    var cf: CompressionFacade = null
		
	if (compressionCodec == DefaultCompressionCodec)
      cf = CompressionFactory(defaultCodec,outputStream)
    else 
      cf = CompressionFactory(compressionCodec,outputStream) 

    val messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
    messages.foreach(m => m.serializeTo(messageByteBuffer))
    messageByteBuffer.rewind

    try {
      cf.write(messageByteBuffer.array)
    } catch {
      case e: IOException => error("Error while writing to the GZIP output stream", e)
      cf.close()
      throw e
    } finally {
      cf.close()
    }

    val oneCompressedMessage:Message = new Message(outputStream.toByteArray, compressionCodec)
    oneCompressedMessage
   }

  def decompress(message: Message): ByteBufferMessageSet = {
    val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream
    val inputStream:InputStream = new ByteBufferBackedInputStream(message.payload)

    val intermediateBuffer = new Array[Byte](1024)

    var cf: CompressionFacade = null
		
	if (message.compressionCodec == DefaultCompressionCodec) 
      cf = CompressionFactory(defaultCodec,inputStream)
    else 
      cf = CompressionFactory(message.compressionCodec,inputStream)

    try {
      Stream.continually(cf.read(intermediateBuffer)).takeWhile(_ > 0).foreach { dataRead =>
        outputStream.write(intermediateBuffer, 0, dataRead)
      }
    }catch {
      case e: IOException => error("Error while reading from the GZIP input stream", e)
      cf.close()
      throw e
    } finally {
      cf.close()
    }

    val outputBuffer = ByteBuffer.allocate(outputStream.size)
    outputBuffer.put(outputStream.toByteArray)
    outputBuffer.rewind
    val outputByteArray = outputStream.toByteArray
    new ByteBufferMessageSet(outputBuffer)
  }
}
