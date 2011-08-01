/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.nio.ByteBuffer
import org.apache.log4j.Logger

object CompressionUtils {
  private val logger = Logger.getLogger(getClass)

  def compress(messages: Iterable[Message]): Message = compress(messages, DefaultCompressionCodec)

  def compress(messages: Iterable[Message], compressionCodec: CompressionCodec):Message = compressionCodec match {
    case DefaultCompressionCodec =>
      val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream()
      val gzipOutput:GZIPOutputStream = new GZIPOutputStream(outputStream)
      if(logger.isDebugEnabled)
        logger.debug("Allocating message byte buffer of size = " + MessageSet.messageSetSize(messages))

      val messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      messages.foreach(m => m.serializeTo(messageByteBuffer))
      messageByteBuffer.rewind

      try {
        gzipOutput.write(messageByteBuffer.array)
      } catch {
        case e: IOException => logger.error("Error while writing to the GZIP output stream", e)
        if(gzipOutput != null) gzipOutput.close();
        if(outputStream != null) outputStream.close()
        throw e
      } finally {
        if(gzipOutput != null) gzipOutput.close()
        if(outputStream != null) outputStream.close()
      }

      val oneCompressedMessage:Message = new Message(outputStream.toByteArray, compressionCodec)
      oneCompressedMessage
    case GZIPCompressionCodec =>
      val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream()
      val gzipOutput:GZIPOutputStream = new GZIPOutputStream(outputStream)
      if(logger.isDebugEnabled)
        logger.debug("Allocating message byte buffer of size = " + MessageSet.messageSetSize(messages))

      val messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      messages.foreach(m => m.serializeTo(messageByteBuffer))
      messageByteBuffer.rewind

      try {
        gzipOutput.write(messageByteBuffer.array)
      } catch {
        case e: IOException => logger.error("Error while writing to the GZIP output stream", e)
        if(gzipOutput != null)
          gzipOutput.close()
        if(outputStream != null)
          outputStream.close()
        throw e
      } finally {
        if(gzipOutput != null)
          gzipOutput.close()
        if(outputStream != null)
          outputStream.close()
      }

      val oneCompressedMessage:Message = new Message(outputStream.toByteArray, compressionCodec)
      oneCompressedMessage
    case _ =>
      throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
  }

  def decompress(message: Message): ByteBufferMessageSet = message.compressionCodec match {
    case DefaultCompressionCodec =>
      val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream
      val inputStream:InputStream = new ByteBufferBackedInputStream(message.payload)
      val gzipIn:GZIPInputStream = new GZIPInputStream(inputStream)
      val intermediateBuffer = new Array[Byte](1024)

      try {
        Stream.continually(gzipIn.read(intermediateBuffer)).takeWhile(_ > 0).foreach { dataRead =>
          outputStream.write(intermediateBuffer, 0, dataRead)
        }
      }catch {
        case e: IOException => logger.error("Error while reading from the GZIP input stream", e)
        if(gzipIn != null) gzipIn.close
        if(outputStream != null) outputStream.close
        throw e
      } finally {
        if(gzipIn != null) gzipIn.close
        if(outputStream != null) outputStream.close
      }

      val outputBuffer = ByteBuffer.allocate(outputStream.size)
      outputBuffer.put(outputStream.toByteArray)
      outputBuffer.rewind
      val outputByteArray = outputStream.toByteArray
      new ByteBufferMessageSet(outputBuffer)
    case GZIPCompressionCodec =>
      val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream
      val inputStream:InputStream = new ByteBufferBackedInputStream(message.payload)
      val gzipIn:GZIPInputStream = new GZIPInputStream(inputStream)
      val intermediateBuffer = new Array[Byte](1024)

      try {
        Stream.continually(gzipIn.read(intermediateBuffer)).takeWhile(_ > 0).foreach { dataRead =>
          outputStream.write(intermediateBuffer, 0, dataRead)
        }
      }catch {
        case e: IOException => logger.error("Error while reading from the GZIP input stream", e)
        if(gzipIn != null) gzipIn.close
        if(outputStream != null) outputStream.close
        throw e
      } finally {
        if(gzipIn != null) gzipIn.close
        if(outputStream != null) outputStream.close
      }

      val outputBuffer = ByteBuffer.allocate(outputStream.size)
      outputBuffer.put(outputStream.toByteArray)
      outputBuffer.rewind
      val outputByteArray = outputStream.toByteArray
      new ByteBufferMessageSet(outputBuffer)
    case _ =>
      throw new kafka.common.UnknownCodecException("Unknown Codec: " + message.compressionCodec)
  }
}
