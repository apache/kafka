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
package kafka

import joptsimple.OptionParser
import kafka.message.{CompressionCodec, NoCompressionCodec}
import kafka.utils.CommandLineUtils
import kafka.utils.Implicits.PropertiesOps
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.record.{CompressionConfig, CompressionType, DefaultRecordBatch, MemoryRecords, SimpleRecord, TimestampType}
import org.apache.kafka.common.serialization.ByteArraySerializer

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.{Properties, Random}
import scala.jdk.CollectionConverters._

object TestCompression {
  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val dirOpt = parser.accepts("dir", "The directory that contains the uncompressed messages.")
      .withRequiredArg
      .describedAs("path")
      .ofType(classOf[java.lang.String])
    val msgSizeOpt = parser.accepts("msg-size", "The size of the random-generated message.")
      .withRequiredArg
      .describedAs("num_bytes")
      .ofType(classOf[java.lang.Integer])
    val batchSizeOpt = parser.accepts("batch-size", "The number of the messages in a batch.")
      .withRequiredArg
      .describedAs("num_count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10)
    val batchCountOpt = parser.accepts("batch-count", "The number of the batches to test.")
      .withRequiredArg
      .describedAs("num_count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)
    val compressionCodecOpt = parser.accepts("compression", "The compression codec to use")
      .withRequiredArg
      .describedAs("codec")
      .ofType(classOf[java.lang.String])
      .defaultsTo(NoCompressionCodec.name)
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
      .withRequiredArg
      .describedAs("extra_prop")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options)

    val properties = new Properties()
    val extraProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt).asScala)
    properties ++= extraProps
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOpt))
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.forId(compressionCodec.codec).name)
    val producerConfig = new ProducerConfig(properties)

    // Generate SimpleRecord instances
    val batchCount = options.valueOf(batchCountOpt)
    val batchSize = options.valueOf(batchSizeOpt)
    val records: Seq[SimpleRecord] = if (options.has(dirOpt)) {
      // Read record payloads from files
      val dir = options.valueOf(dirOpt)
      val payloads = new File(dir).listFiles.toSeq.filter(_.isFile)
        .take(batchSize * batchCount).map { f: File =>
        Files.readAllBytes(f.toPath)
      }
      val fileSizes = payloads.map(_.length.toDouble)
      val fileAvg = fileSizes.sum / fileSizes.length.toDouble
      System.out.println(s"Load files: (min, avg, max) = (${fileSizes.min}, $fileAvg, ${fileSizes.max}) bytes")
      payloads.map(new SimpleRecord(0L, null, _))
    } else {
      // Generate record payloads randomly
      val msgSize = options.valueOf(msgSizeOpt)
      val rand = new Random()
      (0 until batchSize * batchCount).map { _ =>
        val array = new Array[Byte](msgSize)
        rand.nextBytes(array)
        new SimpleRecord(0L, null, array)
      }
    }
    val batches = records.sliding(batchCount, batchCount).toSeq

    // uncompressed
    val uncompressedSizes = batches.map { records: Seq[SimpleRecord] =>
      val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
      val builder = MemoryRecords.builder(
        buf, CompressionConfig.NONE, TimestampType.CREATE_TIME, 0L)
      records.foreach(builder.append)
      builder.build
      builder
    }.map(builder => builder.buffer().position().toDouble)
    val uncompressedAvg = uncompressedSizes.sum / uncompressedSizes.length.toDouble
    println(s"Uncompressed Size (avg): $uncompressedAvg bytes.")

    // compressed
    val start = System.nanoTime()
    val compressedSizes = batches.map { records: Seq[SimpleRecord] =>
      val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
      val builder = MemoryRecords.builder(
        buf, producerConfig.getCompressionConfig, TimestampType.CREATE_TIME, 0L)
      records.foreach(builder.append)
      builder.build
      builder
    }.map(builder => builder.buffer().position().toDouble)
    val end = System.nanoTime()
    val compressedAvg = compressedSizes.sum / compressedSizes.length.toDouble
    println(s"Compressed Size (avg): $compressedAvg bytes.")
    println(s"Elapsed: ${(end - start) / (1000.0 * 1000.0 * 1000.0)} sec.")
  }
}