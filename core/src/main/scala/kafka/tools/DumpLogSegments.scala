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

package kafka.tools

import java.io._
import java.nio.ByteBuffer

import kafka.coordinator.group.{GroupMetadataKey, GroupMetadataManager, OffsetKey}
import kafka.coordinator.transaction.TransactionLog
import kafka.log._
import kafka.serializer.Decoder
import kafka.utils._
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object DumpLogSegments {

  // visible for testing
  private[tools] val RecordIndent = "|"

  def main(args: Array[String]) {
    val opts = new DumpLogSegmentsOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to parse a log file and dump its contents to the console, useful for debugging a seemingly corrupt log segment.")
    opts.checkArgs()

    val misMatchesForIndexFilesMap = mutable.Map[String, List[(Long, Long)]]()
    val timeIndexDumpErrors = new TimeIndexDumpErrors
    val nonConsecutivePairsForLogFilesMap = mutable.Map[String, List[(Long, Long)]]()

    for (arg <- opts.files) {
      val file = new File(arg)
      println(s"Dumping $file")

      val filename = file.getName
      val suffix = filename.substring(filename.lastIndexOf("."))
      suffix match {
        case Log.LogFileSuffix =>
          dumpLog(file, opts.shouldPrintDataLog, nonConsecutivePairsForLogFilesMap, opts.isDeepIteration,
            opts.maxMessageSize, opts.messageParser)
        case Log.IndexFileSuffix =>
          dumpIndex(file, opts.indexSanityOnly, opts.verifyOnly, misMatchesForIndexFilesMap, opts.maxMessageSize)
        case Log.TimeIndexFileSuffix =>
          dumpTimeIndex(file, opts.indexSanityOnly, opts.verifyOnly, timeIndexDumpErrors, opts.maxMessageSize)
        case Log.ProducerSnapshotFileSuffix =>
          dumpProducerIdSnapshot(file)
        case Log.TxnIndexFileSuffix =>
          dumpTxnIndex(file)
        case _ =>
          System.err.println(s"Ignoring unknown file $file")
      }
    }

    misMatchesForIndexFilesMap.foreach { case (fileName, listOfMismatches) =>
      System.err.println(s"Mismatches in :$fileName")
      listOfMismatches.foreach { case (indexOffset, logOffset) =>
        System.err.println(s"  Index offset: $indexOffset, log offset: $logOffset")
      }
    }

    timeIndexDumpErrors.printErrors()

    nonConsecutivePairsForLogFilesMap.foreach { case (fileName, listOfNonConsecutivePairs) =>
      System.err.println(s"Non-consecutive offsets in $fileName")
      listOfNonConsecutivePairs.foreach { case (first, second) =>
        System.err.println(s"  $first is followed by $second")
      }
    }
  }

  private def dumpTxnIndex(file: File): Unit = {
    val index = new TransactionIndex(Log.offsetFromFile(file), file)
    for (abortedTxn <- index.allAbortedTxns) {
      println(s"version: ${abortedTxn.version} producerId: ${abortedTxn.producerId} firstOffset: ${abortedTxn.firstOffset} " +
        s"lastOffset: ${abortedTxn.lastOffset} lastStableOffset: ${abortedTxn.lastStableOffset}")
    }
  }

  private def dumpProducerIdSnapshot(file: File): Unit = {
    try {
      ProducerStateManager.readSnapshot(file).foreach { entry =>
        print(s"producerId: ${entry.producerId} producerEpoch: ${entry.producerEpoch} " +
          s"coordinatorEpoch: ${entry.coordinatorEpoch} currentTxnFirstOffset: ${entry.currentTxnFirstOffset} ")
        entry.batchMetadata.headOption.foreach { metadata =>
          print(s"firstSequence: ${metadata.firstSeq} lastSequence: ${metadata.lastSeq} " +
            s"lastOffset: ${metadata.lastOffset} offsetDelta: ${metadata.offsetDelta} timestamp: ${metadata.timestamp}")
        }
        println()
      }
    } catch {
      case e: CorruptSnapshotException =>
        System.err.println(e.getMessage)
    }
  }

  /* print out the contents of the index */
  // Visible for testing
  private[tools] def dumpIndex(file: File,
                               indexSanityOnly: Boolean,
                               verifyOnly: Boolean,
                               misMatchesForIndexFilesMap: mutable.Map[String, List[(Long, Long)]],
                               maxMessageSize: Int) {
    val startOffset = file.getName.split("\\.")(0).toLong
    val logFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + Log.LogFileSuffix)
    val fileRecords = FileRecords.open(logFile, false)
    val index = new OffsetIndex(file, baseOffset = startOffset, writable = false)

    //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
    if (indexSanityOnly) {
      index.sanityCheck()
      println(s"$file passed sanity check.")
      return
    }

    for (i <- 0 until index.entries) {
      val entry = index.entry(i)

      // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
      if (entry.offset == index.baseOffset && i > 0)
        return

      val slice = fileRecords.slice(entry.position, maxMessageSize)
      val firstBatchLastOffset = slice.batches.iterator.next().lastOffset
      if (firstBatchLastOffset != entry.offset) {
        var misMatchesSeq = misMatchesForIndexFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
        misMatchesSeq ::= (entry.offset, firstBatchLastOffset)
        misMatchesForIndexFilesMap.put(file.getAbsolutePath, misMatchesSeq)
      }
      if (!verifyOnly)
        println(s"offset: ${entry.offset} position: ${entry.position}")
    }
  }

  // Visible for testing
  private[tools] def dumpTimeIndex(file: File,
                                   indexSanityOnly: Boolean,
                                   verifyOnly: Boolean,
                                   timeIndexDumpErrors: TimeIndexDumpErrors,
                                   maxMessageSize: Int) {
    val startOffset = file.getName.split("\\.")(0).toLong
    val logFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + Log.LogFileSuffix)
    val fileRecords = FileRecords.open(logFile, false)
    val indexFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + Log.IndexFileSuffix)
    val index = new OffsetIndex(indexFile, baseOffset = startOffset, writable = false)
    val timeIndex = new TimeIndex(file, baseOffset = startOffset, writable = false)

    try {
      //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
      if (indexSanityOnly) {
        timeIndex.sanityCheck()
        println(s"$file passed sanity check.")
        return
      }

      var prevTimestamp = RecordBatch.NO_TIMESTAMP
      for (i <- 0 until timeIndex.entries) {
        val entry = timeIndex.entry(i)

        // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
        if (entry.offset == timeIndex.baseOffset && i > 0)
          return

        val position = index.lookup(entry.offset).position
        val partialFileRecords = fileRecords.slice(position, Int.MaxValue)
        val batches = partialFileRecords.batches.asScala
        var maxTimestamp = RecordBatch.NO_TIMESTAMP
        // We first find the message by offset then check if the timestamp is correct.
        batches.find(_.lastOffset >= entry.offset) match {
          case None =>
            timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset,
              -1.toLong)
          case Some(batch) if batch.lastOffset != entry.offset =>
            timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset, batch.lastOffset)
          case Some(batch) =>
            for (record <- batch.asScala)
              maxTimestamp = math.max(maxTimestamp, record.timestamp)

            if (maxTimestamp != entry.timestamp)
              timeIndexDumpErrors.recordMismatchTimeIndex(file, entry.timestamp, maxTimestamp)

            if (prevTimestamp >= entry.timestamp)
              timeIndexDumpErrors.recordOutOfOrderIndexTimestamp(file, entry.timestamp, prevTimestamp)
        }
        if (!verifyOnly)
          println(s"timestamp: ${entry.timestamp} offset: ${entry.offset}")
        prevTimestamp = entry.timestamp
      }
    } finally {
      fileRecords.closeHandlers()
      index.closeHandler()
      timeIndex.closeHandler()
    }
  }

  private trait MessageParser[K, V] {
    def parse(record: Record): (Option[K], Option[V])
  }

  private class DecoderMessageParser[K, V](keyDecoder: Decoder[K], valueDecoder: Decoder[V]) extends MessageParser[K, V] {
    override def parse(record: Record): (Option[K], Option[V]) = {
      if (!record.hasValue) {
        (None, None)
      } else {
        val key = if (record.hasKey)
          Some(keyDecoder.fromBytes(Utils.readBytes(record.key)))
        else
          None

        val payload = Some(valueDecoder.fromBytes(Utils.readBytes(record.value)))

        (key, payload)
      }
    }
  }

  private class TransactionLogMessageParser extends MessageParser[String, String] {

    override def parse(record: Record): (Option[String], Option[String]) = {
      val txnKey = TransactionLog.readTxnRecordKey(record.key)
      val txnMetadata = TransactionLog.readTxnRecordValue(txnKey.transactionalId, record.value)

      val keyString = s"transactionalId=${txnKey.transactionalId}"
      val valueString = s"producerId:${txnMetadata.producerId}," +
        s"producerEpoch:${txnMetadata.producerEpoch}," +
        s"state=${txnMetadata.state}," +
        s"partitions=${txnMetadata.topicPartitions}," +
        s"txnLastUpdateTimestamp=${txnMetadata.txnLastUpdateTimestamp}," +
        s"txnTimeoutMs=${txnMetadata.txnTimeoutMs}"

      (Some(keyString), Some(valueString))
    }

  }

  private class OffsetsMessageParser extends MessageParser[String, String] {
    private def hex(bytes: Array[Byte]): String = {
      if (bytes.isEmpty)
        ""
      else
        "%X".format(BigInt(1, bytes))
    }

    private def parseOffsets(offsetKey: OffsetKey, payload: ByteBuffer) = {
      val group = offsetKey.key.group
      val topicPartition = offsetKey.key.topicPartition
      val offset = GroupMetadataManager.readOffsetMessageValue(payload)

      val keyString = s"offset::$group:${topicPartition.topic}:${topicPartition.partition}"
      val valueString = if (offset.metadata.isEmpty)
        String.valueOf(offset.offset)
      else
        s"${offset.offset}:${offset.metadata}"

      (Some(keyString), Some(valueString))
    }

    private def parseGroupMetadata(groupMetadataKey: GroupMetadataKey, payload: ByteBuffer) = {
      val groupId = groupMetadataKey.key
      val group = GroupMetadataManager.readGroupMessageValue(groupId, payload, Time.SYSTEM)
      val protocolType = group.protocolType.getOrElse("")

      val assignment = group.allMemberMetadata.map { member =>
        if (protocolType == ConsumerProtocol.PROTOCOL_TYPE) {
          val partitionAssignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
          val userData = hex(Utils.toArray(partitionAssignment.userData()))

          if (userData.isEmpty)
            s"${member.memberId}=${partitionAssignment.partitions()}"
          else
            s"${member.memberId}=${partitionAssignment.partitions()}:$userData"
        } else {
          s"${member.memberId}=${hex(member.assignment)}"
        }
      }.mkString("{", ",", "}")

      val keyString = Json.encodeAsString(Map("metadata" -> groupId).asJava)

      val valueString = Json.encodeAsString(Map(
        "protocolType" -> protocolType,
        "protocol" -> group.protocolOrNull,
        "generationId" -> group.generationId,
        "assignment" -> assignment
      ).asJava)

      (Some(keyString), Some(valueString))
    }

    override def parse(record: Record): (Option[String], Option[String]) = {
      if (!record.hasValue)
        (None, None)
      else if (!record.hasKey) {
        throw new KafkaException("Failed to decode message using offset topic decoder (message had a missing key)")
      } else {
        GroupMetadataManager.readMessageKey(record.key) match {
          case offsetKey: OffsetKey => parseOffsets(offsetKey, record.value)
          case groupMetadataKey: GroupMetadataKey => parseGroupMetadata(groupMetadataKey, record.value)
          case _ => throw new KafkaException("Failed to decode message using offset topic decoder (message had an invalid key)")
        }
      }
    }
  }

  /* print out the contents of the log */
  private def dumpLog(file: File,
                      printContents: Boolean,
                      nonConsecutivePairsForLogFilesMap: mutable.Map[String, List[(Long, Long)]],
                      isDeepIteration: Boolean,
                      maxMessageSize: Int,
                      parser: MessageParser[_, _]) {
    val startOffset = file.getName.split("\\.")(0).toLong
    println("Starting offset: " + startOffset)
    val fileRecords = FileRecords.open(file, false)
    try {
      var validBytes = 0L
      var lastOffset = -1L

      for (batch <- fileRecords.batches.asScala) {
        printBatchLevel(batch, validBytes)
        if (isDeepIteration) {
          for (record <- batch.asScala) {
            if (lastOffset == -1)
              lastOffset = record.offset
            else if (record.offset != lastOffset + 1) {
              var nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
              nonConsecutivePairsSeq ::= (lastOffset, record.offset)
              nonConsecutivePairsForLogFilesMap.put(file.getAbsolutePath, nonConsecutivePairsSeq)
            }
            lastOffset = record.offset

            print(s"$RecordIndent offset: ${record.offset} ${batch.timestampType}: ${record.timestamp} " +
              s"keysize: ${record.keySize} valuesize: ${record.valueSize}")

            if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
              print(" sequence: " + record.sequence + " headerKeys: " + record.headers.map(_.key).mkString("[", ",", "]"))
            } else {
              print(s" crc: ${record.checksumOrNull} isvalid: ${record.isValid}")
            }

            if (batch.isControlBatch) {
              val controlTypeId = ControlRecordType.parseTypeId(record.key)
              ControlRecordType.fromTypeId(controlTypeId) match {
                case ControlRecordType.ABORT | ControlRecordType.COMMIT =>
                  val endTxnMarker = EndTransactionMarker.deserialize(record)
                  print(s" endTxnMarker: ${endTxnMarker.controlType} coordinatorEpoch: ${endTxnMarker.coordinatorEpoch}")
                case controlType =>
                  print(s" controlType: $controlType($controlTypeId)")
              }
            } else if (printContents) {
              val (key, payload) = parser.parse(record)
              key.foreach(key => print(s" key: $key"))
              payload.foreach(payload => print(s" payload: $payload"))
            }
            println()
          }
        }
        validBytes += batch.sizeInBytes
      }
      val trailingBytes = fileRecords.sizeInBytes - validBytes
      if (trailingBytes > 0)
        println(s"Found $trailingBytes invalid bytes at the end of ${file.getName}")
    } finally fileRecords.closeHandlers()
  }

  private def printBatchLevel(batch: FileLogInputStream.FileChannelRecordBatch, accumulativeBytes: Long): Unit = {
    if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
      print("baseOffset: " + batch.baseOffset + " lastOffset: " + batch.lastOffset + " count: " + batch.countOrNull +
        " baseSequence: " + batch.baseSequence + " lastSequence: " + batch.lastSequence +
        " producerId: " + batch.producerId + " producerEpoch: " + batch.producerEpoch +
        " partitionLeaderEpoch: " + batch.partitionLeaderEpoch + " isTransactional: " + batch.isTransactional +
        " isControl: " + batch.isControlBatch)
    else
      print("offset: " + batch.lastOffset)

    println(" position: " + accumulativeBytes + " " + batch.timestampType + ": " + batch.maxTimestamp +
      " size: " + batch.sizeInBytes + " magic: " + batch.magic +
      " compresscodec: " + batch.compressionType + " crc: " + batch.checksum + " isvalid: " + batch.isValid)
  }

  class TimeIndexDumpErrors {
    val misMatchesForTimeIndexFilesMap = mutable.Map[String, ArrayBuffer[(Long, Long)]]()
    val outOfOrderTimestamp = mutable.Map[String, ArrayBuffer[(Long, Long)]]()
    val shallowOffsetNotFound = mutable.Map[String, ArrayBuffer[(Long, Long)]]()

    def recordMismatchTimeIndex(file: File, indexTimestamp: Long, logTimestamp: Long) {
      val misMatchesSeq = misMatchesForTimeIndexFilesMap.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (misMatchesSeq.isEmpty)
        misMatchesForTimeIndexFilesMap.put(file.getAbsolutePath, misMatchesSeq)
      misMatchesSeq += ((indexTimestamp, logTimestamp))
    }

    def recordOutOfOrderIndexTimestamp(file: File, indexTimestamp: Long, prevIndexTimestamp: Long) {
      val outOfOrderSeq = outOfOrderTimestamp.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (outOfOrderSeq.isEmpty)
        outOfOrderTimestamp.put(file.getAbsolutePath, outOfOrderSeq)
      outOfOrderSeq += ((indexTimestamp, prevIndexTimestamp))
    }

    def recordShallowOffsetNotFound(file: File, indexOffset: Long, logOffset: Long) {
      val shallowOffsetNotFoundSeq = shallowOffsetNotFound.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (shallowOffsetNotFoundSeq.isEmpty)
        shallowOffsetNotFound.put(file.getAbsolutePath, shallowOffsetNotFoundSeq)
      shallowOffsetNotFoundSeq += ((indexOffset, logOffset))
    }

    def printErrors() {
      misMatchesForTimeIndexFilesMap.foreach {
        case (fileName, listOfMismatches) => {
          System.err.println("Found timestamp mismatch in :" + fileName)
          listOfMismatches.foreach(m => {
            System.err.println("  Index timestamp: %d, log timestamp: %d".format(m._1, m._2))
          })
        }
      }

      outOfOrderTimestamp.foreach {
        case (fileName, outOfOrderTimestamps) => {
          System.err.println("Found out of order timestamp in :" + fileName)
          outOfOrderTimestamps.foreach(m => {
            System.err.println("  Index timestamp: %d, Previously indexed timestamp: %d".format(m._1, m._2))
          })
        }
      }

      shallowOffsetNotFound.values.foreach { listOfShallowOffsetNotFound =>
        System.err.println("The following indexed offsets are not found in the log.")
        listOfShallowOffsetNotFound.foreach { case (indexedOffset, logOffset) =>
          System.err.println(s"Indexed offset: $indexedOffset, found log offset: $logOffset")
        }
      }
    }
  }

  private class DumpLogSegmentsOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val printOpt = parser.accepts("print-data-log", "if set, printing the messages content when dumping data logs. Automatically set if any decoder option is specified.")
    val verifyOpt = parser.accepts("verify-index-only", "if set, just verify the index log without printing its content.")
    val indexSanityOpt = parser.accepts("index-sanity-check", "if set, just checks the index sanity without printing its content. " +
      "This is the same check that is executed on broker startup to determine if an index needs rebuilding or not.")
    val filesOpt = parser.accepts("files", "REQUIRED: The comma separated list of data and index log files to be dumped.")
      .withRequiredArg
      .describedAs("file1, file2, ...")
      .ofType(classOf[String])
    val maxMessageSizeOpt = parser.accepts("max-message-size", "Size of largest message.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(5 * 1024 * 1024)
    val deepIterationOpt = parser.accepts("deep-iteration", "if set, uses deep instead of shallow iteration. Automatically set if print-data-log is enabled.")
    val valueDecoderOpt = parser.accepts("value-decoder-class", "if set, used to deserialize the messages. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
      .withOptionalArg()
      .ofType(classOf[java.lang.String])
      .defaultsTo("kafka.serializer.StringDecoder")
    val keyDecoderOpt = parser.accepts("key-decoder-class", "if set, used to deserialize the keys. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
      .withOptionalArg()
      .ofType(classOf[java.lang.String])
      .defaultsTo("kafka.serializer.StringDecoder")
    val offsetsOpt = parser.accepts("offsets-decoder", "if set, log data will be parsed as offset data from the " +
      "__consumer_offsets topic.")
    val transactionLogOpt = parser.accepts("transaction-log-decoder", "if set, log data will be parsed as " +
      "transaction metadata from the __transaction_state topic.")
    options = parser.parse(args : _*)

    def messageParser: MessageParser[_, _] =
      if (options.has(offsetsOpt)) {
        new OffsetsMessageParser
      } else if (options.has(transactionLogOpt)) {
        new TransactionLogMessageParser
      } else {
        val valueDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]](options.valueOf(valueDecoderOpt), new VerifiableProperties)
        val keyDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]](options.valueOf(keyDecoderOpt), new VerifiableProperties)
        new DecoderMessageParser(keyDecoder, valueDecoder)
      }

    lazy val shouldPrintDataLog: Boolean = options.has(printOpt) ||
      options.has(offsetsOpt) ||
      options.has(transactionLogOpt) ||
      options.has(valueDecoderOpt) ||
      options.has(keyDecoderOpt)

    lazy val isDeepIteration: Boolean = options.has(deepIterationOpt) || shouldPrintDataLog
    lazy val verifyOnly: Boolean = options.has(verifyOpt)
    lazy val indexSanityOnly: Boolean = options.has(indexSanityOpt)
    lazy val files = options.valueOf(filesOpt).split(",")
    lazy val maxMessageSize = options.valueOf(maxMessageSizeOpt).intValue()

    def checkArgs(): Unit = CommandLineUtils.checkRequiredArgs(parser, options, filesOpt)

  }
}
