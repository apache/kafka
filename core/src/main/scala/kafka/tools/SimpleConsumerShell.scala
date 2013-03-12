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

import joptsimple._
import kafka.utils._
import kafka.consumer._
import kafka.client.ClientUtils
import kafka.api.{OffsetRequest, FetchRequestBuilder, Request}
import kafka.cluster.Broker
import scala.collection.JavaConversions._

/**
 * Command line program to dump out messages to standard out using the simple consumer
 */
object SimpleConsumerShell extends Logging {

  def UseLeaderReplica = -1

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                           .withRequiredArg
                           .describedAs("hostname:port,...,hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionIdOpt = parser.accepts("partition", "The partition to consume from.")
                           .withRequiredArg
                           .describedAs("partition")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(0)
    val replicaIdOpt = parser.accepts("replica", "The replica id to consume from, default -1 means leader broker.")
                           .withRequiredArg
                           .describedAs("replica id")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(UseLeaderReplica)
    val offsetOpt = parser.accepts("offset", "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end")
                           .withOptionalArg()
                           .describedAs("consume offset")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(OffsetRequest.EarliestTime)
    val clientIdOpt = parser.accepts("clientId", "The ID of this client.")
                           .withOptionalArg
                           .describedAs("clientId")
                           .ofType(classOf[String])
                           .defaultsTo("SimpleConsumerShell")
    val fetchSizeOpt = parser.accepts("fetchsize", "The fetch size of each request.")
                           .withRequiredArg
                           .describedAs("fetchsize")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1024 * 1024)
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
                           .withRequiredArg
                           .describedAs("class")
                           .ofType(classOf[String])
                           .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property")
                           .withRequiredArg
                           .describedAs("prop")
                           .ofType(classOf[String])
    val printOffsetOpt = parser.accepts("print-offsets", "Print the offsets returned by the iterator")
                           .withOptionalArg
                           .describedAs("print offsets")
                           .ofType(classOf[java.lang.Boolean])
                           .defaultsTo(false)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000)
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
        "skip it instead of halt.")
    val noWaitAtEndOfLogOpt = parser.accepts("no-wait-at-logend",
        "If set, when the simple consumer reaches the end of the Log, it will stop, not waiting for new produced messages")

    val options = parser.parse(args : _*)
    for(arg <- List(brokerListOpt, topicOpt, partitionIdOpt)) {
      if(!options.has(arg)) {
        error("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val partitionId = options.valueOf(partitionIdOpt).intValue()
    val replicaId = options.valueOf(replicaIdOpt).intValue()
    var startingOffset = options.valueOf(offsetOpt).longValue
    val fetchSize = options.valueOf(fetchSizeOpt).intValue
    val clientId = options.valueOf(clientIdOpt).toString
    val maxWaitMs = options.valueOf(maxWaitMsOpt).intValue()

    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false
    val printOffsets = if(options.has(printOffsetOpt)) true else false
    val noWaitAtEndOfLog = options.has(noWaitAtEndOfLogOpt)

    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = MessageFormatter.tryParseFormatterArgs(options.valuesOf(messageFormatterArgOpt))

    val fetchRequestBuilder = new FetchRequestBuilder()
                       .clientId(clientId)
                       .replicaId(Request.DebuggingConsumerId)
                       .maxWait(maxWaitMs)
                       .minBytes(ConsumerConfig.MinFetchBytes)

    // getting topic metadata
    info("Getting topic metatdata...")
    val metadataTargetBrokers = ClientUtils.parseBrokerList(options.valueOf(brokerListOpt))
    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
    if(topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
      System.err.println(("Error: no valid topic metadata for topic: %s, " + "what we get from server is only: %s").format(topic, topicsMetadata))
      System.exit(1)
    }

    // validating partition id
    val partitionsMetadata = topicsMetadata(0).partitionsMetadata
    val partitionMetadataOpt = partitionsMetadata.find(p => p.partitionId == partitionId)
    if(!partitionMetadataOpt.isDefined) {
      System.err.println("Error: partition %d does not exist for topic %s".format(partitionId, topic))
      System.exit(1)
    }

    // validating replica id and initializing target broker
    var fetchTargetBroker: Broker = null
    var replicaOpt: Option[Broker] = null
    if(replicaId == UseLeaderReplica) {
      replicaOpt = partitionMetadataOpt.get.leader
      if(!replicaOpt.isDefined) {
        System.err.println("Error: user speicifies to fetch from leader for partition (%s, %d) which has not been elected yet".format(replicaId, topic, partitionId))
        System.exit(1)
      }
    }
    else {
      val replicasForPartition = partitionMetadataOpt.get.replicas
      replicaOpt = replicasForPartition.find(r => r.id == replicaId)
      if(!replicaOpt.isDefined) {
        System.err.println("Error: replica %d does not exist for partition (%s, %d)".format(replicaId, topic, partitionId))
        System.exit(1)
      }
    }
    fetchTargetBroker = replicaOpt.get

    // initializing starting offset
    if(startingOffset < OffsetRequest.EarliestTime) {
      System.err.println("Invalid starting offset: %d".format(startingOffset))
      System.exit(1)
    }
    if(startingOffset < 0)
      startingOffset = SimpleConsumer.earliestOrLatestOffset(fetchTargetBroker, topic, partitionId, startingOffset, clientId, false)

    // initializing formatter
    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]
    formatter.init(formatterArgs)

    info("Starting simple consumer shell to partition [%s, %d], replica [%d], host and port: [%s, %d], from offset [%d]"
                 .format(topic, partitionId, replicaId, fetchTargetBroker.host, fetchTargetBroker.port, startingOffset))
    val simpleConsumer = new SimpleConsumer(fetchTargetBroker.host, fetchTargetBroker.port, 10000, 64*1024, clientId)
    val thread = Utils.newThread("kafka-simpleconsumer-shell", new Runnable() {
      def run() {
        var offset = startingOffset
        try {
          while(true) {
            val fetchRequest = fetchRequestBuilder
                    .addFetch(topic, partitionId, offset, fetchSize)
                    .build()
            val fetchResponse = simpleConsumer.fetch(fetchRequest)
            val messageSet = fetchResponse.messageSet(topic, partitionId)
            if (messageSet.validBytes <= 0 && noWaitAtEndOfLog) {
              println("Terminating. Reached the end of partition (%s, %d) at offset %d".format(topic, partitionId, offset))
              return
            }
            debug("multi fetched " + messageSet.sizeInBytes + " bytes from offset " + offset)
            for(messageAndOffset <- messageSet) {
              try {
                offset = messageAndOffset.nextOffset
                if(printOffsets)
                  System.out.println("next offset = " + offset)
                val message = messageAndOffset.message
                val key = if(message.hasKey) Utils.readBytes(message.key) else null
                formatter.writeTo(key, if(message.isNull) null else Utils.readBytes(message.payload), System.out)
              } catch {
                case e =>
                  if (skipMessageOnError)
                    error("Error processing message, skipping this message: ", e)
                  else
                    throw e
              }
              if(System.out.checkError()) {
                // This means no one is listening to our output stream any more, time to shutdown
                System.err.println("Unable to write to standard out, closing consumer.")
                formatter.close()
                simpleConsumer.close()
                System.exit(1)
              }
            }
          }
        } catch {
          case e: Throwable =>
            error("Error consuming topic, partition, replica (%s, %d, %d) with offset [%d]".format(topic, partitionId, replicaId, offset), e)
        }
      }
    }, false)
    thread.start()
    thread.join()
    System.out.flush()
    formatter.close()
    simpleConsumer.close()
  }
}
