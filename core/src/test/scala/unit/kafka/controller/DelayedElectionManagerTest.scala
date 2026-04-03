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
package kafka.controller

import kafka.server.{KafkaConfig, OffsetAndEpoch}
import kafka.utils.{KafkaScheduler, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, ListOffsetsRequest, ListOffsetsResponse}
import org.easymock.{Capture, EasyMock, IArgumentMatcher}
import org.junit.jupiter.api.{BeforeEach, Test}
import kafka.controller.DelayedElectionManagerTest.{DelayedElectionWaitMs, equalListOffsetsRequest}
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.junit.jupiter.api.Assertions.assertEquals

import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.{Collections, Properties}

object DelayedElectionManagerTest {
  private val DelayedElectionWaitMs = 1000

  private def equalListOffsetsRequest(listOffsetsRequestBuilder: ListOffsetsRequest.Builder): ListOffsetsRequest.Builder = {
    EasyMock.reportMatcher(new IArgumentMatcher {
      override def matches(other: Any): Boolean = {
        other match {
          case builder: ListOffsetsRequest.Builder =>
            val otherData = builder.build().data()
            val thisData = listOffsetsRequestBuilder.build().data()
            thisData.equals(otherData)
          case _ => false
        }
      }

      override def appendTo(stringBuffer: StringBuffer): Unit = {
        stringBuffer.append(s"listOffsetsRequest($listOffsetsRequestBuilder)")
      }
    })
    null
  }
}

class DelayedElectionManagerTest {
  private var controllerContext: ControllerContext = null
  private var delayedElectionManager: DelayedElectionManager = null
  private var eventManager: ControllerEventManager = null
  private var channelManager: ControllerChannelManager = null
  private var kafkaScheduler: KafkaScheduler = null

  private val controllerBrokerId = 5
  val extraProps = new Properties()
  extraProps.put(KafkaConfig.LiLeaderElectionOnCorruptionWaitMsProp, DelayedElectionWaitMs: java.lang.Long)

  private val config = KafkaConfig.fromProps(
    TestUtils.createBrokerConfig(controllerBrokerId, "zkConnect"),
    extraProps
  )
  private val controllerEpoch = 50
  private val partition = new TopicPartition("t", 0)
  private val partitions = Seq(partition)

  @BeforeEach
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(0, 1, 2, 3, 4)))

    eventManager = EasyMock.createMock(classOf[ControllerEventManager])
    channelManager = EasyMock.createMock(classOf[ControllerChannelManager])
    kafkaScheduler = EasyMock.createMock(classOf[KafkaScheduler])
    delayedElectionManager = new DelayedElectionManager(
      config, controllerContext, eventManager, channelManager, kafkaScheduler)
  }

  private def expectListOffsetsToBroker(brokerId: Int): Unit = {
    val expectedRequest = ListOffsetsRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion(), ListOffsetsRequest.CONTROLLER_REPLICA_ID)
      .setTargetTimes(Collections.singletonList(
        new ListOffsetsTopic()
          .setName(partition.topic())
          .setPartitions(
            Collections.singletonList(new ListOffsetsPartition()
              .setPartitionIndex(partition.partition())
              .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
            ))
      ))
    channelManager.sendRequest(
      EasyMock.eq(brokerId), equalListOffsetsRequest(expectedRequest),
      EasyMock.anyObject(classOf[AbstractResponse => Unit]))
    EasyMock.expectLastCall()
  }

  @Test
  def testNewCorruptedPartitionsStartElections(): Unit = {
    controllerContext.setCorruptedBrokers(Map(0 -> true, 1 -> true, 2 -> true))
    controllerContext.setLiveBrokers(Map(
      TestUtils.createBrokerAndEpoch(0, "host", 0),
      TestUtils.createBrokerAndEpoch(1, "host", 0)))

    Seq(0, 1).foreach(expectListOffsetsToBroker)

    expectElectionScheduled()

    EasyMock.replay(eventManager, channelManager, kafkaScheduler)
    delayedElectionManager.startDelayedElectionsForPartitions(partitions)
    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
  }

  private def expectElectionScheduled(): (ScheduledFuture[Void], Capture[() => Unit]) = {
    val scheduledFuture: ScheduledFuture[Void] = EasyMock.createMock(classOf[ScheduledFuture[Void]])
    val callbackCapture = Capture.newInstance[() => Unit]()

    EasyMock.expect(kafkaScheduler.schedule(
      EasyMock.anyString(), EasyMock.capture(callbackCapture), EasyMock.anyLong(),
      EasyMock.anyLong(), EasyMock.anyObject(classOf[TimeUnit]))
      .asInstanceOf[ScheduledFuture[Void]])
      .andReturn(scheduledFuture)

    (scheduledFuture, callbackCapture)
  }

  @Test
  def testCorruptedBrokerStartup(): Unit = {
    controllerContext.setCorruptedBrokers(Map.empty)
    controllerContext.setLiveBrokers(Map.empty)

    expectElectionScheduled()
    EasyMock.replay(eventManager, channelManager, kafkaScheduler)

    delayedElectionManager.startDelayedElectionsForPartitions(partitions)
    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
    EasyMock.reset(eventManager, channelManager, kafkaScheduler)

    controllerContext.setCorruptedBrokers(Map(0 -> true))
    controllerContext.addLiveBrokers(Map(TestUtils.createBrokerAndEpoch(0, "host", 0)))

    expectListOffsetsToBroker(0)
    EasyMock.replay(eventManager, channelManager, kafkaScheduler)

    delayedElectionManager.onCorruptedBrokerStartup(0)
    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
  }

  def buildResponse(leaderEpoch: Int, offset: Int): ListOffsetsResponse = {
    new ListOffsetsResponse(new ListOffsetsResponseData()
      .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
        .setName(partition.topic())
        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
          .setPartitionIndex(partition.partition())
          .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
          .setLeaderEpoch(leaderEpoch)
          .setOffset(offset)
        ))
      )))
  }

  def buildErrorResponse(): ListOffsetsResponse = {
    new ListOffsetsResponse(new ListOffsetsResponseData()
      .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
        .setName(partition.topic())
        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
          .setPartitionIndex(partition.partition())
          .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
          .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
        ))
      )))
  }

  @Test
  def testDelayedElection(): Unit = {
    // Part 1: delayed election started; broker 0 and 1 are live
    Seq(0, 1).foreach(expectListOffsetsToBroker)
    val (scheduledFuture, callbackCapture) = expectElectionScheduled()

    EasyMock.replay(eventManager, channelManager, kafkaScheduler)

    // Set up 0 and 1 as live brokers
    controllerContext.setCorruptedBrokers(Map(0 -> true, 1 -> true, 2 -> true))
    controllerContext.setLiveBrokers(Map(
      TestUtils.createBrokerAndEpoch(0, "host", 0),
      TestUtils.createBrokerAndEpoch(1, "host", 0)))

    delayedElectionManager.startDelayedElectionsForPartitions(partitions)

    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
    val callback = callbackCapture.getValue
    EasyMock.reset(eventManager, channelManager, kafkaScheduler)

    // Part 2: Broker 2 comes up as live
    expectListOffsetsToBroker(2)
    EasyMock.replay(eventManager, channelManager, kafkaScheduler)

    controllerContext.addLiveBrokers(Map(TestUtils.createBrokerAndEpoch(2, "host", 0)))
    delayedElectionManager.onCorruptedBrokerStartup(2)

    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
    EasyMock.reset(eventManager, channelManager, kafkaScheduler)

    // Part 3: ListOffsets reponse for brokers 0 and 2
    EasyMock.replay(eventManager, channelManager, kafkaScheduler)
    delayedElectionManager.onListOffsetsResponse(0, buildResponse(100, 1002))
    delayedElectionManager.onListOffsetsResponse(1, buildErrorResponse())
    delayedElectionManager.onListOffsetsResponse(2, buildResponse(100, 1000))
    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
    EasyMock.reset(eventManager, channelManager, kafkaScheduler)

    // Part 4: Election completes
    val captureEvent = Capture.newInstance[DelayedElectionSuccess]()
    EasyMock.expect(eventManager.put(EasyMock.capture(captureEvent)))
      .andReturn(EasyMock.createMock(classOf[QueuedEvent]))
    EasyMock.expect(scheduledFuture.isDone).andReturn(true)
    EasyMock.replay(eventManager, channelManager, kafkaScheduler, scheduledFuture)
    callback()
    EasyMock.verify(eventManager, channelManager, kafkaScheduler, scheduledFuture)

    val event = captureEvent.getValue
    assertEquals(partition, event.partition)
    assertEquals(Map(
      0 -> OffsetAndEpoch(1002, 100),
      2 -> OffsetAndEpoch(1000, 100),
    ), event.brokerIdToOffsetAndEpoch)
  }
}
