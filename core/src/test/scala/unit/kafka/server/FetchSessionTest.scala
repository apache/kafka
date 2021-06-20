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
package kafka.server

import kafka.utils.MockTime
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchRequestData
import org.apache.kafka.common.message.FetchRequestData.FetchPartition
import org.apache.kafka.common.message.FetchRequestData.FetchTopic
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.{FetchRequest, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.util
import java.util.{Collections, Optional}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

@Timeout(120)
class FetchSessionTest {

  @Test
  def testNewSessionId(): Unit = {
    val cache = new FetchSessionCache(3, 100)
    for (_ <- 0 to 10000) {
      val id = cache.newSessionId()
      assertTrue(id > 0)
    }
  }

  def assertCacheContains(cache: FetchSessionCache, sessionIds: Int*) = {
    var i = 0
    for (sessionId <- sessionIds) {
      i = i + 1
      assertTrue(cache.get(sessionId).isDefined,
        "Missing session " + i + " out of " + sessionIds.size + "(" + sessionId + ")")
    }
    assertEquals(sessionIds.size, cache.size)
  }

  private def dummyCreate(size: Int): FetchSession.CACHE_MAP = {
    val cacheMap = new FetchSession.CACHE_MAP(size)
    for (i <- 0 until size) {
      cacheMap.add(new CachedPartition("test", i))
    }
    cacheMap
  }

  @Test
  def testSessionCache(): Unit = {
    val cache = new FetchSessionCache(3, 100)
    assertEquals(0, cache.size)
    val id1 = cache.maybeCreateSession(0, false, 10, () => dummyCreate(10))
    val id2 = cache.maybeCreateSession(10, false, 20, () => dummyCreate(20))
    val id3 = cache.maybeCreateSession(20, false, 30, () => dummyCreate(30))
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(30, false, 40, () => dummyCreate(40)))
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(40, false, 5, () => dummyCreate(5)))
    assertCacheContains(cache, id1, id2, id3)
    cache.touch(cache.get(id1).get, 200)
    val id4 = cache.maybeCreateSession(210, false, 11, () => dummyCreate(11))
    assertCacheContains(cache, id1, id3, id4)
    cache.touch(cache.get(id1).get, 400)
    cache.touch(cache.get(id3).get, 390)
    cache.touch(cache.get(id4).get, 400)
    val id5 = cache.maybeCreateSession(410, false, 50, () => dummyCreate(50))
    assertCacheContains(cache, id3, id4, id5)
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(410, false, 5, () => dummyCreate(5)))
    val id6 = cache.maybeCreateSession(410, true, 5, () => dummyCreate(5))
    assertCacheContains(cache, id3, id5, id6)
  }

  @Test
  def testResizeCachedSessions(): Unit = {
    val cache = new FetchSessionCache(2, 100)
    assertEquals(0, cache.totalPartitions)
    assertEquals(0, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    val id1 = cache.maybeCreateSession(0, false, 2, () => dummyCreate(2))
    assertTrue(id1 > 0)
    assertCacheContains(cache, id1)
    val session1 = cache.get(id1).get
    assertEquals(2, session1.size)
    assertEquals(2, cache.totalPartitions)
    assertEquals(1, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    val id2 = cache.maybeCreateSession(0, false, 4, () => dummyCreate(4))
    val session2 = cache.get(id2).get
    assertTrue(id2 > 0)
    assertCacheContains(cache, id1, id2)
    assertEquals(6, cache.totalPartitions)
    assertEquals(2, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    cache.touch(session1, 200)
    cache.touch(session2, 200)
    val id3 = cache.maybeCreateSession(200, false, 5, () => dummyCreate(5))
    assertTrue(id3 > 0)
    assertCacheContains(cache, id2, id3)
    assertEquals(9, cache.totalPartitions)
    assertEquals(2, cache.size)
    assertEquals(1, cache.evictionsMeter.count)
    cache.remove(id3)
    assertCacheContains(cache, id2)
    assertEquals(1, cache.size)
    assertEquals(1, cache.evictionsMeter.count)
    assertEquals(4, cache.totalPartitions)
    val iter = session2.partitionMap.iterator
    iter.next()
    iter.remove()
    assertEquals(3, session2.size)
    assertEquals(4, session2.cachedSize)
    cache.touch(session2, session2.lastUsedMs)
    assertEquals(3, cache.totalPartitions)
  }

  private def cachedLeaderEpochs(context: FetchContext): Map[TopicPartition, Optional[Integer]] = {
    val mapBuilder = Map.newBuilder[TopicPartition, Optional[Integer]]
    context.foreachPartition { (tp, data) =>
      mapBuilder += tp -> FetchRequest.optionalEpoch(data.currentLeaderEpoch)
    }
    mapBuilder.result()
  }

  private def cachedLastFetchedEpochs(context: FetchContext): Map[TopicPartition, Optional[Integer]] = {
    val mapBuilder = Map.newBuilder[TopicPartition, Optional[Integer]]
    context.foreachPartition { (tp, data) =>
      mapBuilder += tp -> FetchRequest.optionalEpoch(data.lastFetchedEpoch)
    }
    mapBuilder.result()
  }

  @Test
  def testCachedLeaderEpoch(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 1)

    val fetchRequest1 = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(JFetchMetadata.INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic(tp0.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp0.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(tp1.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(1)
        )),
        new FetchTopic().setTopic(tp2.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp2.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(2)
        ))
      ))

    val context1 = fetchManager.newContext(fetchRequest1, false)
    val epochs1 = cachedLeaderEpochs(context1)
    assertEquals(Optional.empty(), epochs1(tp0))
    assertEquals(Optional.of(1), epochs1(tp1))
    assertEquals(Optional.of(2), epochs1(tp2))

    val response = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    response.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp0.partition)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    response.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    response.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.partition)
      .setHighWatermark(5)
      .setLastStableOffset(5)
      .setLogStartOffset(5))

    val sessionId = context1.updateAndGenerateResponseData(response).sessionId()

    // With no changes, the cached epochs should remain the same
    val fetchRequest2 = new FetchRequestData()
      .setSessionId(sessionId)
      .setSessionEpoch(1)
    val context2 = fetchManager.newContext(fetchRequest2, false)
    val epochs2 = cachedLeaderEpochs(context2)
    assertEquals(Optional.empty(), epochs1(tp0))
    assertEquals(Optional.of(1), epochs2(tp1))
    assertEquals(Optional.of(2), epochs2(tp2))
    context2.updateAndGenerateResponseData(response).sessionId()

    // Now verify we can change the leader epoch and the context is updated
    val fetchRequest3 = new FetchRequestData()
      .setSessionId(sessionId)
      .setSessionEpoch(2)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic(tp0.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp0.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(6),
          new FetchPartition()
            .setPartition(tp1.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        )),
        new FetchTopic().setTopic(tp2.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp2.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(3)
        ))
      ))

    val context3 = fetchManager.newContext(fetchRequest3, false)
    val epochs3 = cachedLeaderEpochs(context3)
    assertEquals(Optional.of(6), epochs3(tp0))
    assertEquals(Optional.empty(), epochs3(tp1))
    assertEquals(Optional.of(3), epochs3(tp2))
  }

  @Test
  def testLastFetchedEpoch(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 1)

    val fetchRequest1 = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(JFetchMetadata.INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic(tp0.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp0.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(tp1.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(1)
        )),
        new FetchTopic().setTopic(tp2.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp2.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(2)
            .setLastFetchedEpoch(1)
        ))
      ))

    val context1 = fetchManager.newContext(fetchRequest1, false)
    assertEquals(Map(
      tp0 -> Optional.empty,
      tp1 -> Optional.of(1),
      tp2 -> Optional.of(2)
    ), cachedLeaderEpochs(context1))
    assertEquals(Map(
      tp0 -> Optional.empty,
      tp1 -> Optional.empty,
      tp2 -> Optional.of(1)
    ), cachedLastFetchedEpochs(context1))

    val response = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    response.put(tp0, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp0.partition)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    response.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    response.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.partition)
      .setHighWatermark(5)
      .setLastStableOffset(5)
      .setLogStartOffset(5))

    val sessionId = context1.updateAndGenerateResponseData(response).sessionId()

    // With no changes, the cached epochs should remain the same
    val fetchRequest2 = new FetchRequestData()
      .setSessionId(sessionId)
      .setSessionEpoch(1)
    val context2 = fetchManager.newContext(fetchRequest2, false)
    assertEquals(Map(
      tp0 -> Optional.empty,
      tp1 -> Optional.of(1),
      tp2 -> Optional.of(2)
    ), cachedLeaderEpochs(context2))
    assertEquals(Map(
      tp0 -> Optional.empty,
      tp1 -> Optional.empty,
      tp2 -> Optional.of(1)
    ), cachedLastFetchedEpochs(context2))
    context2.updateAndGenerateResponseData(response).sessionId()

    // Now verify we can change the leader epoch and the context is updated
    val fetchRequest3 = new FetchRequestData()
      .setSessionId(sessionId)
      .setSessionEpoch(2)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic(tp0.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp0.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(6)
            .setLastFetchedEpoch(5),
          new FetchPartition()
            .setPartition(tp1.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        )),
        new FetchTopic().setTopic(tp2.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp2.partition)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(3)
            .setLastFetchedEpoch(3)
        ))
      ))

    val context3 = fetchManager.newContext(fetchRequest3, false)
    assertEquals(Map(
      tp0 -> Optional.of(6),
      tp1 -> Optional.empty,
      tp2 -> Optional.of(3)
    ), cachedLeaderEpochs(context3))
    assertEquals(Map(
      tp0 -> Optional.of(5),
      tp1 -> Optional.empty,
      tp2 -> Optional.of(3)
    ), cachedLastFetchedEpochs(context2))
  }

  private def assertPartitionsInContext(
    expectedFetchRequest: FetchRequestData,
    context: FetchContext
  ): Unit = {
    val iterator = expectedFetchRequest.topics.asScala.flatMap { topic =>
      topic.partitions.asScala.map { partition =>
        new TopicPartition(topic.topic, partition.partition) -> partition
      }
    }.iterator

    context.foreachPartition { (topicPartition, data) =>
      val (expectedTopicPartition, expectedPartition) = iterator.next()
      assertEquals(expectedTopicPartition, topicPartition)
      assertEquals(expectedPartition.fetchOffset(), data.fetchOffset())
      assertEquals(expectedPartition.logStartOffset(), data.logStartOffset())
      assertEquals(expectedPartition.partitionMaxBytes(), data.partitionMaxBytes())
      assertEquals(expectedPartition.currentLeaderEpoch(), data.currentLeaderEpoch())
      assertEquals(expectedPartition.lastFetchedEpoch(), data.lastFetchedEpoch())
    }
  }

  @Test
  def testFetchRequests(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Verify that SESSIONLESS requests get a SessionlessFetchContext
    val fetchRequest1 = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(FINAL_EPOCH)

    val context = fetchManager.newContext(fetchRequest1, true)
    assertEquals(classOf[SessionlessFetchContext], context.getClass)

    // Create a new fetch session with a FULL fetch request
    val fetchRequest2 = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val context2 = fetchManager.newContext(fetchRequest2, false)
    assertEquals(classOf[FullFetchContext], context2.getClass)
    assertPartitionsInContext(fetchRequest2, context2)
    assertEquals(0, context2.getFetchOffset(new TopicPartition("foo", 0)).get)
    assertEquals(10, context2.getFetchOffset(new TopicPartition("foo", 1)).get)

    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData2.put(new TopicPartition("foo", 0),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData2.put(new TopicPartition("foo", 1),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))

    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error())
    assertTrue(resp2.sessionId() != INVALID_SESSION_ID)
    assertEquals(respData2, resp2.responseData)

    // Test trying to create a new session with an invalid epoch
    val context3 = fetchManager.newContext(
      new FetchRequestData().setSessionId(resp2.sessionId()).setSessionEpoch(5),
      false
    )
    assertEquals(classOf[SessionErrorContext], context3.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context3.updateAndGenerateResponseData(respData2).error())

    // Test trying to create a new session with a non-existent session id
    fetchRequest2.setSessionId(resp2.sessionId() + 1).setSessionEpoch(1)
    val context4 = fetchManager.newContext(
      new FetchRequestData().setSessionId(resp2.sessionId() + 1).setSessionEpoch(1),
      false
    )
    assertEquals(classOf[SessionErrorContext], context4.getClass)
    assertEquals(Errors.FETCH_SESSION_ID_NOT_FOUND,
      context4.updateAndGenerateResponseData(respData2).error())

    // Continue the first fetch session we created.
    val fetchRequest3 = new FetchRequestData()
      .setSessionId(resp2.sessionId())
      .setSessionEpoch(1)
    val context5 = fetchManager.newContext(fetchRequest3, false)
    assertEquals(classOf[IncrementalFetchContext], context5.getClass)
    assertPartitionsInContext(fetchRequest2, context5)
    assertEquals(10, context5.getFetchOffset(new TopicPartition("foo", 1)).get)
    val resp5 = context5.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp5.error())
    assertEquals(resp2.sessionId(), resp5.sessionId())
    assertEquals(0, resp5.responseData.size())

    // Test setting an invalid fetch session epoch.
    val context6 = fetchManager.newContext(
      new FetchRequestData().setSessionId(resp2.sessionId()).setSessionEpoch(5),
      false
    )
    assertEquals(classOf[SessionErrorContext], context6.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context6.updateAndGenerateResponseData(respData2).error())

    // Test generating a throttled response for the incremental fetch session
    val context7 = fetchManager.newContext(
      new FetchRequestData().setSessionId(resp2.sessionId()).setSessionEpoch(2),
      false
    )
    val resp7 = context7.getThrottledResponse(100)
    assertEquals(Errors.NONE, resp7.error())
    assertEquals(resp2.sessionId(), resp7.sessionId())
    assertEquals(100, resp7.throttleTimeMs())

    // Close the incremental fetch session.
    val prevSessionId = resp5.sessionId
    var nextSessionId = prevSessionId
    do {
      val fetchRequest8 = new FetchRequestData()
        .setSessionId(prevSessionId)
        .setSessionEpoch(FINAL_EPOCH)
        .setTopics(util.Arrays.asList(
          new FetchTopic().setTopic("bar").setPartitions(util.Arrays.asList(
            new FetchPartition()
              .setPartition(0)
              .setFetchOffset(0)
              .setLogStartOffset(0)
              .setPartitionMaxBytes(100),
            new FetchPartition()
              .setPartition(1)
              .setFetchOffset(10)
              .setLogStartOffset(0)
              .setPartitionMaxBytes(100)
          ))
        ))

      val context8 = fetchManager.newContext(fetchRequest8, false)
      assertEquals(classOf[SessionlessFetchContext], context8.getClass)
      assertEquals(0, cache.size)
      val respData8 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
      respData8.put(new TopicPartition("bar", 0),
        new FetchResponseData.PartitionData()
          .setPartitionIndex(0)
          .setHighWatermark(100)
          .setLastStableOffset(100)
          .setLogStartOffset(100))
      respData8.put(new TopicPartition("bar", 1),
        new FetchResponseData.PartitionData()
          .setPartitionIndex(1)
          .setHighWatermark(100)
          .setLastStableOffset(100)
          .setLogStartOffset(100))
      val resp8 = context8.updateAndGenerateResponseData(respData8)
      assertEquals(Errors.NONE, resp8.error)
      nextSessionId = resp8.sessionId
    } while (nextSessionId == prevSessionId)
  }

  @Test
  def testIncrementalFetchSession(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Create a new fetch session with foo-0 and foo-1
    val fetchRequest0 = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val context1 = fetchManager.newContext(fetchRequest0, false)
    assertEquals(classOf[FullFetchContext], context1.getClass)

    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(new TopicPartition("foo", 1), new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))

    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData.size())

    // Create an incremental fetch request that removes foo-0 and adds bar-0
    val fetchRequest2 = new FetchRequestData()
      .setSessionId(resp1.sessionId())
      .setSessionEpoch(1)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("bar").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(15)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)))))
      .setForgottenTopicsData(util.Arrays.asList(
        new ForgottenTopic().setTopic("foo").setPartitions(util.Arrays.asList(0))))

    val context2 = fetchManager.newContext(fetchRequest2, false)
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)

    val parts2 = Set(new TopicPartition("foo", 1), new TopicPartition("bar", 0))
    val reqData2Iter = parts2.iterator
    context2.foreachPartition((topicPart, _) => {
      assertEquals(reqData2Iter.next(), topicPart)
    })
    assertEquals(None, context2.getFetchOffset(new TopicPartition("foo", 0)))
    assertEquals(10, context2.getFetchOffset(new TopicPartition("foo", 1)).get)
    assertEquals(15, context2.getFetchOffset(new TopicPartition("bar", 0)).get)
    assertEquals(None, context2.getFetchOffset(new TopicPartition("bar", 2)))
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData2.put(new TopicPartition("foo", 1),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    respData2.put(new TopicPartition("bar", 0),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(1, resp2.responseData.size)
    assertTrue(resp2.sessionId > 0)
  }

  @Test
  def testFetchSessionExpiration(): Unit = {
    val time = new MockTime()
    // set maximum entries to 2 to allow for eviction later
    val cache = new FetchSessionCache(2, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Create a new fetch session, session 1
    val session1req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session1context1 = fetchManager.newContext(session1req, false)
    assertEquals(classOf[FullFetchContext], session1context1.getClass)

    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(new TopicPartition("foo", 1), new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session1resp = session1context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session1resp.error())
    assertTrue(session1resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session1resp.responseData.size)

    // check session entered into case
    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    time.sleep(500)

    // Create a second new fetch session
    val session2req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session2context = fetchManager.newContext(session2req, false)
    assertEquals(classOf[FullFetchContext], session2context.getClass)

    val session2RespData = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    session2RespData.put(new TopicPartition("foo", 0),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    session2RespData.put(new TopicPartition("foo", 1), new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    val session2resp = session2context.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session2resp.error())
    assertTrue(session2resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session2resp.responseData.size)

    // both newly created entries are present in cache
    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    assertTrue(cache.get(session2resp.sessionId()).isDefined)
    time.sleep(500)

    // Create an incremental fetch request for session 1
    val context1v2 = fetchManager.newContext(
      new FetchRequestData().setSessionId(session1resp.sessionId()).setSessionEpoch(1),
      false
    )
    assertEquals(classOf[IncrementalFetchContext], context1v2.getClass)

    // total sleep time will now be large enough that fetch session 1 will be evicted if not correctly touched
    time.sleep(501)

    // create one final session to test that the least recently used entry is evicted
    // the second session should be evicted because the first session was incrementally fetched
    // more recently than the second session was created
    val session3req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session3context = fetchManager.newContext(session3req, false)
    assertEquals(classOf[FullFetchContext], session3context.getClass)

    val respData3 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData3.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    respData3.put(new TopicPartition("foo", 1),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session3resp = session3context.updateAndGenerateResponseData(respData3)
    assertEquals(Errors.NONE, session3resp.error())
    assertTrue(session3resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session3resp.responseData.size)

    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    assertFalse(cache.get(session2resp.sessionId()).isDefined, "session 2 should have been evicted by latest session, as session 1 was used more recently")
    assertTrue(cache.get(session3resp.sessionId()).isDefined)
  }

  @Test
  def testPrivilegedSessionHandling(): Unit = {
    val time = new MockTime()
    // set maximum entries to 2 to allow for eviction later
    val cache = new FetchSessionCache(2, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Create a new fetch session, session 1
    val session1req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session1context = fetchManager.newContext(session1req, true)
    assertEquals(classOf[FullFetchContext], session1context.getClass)

    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(new TopicPartition("foo", 1), new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session1resp = session1context.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session1resp.error())
    assertTrue(session1resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session1resp.responseData.size)
    assertEquals(1, cache.size)

    // move time forward to age session 1 a little compared to session 2
    time.sleep(500)

    // Create a second new fetch session, unprivileged
    val session2req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session2context = fetchManager.newContext(session2req, false)
    assertEquals(classOf[FullFetchContext], session2context.getClass)

    val session2RespData = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    session2RespData.put(new TopicPartition("foo", 0),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    session2RespData.put(new TopicPartition("foo", 1),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session2resp = session2context.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, session2resp.error())
    assertTrue(session2resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session2resp.responseData.size)

    // both newly created entries are present in cache
    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    assertTrue(cache.get(session2resp.sessionId()).isDefined)
    assertEquals(2, cache.size)
    time.sleep(500)

    // create a session to test session1 privileges mean that session 1 is retained and session 2 is evicted
    val session3req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session3context = fetchManager.newContext(session3req, true)
    assertEquals(classOf[FullFetchContext], session3context.getClass)

    val respData3 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData3.put(new TopicPartition("foo", 0),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData3.put(new TopicPartition("foo", 1),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session3resp = session3context.updateAndGenerateResponseData(respData3)
    assertEquals(Errors.NONE, session3resp.error())
    assertTrue(session3resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session3resp.responseData.size)

    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    // even though session 2 is more recent than session 1, and has not reached expiry time, it is less
    // privileged than session 2, and thus session 3 should be entered and session 2 evicted.
    assertFalse(cache.get(session2resp.sessionId()).isDefined, "session 2 should have been evicted by session 3")
    assertTrue(cache.get(session3resp.sessionId()).isDefined)
    assertEquals(2, cache.size)

    time.sleep(501)

    // create a final session to test whether session1 can be evicted due to age even though it is privileged
    val session4req = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val session4context = fetchManager.newContext(session4req, true)
    assertEquals(classOf[FullFetchContext], session4context.getClass)

    val respData4 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData4.put(new TopicPartition("foo", 0),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData4.put(new TopicPartition("foo", 1),
      new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val session4resp = session3context.updateAndGenerateResponseData(respData4)
    assertEquals(Errors.NONE, session4resp.error())
    assertTrue(session4resp.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, session4resp.responseData.size)

    assertFalse(cache.get(session1resp.sessionId()).isDefined, "session 1 should have been evicted by session 4 even though it is privileged as it has hit eviction time")
    assertTrue(cache.get(session3resp.sessionId()).isDefined)
    assertTrue(cache.get(session4resp.sessionId()).isDefined)
    assertEquals(2, cache.size)
  }

  @Test
  def testZeroSizeFetchSession(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic("foo").setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(0)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100),
          new FetchPartition()
            .setPartition(1)
            .setFetchOffset(10)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
        ))
      ))

    val context1 = fetchManager.newContext(reqData1, false)
    assertEquals(classOf[FullFetchContext], context1.getClass)

    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(100)
        .setLastStableOffset(100)
        .setLogStartOffset(100))
    respData1.put(new TopicPartition("foo", 1), new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error)
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData.size)

    // Create an incremental fetch request that removes foo-0 and foo-1
    // Verify that the previous fetch session was closed.
    val reqData2 = new FetchRequestData()
      .setSessionId(resp1.sessionId)
      .setSessionEpoch(1)
      .setForgottenTopicsData(util.Arrays.asList(
        new ForgottenTopic().setTopic("foo").setPartitions(util.Arrays.asList(0, 1))))

    val context2 = fetchManager.newContext(reqData2, false)
    assertEquals(classOf[SessionlessFetchContext], context2.getClass)

    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(INVALID_SESSION_ID, resp2.sessionId)
    assertTrue(resp2.responseData.isEmpty)
    assertEquals(0, cache.size)
  }

  @Test
  def testDivergingEpoch(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 2)

    val reqData = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic(tp1.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp1.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(5)
            .setLastFetchedEpoch(4))),
        new FetchTopic().setTopic(tp2.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp2.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(100)
            .setCurrentLeaderEpoch(5)
            .setLastFetchedEpoch(4)))
      ))

    // Full fetch context returns all partitions in the response
    val context1 = fetchManager.newContext(reqData, isFollower = false)
    assertEquals(classOf[FullFetchContext], context1.getClass)

    val respData = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData.put(tp1, new FetchResponseData.PartitionData()
        .setPartitionIndex(tp1.partition)
        .setHighWatermark(105)
        .setLastStableOffset(105)
        .setLogStartOffset(0))
    val divergingEpoch = new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(90)
    respData.put(tp2, new FetchResponseData.PartitionData()
        .setPartitionIndex(tp2.partition)
        .setHighWatermark(105)
        .setLastStableOffset(105)
        .setLogStartOffset(0)
        .setDivergingEpoch(divergingEpoch))
    val resp1 = context1.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp1.error)
    assertNotEquals(INVALID_SESSION_ID, resp1.sessionId)
    assertEquals(Utils.mkSet(tp1, tp2), resp1.responseData.keySet)

    // Incremental fetch context returns partitions with divergent epoch even if none
    // of the other conditions for return are met.
    val context2 = fetchManager.newContext(
      new FetchRequestData().setSessionId(resp1.sessionId).setSessionEpoch(1),
      isFollower = false
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)

    val resp2 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(resp1.sessionId, resp2.sessionId)
    assertEquals(Collections.singleton(tp2), resp2.responseData.keySet)

    // All partitions with divergent epoch should be returned.
    respData.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(105)
      .setLastStableOffset(105)
      .setLogStartOffset(0)
      .setDivergingEpoch(divergingEpoch))
    val resp3 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp3.error)
    assertEquals(resp1.sessionId, resp3.sessionId)
    assertEquals(Utils.mkSet(tp1, tp2), resp3.responseData.keySet)

    // Partitions that meet other conditions should be returned regardless of whether
    // divergingEpoch is set or not.
    respData.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(110)
      .setLastStableOffset(110)
      .setLogStartOffset(0))
    val resp4 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp4.error)
    assertEquals(resp1.sessionId, resp4.sessionId)
    assertEquals(Utils.mkSet(tp1, tp2), resp4.responseData.keySet)
  }

  @Test
  def testDeprioritizesPartitionsWithRecordsOnly(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 2)
    val tp3 = new TopicPartition("zar", 3)

    val reqData = new FetchRequestData()
      .setSessionId(INVALID_SESSION_ID)
      .setSessionEpoch(INITIAL_EPOCH)
      .setTopics(util.Arrays.asList(
        new FetchTopic().setTopic(tp1.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp1.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(1000)
            .setCurrentLeaderEpoch(5)
            .setLastFetchedEpoch(4))),
        new FetchTopic().setTopic(tp2.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp2.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(1000)
            .setCurrentLeaderEpoch(5)
            .setLastFetchedEpoch(4))),
        new FetchTopic().setTopic(tp3.topic).setPartitions(util.Arrays.asList(
          new FetchPartition()
            .setPartition(tp3.partition)
            .setFetchOffset(0)
            .setLogStartOffset(0)
            .setPartitionMaxBytes(1000)
            .setCurrentLeaderEpoch(5)
            .setLastFetchedEpoch(4)))
      ))

    // Full fetch context returns all partitions in the response
    val context1 = fetchManager.newContext(reqData, isFollower = false)
    assertEquals(classOf[FullFetchContext], context1.getClass)

    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    respData1.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    respData1.put(tp3, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp3.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))

    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error)
    assertNotEquals(INVALID_SESSION_ID, resp1.sessionId)
    assertEquals(Utils.mkSet(tp1, tp2, tp3), resp1.responseData.keySet())

    // Incremental fetch context returns partitions with changes but only deprioritizes
    // the partitions with records
    val context2 = fetchManager.newContext(
      new FetchRequestData().setSessionId(resp1.sessionId).setSessionEpoch(1),
      isFollower = false
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)

    // Partitions are ordered in the session as per last response
    assertPartitionsOrder(context2, Seq(tp1, tp2, tp3))

    // Response is empty
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(resp1.sessionId, resp2.sessionId)
    assertEquals(Collections.emptySet(), resp2.responseData.keySet)

    // All partitions with changes should be returned.
    val respData3 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData3.put(tp1, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp1.partition)
      .setHighWatermark(60)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    respData3.put(tp2, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp2.partition)
      .setHighWatermark(60)
      .setLastStableOffset(50)
      .setLogStartOffset(0)
      .setRecords(MemoryRecords.withRecords(CompressionType.NONE,
        new SimpleRecord(100, null))))
    respData3.put(tp3, new FetchResponseData.PartitionData()
      .setPartitionIndex(tp3.partition)
      .setHighWatermark(50)
      .setLastStableOffset(50)
      .setLogStartOffset(0))
    val resp3 = context2.updateAndGenerateResponseData(respData3)
    assertEquals(Errors.NONE, resp3.error)
    assertEquals(resp1.sessionId, resp3.sessionId)
    assertEquals(Utils.mkSet(tp1, tp2), resp3.responseData.keySet)

    // Only the partitions whose returned records in the last response
    // were deprioritized
    assertPartitionsOrder(context2, Seq(tp1, tp3, tp2))
  }

  private def assertPartitionsOrder(context: FetchContext, partitions: Seq[TopicPartition]): Unit = {
    val partitionsInContext = ArrayBuffer.empty[TopicPartition]
    context.foreachPartition { (tp, _) =>
      partitionsInContext += tp
    }
    assertEquals(partitions, partitionsInContext.toSeq)
  }
}
