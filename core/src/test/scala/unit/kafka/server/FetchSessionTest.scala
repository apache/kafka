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
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.{FetchRequest, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import scala.jdk.CollectionConverters._
import java.util
import java.util.{Collections, Optional}

import scala.collection.mutable.ArrayBuffer

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

  private def dummyCreate(size: Int): (FetchSession.CACHE_MAP, FetchSession.TOPIC_ID_MAP) = {
    val cacheMap = new FetchSession.CACHE_MAP(size)
    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put("topic", Uuid.randomUuid())
    for (i <- 0 until size) {
      cacheMap.add(new CachedPartition("test", i, topicIds.get("test")))
    }
    (cacheMap, topicIds)
  }

  @Test
  def testSessionCache(): Unit = {
    val cache = new FetchSessionCache(3, 100)
    assertEquals(0, cache.size)
    val id1 = cache.maybeCreateSession(0, false, 10, true, () => dummyCreate(10))
    val id2 = cache.maybeCreateSession(10, false, 20, true, () => dummyCreate(20))
    val id3 = cache.maybeCreateSession(20, false, 30, true, () => dummyCreate(30))
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(30, false, 40, true, () => dummyCreate(40)))
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(40, false, 5, true, () => dummyCreate(5)))
    assertCacheContains(cache, id1, id2, id3)
    cache.touch(cache.get(id1).get, 200)
    val id4 = cache.maybeCreateSession(210, false, 11, true, () => dummyCreate(11))
    assertCacheContains(cache, id1, id3, id4)
    cache.touch(cache.get(id1).get, 400)
    cache.touch(cache.get(id3).get, 390)
    cache.touch(cache.get(id4).get, 400)
    val id5 = cache.maybeCreateSession(410, false, 50, true, () => dummyCreate(50))
    assertCacheContains(cache, id3, id4, id5)
    assertEquals(INVALID_SESSION_ID, cache.maybeCreateSession(410, false, 5, true, () => dummyCreate(5)))
    val id6 = cache.maybeCreateSession(410, true, 5, true, () => dummyCreate(5))
    assertCacheContains(cache, id3, id5, id6)
  }

  @Test
  def testResizeCachedSessions(): Unit = {
    val cache = new FetchSessionCache(2, 100)
    assertEquals(0, cache.totalPartitions)
    assertEquals(0, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    val id1 = cache.maybeCreateSession(0, false, 2, true, () => dummyCreate(2))
    assertTrue(id1 > 0)
    assertCacheContains(cache, id1)
    val session1 = cache.get(id1).get
    assertEquals(2, session1.size)
    assertEquals(2, cache.totalPartitions)
    assertEquals(1, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    val id2 = cache.maybeCreateSession(0, false, 4, true, () => dummyCreate(4))
    val session2 = cache.get(id2).get
    assertTrue(id2 > 0)
    assertCacheContains(cache, id1, id2)
    assertEquals(6, cache.totalPartitions)
    assertEquals(2, cache.size)
    assertEquals(0, cache.evictionsMeter.count)
    cache.touch(session1, 200)
    cache.touch(session2, 200)
    val id3 = cache.maybeCreateSession(200, false, 5, true, () => dummyCreate(5))
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

  private val EMPTY_PART_LIST = Collections.unmodifiableList(new util.ArrayList[TopicPartition]())

  def createRequest(metadata: JFetchMetadata,
                    fetchData: util.Map[TopicPartition, FetchRequest.PartitionData],
                    topicIds: util.Map[String, Uuid],
                    toForget: util.List[TopicPartition], isFromFollower: Boolean): FetchRequest = {
    new FetchRequest.Builder(ApiKeys.FETCH.latestVersion, ApiKeys.FETCH.latestVersion, if (isFromFollower) 1 else FetchRequest.CONSUMER_REPLICA_ID,
      0, 0, fetchData, topicIds).metadata(metadata).toForget(toForget).build
  }

  def createRequestWithoutTopicIds(metadata: JFetchMetadata,
                    fetchData: util.Map[TopicPartition, FetchRequest.PartitionData],
                    topicIds: util.Map[String, Uuid],
                    toForget: util.List[TopicPartition], isFromFollower: Boolean): FetchRequest = {
    new FetchRequest.Builder(12, 12, if (isFromFollower) 1 else FetchRequest.CONSUMER_REPLICA_ID,
      0, 0, fetchData, topicIds).metadata(metadata).toForget(toForget).build
  }

  @Test
  def testCachedLeaderEpoch(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)

    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 1)
    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid()).asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    def cachedLeaderEpochs(context: FetchContext): Map[TopicPartition, Optional[Integer]] = {
      val mapBuilder = Map.newBuilder[TopicPartition, Optional[Integer]]
      context.foreachPartition((tp, _, data) => mapBuilder += tp -> data.currentLeaderEpoch)
      mapBuilder.result()
    }

    val requestData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData1.put(tp0, new FetchRequest.PartitionData(0, 0, 100, Optional.empty()))
    requestData1.put(tp1, new FetchRequest.PartitionData(10, 0, 100, Optional.of(1)))
    requestData1.put(tp2, new FetchRequest.PartitionData(10, 0, 100, Optional.of(2)))

    val request1 = createRequest(JFetchMetadata.INITIAL, requestData1, topicIds, EMPTY_PART_LIST, false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
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
    val requestData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(sessionId, 1), requestData2, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )
    val epochs2 = cachedLeaderEpochs(context2)
    assertEquals(Optional.empty(), epochs1(tp0))
    assertEquals(Optional.of(1), epochs2(tp1))
    assertEquals(Optional.of(2), epochs2(tp2))
    context2.updateAndGenerateResponseData(response).sessionId()

    // Now verify we can change the leader epoch and the context is updated
    val requestData3 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData3.put(tp0, new FetchRequest.PartitionData(0, 0, 100, Optional.of(6)))
    requestData3.put(tp1, new FetchRequest.PartitionData(10, 0, 100, Optional.empty()))
    requestData3.put(tp2, new FetchRequest.PartitionData(10, 0, 100, Optional.of(3)))

    val request3 = createRequest(new JFetchMetadata(sessionId, 2), requestData3, topicIds, EMPTY_PART_LIST, false)
    val context3 = fetchManager.newContext(
      request3.version,
      request3.metadata,
      request3.isFromFollower,
      request3.fetchData(topicNames),
      request3.forgottenTopics(topicNames),
      topicIds
    )
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
    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid()).asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    def cachedLeaderEpochs(context: FetchContext): Map[TopicPartition, Optional[Integer]] = {
      val mapBuilder = Map.newBuilder[TopicPartition, Optional[Integer]]
      context.foreachPartition((tp, _, data) => mapBuilder += tp -> data.currentLeaderEpoch)
      mapBuilder.result()
    }

    def cachedLastFetchedEpochs(context: FetchContext): Map[TopicPartition, Optional[Integer]] = {
      val mapBuilder = Map.newBuilder[TopicPartition, Optional[Integer]]
      context.foreachPartition((tp, _, data) => mapBuilder += tp -> data.lastFetchedEpoch)
      mapBuilder.result()
    }

    val requestData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData1.put(tp0, new FetchRequest.PartitionData(0, 0, 100, Optional.empty[Integer], Optional.empty[Integer]))
    requestData1.put(tp1, new FetchRequest.PartitionData(10, 0, 100, Optional.of(1), Optional.empty[Integer]))
    requestData1.put(tp2, new FetchRequest.PartitionData(10, 0, 100, Optional.of(2), Optional.of(1)))

    val request1 = createRequest(JFetchMetadata.INITIAL, requestData1, topicIds, EMPTY_PART_LIST, false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.of(1), tp2 -> Optional.of(2)),
      cachedLeaderEpochs(context1))
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.empty, tp2 -> Optional.of(1)),
      cachedLastFetchedEpochs(context1))

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
    val requestData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(sessionId, 1), requestData2, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.of(1), tp2 -> Optional.of(2)), cachedLeaderEpochs(context2))
    assertEquals(Map(tp0 -> Optional.empty, tp1 -> Optional.empty, tp2 -> Optional.of(1)),
      cachedLastFetchedEpochs(context2))
    context2.updateAndGenerateResponseData(response).sessionId()

    // Now verify we can change the leader epoch and the context is updated
    val requestData3 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    requestData3.put(tp0, new FetchRequest.PartitionData(0, 0, 100, Optional.of(6), Optional.of(5)))
    requestData3.put(tp1, new FetchRequest.PartitionData(10, 0, 100, Optional.empty[Integer], Optional.empty[Integer]))
    requestData3.put(tp2, new FetchRequest.PartitionData(10, 0, 100, Optional.of(3), Optional.of(3)))

    val request3 = createRequest(new JFetchMetadata(sessionId, 2), requestData3, topicIds, EMPTY_PART_LIST, false)
    val context3 = fetchManager.newContext(
      request3.version,
      request3.metadata,
      request3.isFromFollower,
      request3.fetchData(topicNames),
      request3.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(Map(tp0 -> Optional.of(6), tp1 -> Optional.empty, tp2 -> Optional.of(3)),
      cachedLeaderEpochs(context3))
    assertEquals(Map(tp0 -> Optional.of(5), tp1 -> Optional.empty, tp2 -> Optional.of(3)),
      cachedLastFetchedEpochs(context2))
  }

  @Test
  def testFetchRequests(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Verify that SESSIONLESS requests get a SessionlessFetchContext
    val request = createRequest(JFetchMetadata.LEGACY, new util.HashMap[TopicPartition, FetchRequest.PartitionData](), topicIds, EMPTY_PART_LIST, true)
    val context = fetchManager.newContext(
      request.version,
      request.metadata,
      request.isFromFollower,
      request.fetchData(topicNames),
      request.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[SessionlessFetchContext], context.getClass)

    // Create a new fetch session with a FULL fetch request
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData2.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val request2 = createRequest(JFetchMetadata.INITIAL, reqData2, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[FullFetchContext], context2.getClass)
    val reqData2Iter = reqData2.entrySet().iterator()
    context2.foreachPartition((topicPart, topicId, data) => {
      val entry = reqData2Iter.next()
      assertEquals(entry.getKey, topicPart)
      assertEquals(topicIds.get(entry.getKey.topic()), topicId)
      assertEquals(entry.getValue, data)
    })
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
    assertEquals(respData2, resp2.responseData(topicNames, request2.version))

    // Test trying to create a new session with an invalid epoch
    val request3 = createRequest(new JFetchMetadata(resp2.sessionId(), 5), reqData2, topicIds, EMPTY_PART_LIST, false)
    val context3 = fetchManager.newContext(
      request3.version,
      request3.metadata,
      request3.isFromFollower,
      request3.fetchData(topicNames),
      request3.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[SessionErrorContext], context3.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context3.updateAndGenerateResponseData(respData2).error())

    // Test trying to create a new session with a non-existent session id
    val request4 = createRequest(new JFetchMetadata(resp2.sessionId() + 1, 1), reqData2, topicIds, EMPTY_PART_LIST, false)
    val context4 = fetchManager.newContext(
      request4.version,
      request4.metadata,
      request4.isFromFollower,
      request4.fetchData(topicNames),
      request4.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(Errors.FETCH_SESSION_ID_NOT_FOUND,
      context4.updateAndGenerateResponseData(respData2).error())

    // Continue the first fetch session we created.
    val reqData5 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request5 = createRequest( new JFetchMetadata(resp2.sessionId(), 1), reqData5, topicIds, EMPTY_PART_LIST, false)
    val context5 = fetchManager.newContext(
      request5.version,
      request5.metadata,
      request5.isFromFollower,
      request5.fetchData(topicNames),
      request5.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[IncrementalFetchContext], context5.getClass)
    val reqData5Iter = reqData2.entrySet().iterator()
    context5.foreachPartition((topicPart, topicId, data) => {
      val entry = reqData5Iter.next()
      assertEquals(entry.getKey, topicPart)
      assertEquals(topicIds.get(entry.getKey.topic()), topicId)
      assertEquals(entry.getValue, data)
    })
    assertEquals(10, context5.getFetchOffset(new TopicPartition("foo", 1)).get)
    val resp5 = context5.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp5.error())
    assertEquals(resp2.sessionId(), resp5.sessionId())
    assertEquals(0, resp5.responseData(topicNames, request5.version).size())

    // Test setting an invalid fetch session epoch.
    val request6 = createRequest( new JFetchMetadata(resp2.sessionId(), 5), reqData2, topicIds, EMPTY_PART_LIST, false)
    val context6 = fetchManager.newContext(
      request6.version,
      request6.metadata,
      request6.isFromFollower,
      request6.fetchData(topicNames),
      request6.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[SessionErrorContext], context6.getClass)
    assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH,
      context6.updateAndGenerateResponseData(respData2).error())

    // Test generating a throttled response for the incremental fetch session
    val reqData7 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request7 = createRequest( new JFetchMetadata(resp2.sessionId(), 2), reqData7, topicIds, EMPTY_PART_LIST, false)
    val context7 = fetchManager.newContext(
      request7.version,
      request7.metadata,
      request7.isFromFollower,
      request7.fetchData(topicNames),
      request7.forgottenTopics(topicNames),
      topicIds
    )
    val resp7 = context7.getThrottledResponse(100)
    assertEquals(Errors.NONE, resp7.error())
    assertEquals(resp2.sessionId(), resp7.sessionId())
    assertEquals(100, resp7.throttleTimeMs())

    // Close the incremental fetch session.
    val prevSessionId = resp5.sessionId
    var nextSessionId = prevSessionId
    do {
      val reqData8 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      reqData8.put(new TopicPartition("bar", 0), new FetchRequest.PartitionData(0, 0, 100,
        Optional.empty()))
      reqData8.put(new TopicPartition("bar", 1), new FetchRequest.PartitionData(10, 0, 100,
        Optional.empty()))
      val request8 = createRequest(new JFetchMetadata(prevSessionId, FINAL_EPOCH), reqData8, topicIds, EMPTY_PART_LIST, false)
      val context8 = fetchManager.newContext(
        request8.version,
        request8.metadata,
        request8.isFromFollower,
        request8.fetchData(topicNames),
        request8.forgottenTopics(topicNames),
        topicIds
      )
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
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData1.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, resp1.responseData(topicNames, request1.version).size())

    // Create an incremental fetch request that removes foo-0 and adds bar-0
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(new TopicPartition("bar", 0), new FetchRequest.PartitionData(15, 0, 0,
      Optional.empty()))
    val removed2 = new util.ArrayList[TopicPartition]
    removed2.add(new TopicPartition("foo", 0))
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, topicIds, removed2, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val parts2 = Set(new TopicPartition("foo", 1), new TopicPartition("bar", 0))
    val reqData2Iter = parts2.iterator
    context2.foreachPartition((topicPart, _, _) => {
      assertEquals(reqData2Iter.next(), topicPart)
    })
    assertEquals(None, context2.getFetchOffset(new TopicPartition("foo", 0)))
    assertEquals(10, context2.getFetchOffset(new TopicPartition("foo", 1)).get)
    assertEquals(15, context2.getFetchOffset(new TopicPartition("bar", 0)).get)
    assertEquals(None, context2.getFetchOffset(new TopicPartition("bar", 2)))
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData2.put(new TopicPartition("foo", 1), new FetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    respData2.put(new TopicPartition("bar", 0), new FetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(10)
        .setLastStableOffset(10)
        .setLogStartOffset(10))
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(1, resp2.responseData(topicNames, request2.version).size)
    assertTrue(resp2.sessionId > 0)
  }

  // This test simulates a request without IDs sent to a broker with IDs.
  @Test
  def testFetchSessionWithUnknownIdOldRequestVersion(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData1.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val request1 = createRequestWithoutTopicIds(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    // Simulate unknown topic ID for foo.
    val topicNamesOnlyBar = Collections.singletonMap(topicIds.get("bar"), "bar")
    val topicIdsOnlyBar = Collections.singletonMap("bar", topicIds.get("bar"))
    // We should not throw error since we have an older request version.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNamesOnlyBar),
      request1.forgottenTopics(topicNamesOnlyBar),
      topicIdsOnlyBar
    )
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
    // Since we are ignoring IDs, we should have no errors.
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData(topicNames, request1.version).size)
    resp1.responseData(topicNames, request1.version).forEach( (_, resp) => assertEquals(Errors.NONE.code, resp.errorCode))
  }

  @Test
  def testIncrementalFetchSessionWithIdsWhenSessionDoesNotUseIds() : Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val topicIds = new util.HashMap[String, Uuid]()
    val topicNames = new util.HashMap[Uuid, String]()

    // Create a new fetch session with foo-0
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val request1 = createRequestWithoutTopicIds(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    // Start a fetch session using a request version that does not use topic IDs.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)

    // Create an incremental fetch request as though no topics changed. However, send a v13 request.
    // Also simulate the topic ID found on the server.
    val fooId = Uuid.randomUuid()
    topicIds.put("foo", fooId)
    topicNames.put(fooId, "foo")
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )

    assertEquals(classOf[SessionErrorContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR,
      context2.updateAndGenerateResponseData(respData2).error())
  }

  @Test
  def testIncrementalFetchSessionWithoutIdsWhenSessionUsesIds() : Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val fooId = Uuid.randomUuid()
    val topicIds = new util.HashMap[String, Uuid]()
    val topicNames = new util.HashMap[Uuid, String]()
    topicIds.put("foo", fooId)
    topicNames.put(fooId, "foo")

    // Create a new fetch session with foo-0
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    // Start a fetch session using a request version that uses topic IDs.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)

    // Create an incremental fetch request as though no topics changed. However, send a v12 request.
    // Also simulate the topic ID not found on the server
    topicIds.remove("foo")
    topicNames.remove(fooId)
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequestWithoutTopicIds(new JFetchMetadata(resp1.sessionId(), 1), reqData2, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )

    assertEquals(classOf[SessionErrorContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR,
      context2.updateAndGenerateResponseData(respData2).error())
  }

  @Test
  def testIncrementalFetchSessionWithIdsSwitchesIdForTopic() : Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val fooId = Uuid.randomUuid()
    val topicIds = new util.HashMap[String, Uuid]()
    val topicNames = new util.HashMap[Uuid, String]()
    topicIds.put("foo", fooId)
    topicNames.put(fooId, "foo")

    // Create a new fetch session with foo-0
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    // Start a fetch session using a request version that uses topic IDs.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(100)
      .setLastStableOffset(100)
      .setLogStartOffset(100))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)

    // Create an incremental fetch request adding a new partition to change the topic ID.
    // Also simulate the topic ID changed on the server.
    topicNames.remove(fooId)
    val newFooId = Uuid.randomUuid()
    topicIds.put("foo", newFooId)
    topicNames.put(newFooId, "foo")
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData2.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )

    assertEquals(classOf[SessionErrorContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR,
      context2.updateAndGenerateResponseData(respData2).error())
  }

  // This test simulates a session where the topic ID changes broker side (the one handling the request) in both the metadata cache and the log
  // -- as though the topic is deleted and recreated.
  @Test
  def testFetchSessionUpdateTopicIdsBrokerSide(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Create a new fetch session with foo-0 and bar-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData1.put(new TopicPartition("bar", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    // Start a fetch session. Simulate unknown partition foo-0.
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[FullFetchContext], context1.getClass)
    val respData1 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    respData1.put(new TopicPartition("bar", 1), new FetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setHighWatermark(10)
      .setLastStableOffset(10)
      .setLogStartOffset(10))
    respData1.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(-1)
      .setLastStableOffset(-1)
      .setLogStartOffset(-1)
      .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))
    val resp1 = context1.updateAndGenerateResponseData(respData1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() != INVALID_SESSION_ID)
    assertEquals(2, resp1.responseData(topicNames, request1.version).size)

    // Create an incremental fetch request as though no topics changed.
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId(), 1), reqData2, topicIds, EMPTY_PART_LIST, false)
    // Simulate ID changing on server.
    val topicNamesFooChanged =  Map(topicIds.get("bar") -> "bar", Uuid.randomUuid() -> "foo").asJava
    val topicIdsFooChanged = topicNamesFooChanged.asScala.map(_.swap).asJava
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNamesFooChanged),
      request2.forgottenTopics(topicNamesFooChanged),
      topicIdsFooChanged
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    // Likely if the topic ID is different in the broker, it will be different in the log. Simulate the log check finding an inconsistent ID.
    respData2.put(new TopicPartition("foo", 0), new FetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(-1)
      .setLastStableOffset(-1)
      .setLogStartOffset(-1)
      .setErrorCode(Errors.INCONSISTENT_TOPIC_ID.code))
    val resp2 = context2.updateAndGenerateResponseData(respData2)

    assertEquals(Errors.INCONSISTENT_TOPIC_ID, resp2.error)
    assertTrue(resp2.sessionId > 0)
    val responseData2 = resp2.responseData(topicNames, request2.version)
    // We should have no partition responses with this top level error.
    assertEquals(0, responseData2.size())
  }

  @Test
  def testFetchSessionExpiration(): Unit = {
    val time = new MockTime()
    // set maximum entries to 2 to allow for eviction later
    val cache = new FetchSessionCache(2, 1000)
    val fetchManager = new FetchManager(time, cache)
    val topicNames = Map(Uuid.randomUuid() -> "foo").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Create a new fetch session, session 1
    val session1req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session1req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session1req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val session1request1 = createRequest(JFetchMetadata.INITIAL, session1req, topicIds, EMPTY_PART_LIST, false)
    val session1context1 = fetchManager.newContext(
      session1request1.version,
      session1request1.metadata,
      session1request1.isFromFollower,
      session1request1.fetchData(topicNames),
      session1request1.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session1resp.responseData(topicNames, session1request1.version).size)

    // check session entered into case
    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    time.sleep(500)

    // Create a second new fetch session
    val session2req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session2req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session2req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val session2request1 = createRequest(JFetchMetadata.INITIAL, session1req, topicIds, EMPTY_PART_LIST, false)
    val session2context = fetchManager.newContext(
      session2request1.version,
      session2request1.metadata,
      session2request1.isFromFollower,
      session2request1.fetchData(topicNames),
      session2request1.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session2resp.responseData(topicNames, session2request1.version()).size())

    // both newly created entries are present in cache
    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    assertTrue(cache.get(session2resp.sessionId()).isDefined)
    time.sleep(500)

    // Create an incremental fetch request for session 1
    val session1request2 = createRequest(
      new JFetchMetadata(session1resp.sessionId(), 1),
      new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData], topicIds,
      new util.ArrayList[TopicPartition], false)
    val context1v2 = fetchManager.newContext(
      session1request2.version,
      session1request2.metadata,
      session1request2.isFromFollower,
      session1request2.fetchData(topicNames),
      session1request2.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[IncrementalFetchContext], context1v2.getClass)

    // total sleep time will now be large enough that fetch session 1 will be evicted if not correctly touched
    time.sleep(501)

    // create one final session to test that the least recently used entry is evicted
    // the second session should be evicted because the first session was incrementally fetched
    // more recently than the second session was created
    val session3req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session3req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session3req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val session3request1 = createRequest(JFetchMetadata.INITIAL, session3req, topicIds, EMPTY_PART_LIST, false)
    val session3context = fetchManager.newContext(
      session3request1.version,
      session3request1.metadata,
      session3request1.isFromFollower,
      session3request1.fetchData(topicNames),
      session3request1.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session3resp.responseData(topicNames, session3request1.version).size)

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
    val topicNames = Map(Uuid.randomUuid() -> "foo").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Create a new fetch session, session 1
    val session1req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session1req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session1req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val session1request = createRequest(JFetchMetadata.INITIAL, session1req, topicIds, EMPTY_PART_LIST, true)
    val session1context = fetchManager.newContext(
      session1request.version,
      session1request.metadata,
      session1request.isFromFollower,
      session1request.fetchData(topicNames),
      session1request.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session1resp.responseData(topicNames, session1request.version).size)
    assertEquals(1, cache.size)

    // move time forward to age session 1 a little compared to session 2
    time.sleep(500)

    // Create a second new fetch session, unprivileged
    val session2req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session2req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session2req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val session2request = createRequest(JFetchMetadata.INITIAL, session1req, topicIds, EMPTY_PART_LIST, false)
    val session2context = fetchManager.newContext(
      session2request.version,
      session2request.metadata,
      session2request.isFromFollower,
      session2request.fetchData(topicNames),
      session2request.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session2resp.responseData(topicNames, session2request.version).size)

    // both newly created entries are present in cache
    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    assertTrue(cache.get(session2resp.sessionId()).isDefined)
    assertEquals(2, cache.size)
    time.sleep(500)

    // create a session to test session1 privileges mean that session 1 is retained and session 2 is evicted
    val session3req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session3req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session3req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val session3request = createRequest(JFetchMetadata.INITIAL, session3req, topicIds, EMPTY_PART_LIST, true)
    val session3context = fetchManager.newContext(
      session3request.version,
      session3request.metadata,
      session3request.isFromFollower,
      session3request.fetchData(topicNames),
      session3request.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session3resp.responseData(topicNames, session3request.version).size)

    assertTrue(cache.get(session1resp.sessionId()).isDefined)
    // even though session 2 is more recent than session 1, and has not reached expiry time, it is less
    // privileged than session 2, and thus session 3 should be entered and session 2 evicted.
    assertFalse(cache.get(session2resp.sessionId()).isDefined, "session 2 should have been evicted by session 3")
    assertTrue(cache.get(session3resp.sessionId()).isDefined)
    assertEquals(2, cache.size)

    time.sleep(501)

    // create a final session to test whether session1 can be evicted due to age even though it is privileged
    val session4req = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    session4req.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    session4req.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    val session4request = createRequest(JFetchMetadata.INITIAL, session4req, topicIds, EMPTY_PART_LIST, true)
    val session4context = fetchManager.newContext(
      session4request.version,
      session4request.metadata,
      session4request.isFromFollower,
      session4request.fetchData(topicNames),
      session4request.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, session4resp.responseData(topicNames, session4request.version).size)

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
    val topicNames = Map(Uuid.randomUuid() -> "foo").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    // Create a new fetch session with foo-0 and foo-1
    val reqData1 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData1.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0, 0, 100,
      Optional.empty()))
    reqData1.put(new TopicPartition("foo", 1), new FetchRequest.PartitionData(10, 0, 100,
      Optional.empty()))
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData1, topicIds, EMPTY_PART_LIST, false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(2, resp1.responseData(topicNames, request1.version).size)

    // Create an incremental fetch request that removes foo-0 and foo-1
    // Verify that the previous fetch session was closed.
    val reqData2 = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val removed2 = new util.ArrayList[TopicPartition]
    removed2.add(new TopicPartition("foo", 0))
    removed2.add(new TopicPartition("foo", 1))
    val request2 = createRequest( new JFetchMetadata(resp1.sessionId, 1), reqData2, topicIds, removed2, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[SessionlessFetchContext], context2.getClass)
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(INVALID_SESSION_ID, resp2.sessionId)
    assertTrue(resp2.responseData(topicNames, request2.version).isEmpty)
    assertEquals(0, cache.size)
  }

  @Test
  def testDivergingEpoch(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 2)
    val topicNames = Map(Uuid.randomUuid() -> "foo", Uuid.randomUuid() -> "bar").asJava
    val topicIds = topicNames.asScala.map(_.swap).asJava

    val reqData = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData.put(tp1, new FetchRequest.PartitionData(100, 0, 1000, Optional.of(5), Optional.of(4)))
    reqData.put(tp2, new FetchRequest.PartitionData(100, 0, 1000, Optional.of(5), Optional.of(4)))

    // Full fetch context returns all partitions in the response
    val request1 = createRequest(JFetchMetadata.INITIAL, reqData, topicIds, EMPTY_PART_LIST, false)
    val context1 = fetchManager.newContext(
      request1.version,
      request1.metadata,
      request1.isFromFollower,
      request1.fetchData(topicNames),
      request1.forgottenTopics(topicNames),
      topicIds
    )
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
    assertEquals(Utils.mkSet(tp1, tp2), resp1.responseData(topicNames, request1.version).keySet)

    // Incremental fetch context returns partitions with divergent epoch even if none
    // of the other conditions for return are met.
    val request2 = createRequest(new JFetchMetadata(resp1.sessionId, 1), reqData, topicIds, EMPTY_PART_LIST, false)
    val context2 = fetchManager.newContext(
      request2.version,
      request2.metadata,
      request2.isFromFollower,
      request2.fetchData(topicNames),
      request2.forgottenTopics(topicNames),
      topicIds
    )
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)
    val resp2 = context2.updateAndGenerateResponseData(respData)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(resp1.sessionId, resp2.sessionId)
    assertEquals(Collections.singleton(tp2), resp2.responseData(topicNames, request2.version).keySet)

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
    assertEquals(Utils.mkSet(tp1, tp2), resp3.responseData(topicNames, request2.version).keySet)

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
    assertEquals(Utils.mkSet(tp1, tp2), resp4.responseData(topicNames, request2.version).keySet)
  }

  @Test
  def testDeprioritizesPartitionsWithRecordsOnly(): Unit = {
    val time = new MockTime()
    val cache = new FetchSessionCache(10, 1000)
    val fetchManager = new FetchManager(time, cache)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 2)
    val tp3 = new TopicPartition("zar", 3)
    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid(), "zar" -> Uuid.randomUuid()).asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    val reqData = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    reqData.put(tp1, new FetchRequest.PartitionData(100, 0, 1000, Optional.of(5), Optional.of(4)))
    reqData.put(tp2, new FetchRequest.PartitionData(100, 0, 1000, Optional.of(5), Optional.of(4)))
    reqData.put(tp3, new FetchRequest.PartitionData(100, 0, 1000, Optional.of(5), Optional.of(4)))

    // Full fetch context returns all partitions in the response
    val context1 = fetchManager.newContext(ApiKeys.FETCH.latestVersion(), JFetchMetadata.INITIAL, false,
     reqData, Collections.emptyList(), topicIds)
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
    assertEquals(Utils.mkSet(tp1, tp2, tp3), resp1.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet())

    // Incremental fetch context returns partitions with changes but only deprioritizes
    // the partitions with records
    val context2 = fetchManager.newContext(ApiKeys.FETCH.latestVersion(), new JFetchMetadata(resp1.sessionId, 1), false,
      reqData, Collections.emptyList(), topicIds)
    assertEquals(classOf[IncrementalFetchContext], context2.getClass)

    // Partitions are ordered in the session as per last response
    assertPartitionsOrder(context2, Seq(tp1, tp2, tp3))

    // Response is empty
    val respData2 = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
    val resp2 = context2.updateAndGenerateResponseData(respData2)
    assertEquals(Errors.NONE, resp2.error)
    assertEquals(resp1.sessionId, resp2.sessionId)
    assertEquals(Collections.emptySet(), resp2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet)

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
    assertEquals(Utils.mkSet(tp1, tp2), resp3.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet)

    // Only the partitions whose returned records in the last response
    // were deprioritized
    assertPartitionsOrder(context2, Seq(tp1, tp3, tp2))
  }

  private def assertPartitionsOrder(context: FetchContext, partitions: Seq[TopicPartition]): Unit = {
    val partitionsInContext = ArrayBuffer.empty[TopicPartition]
    context.foreachPartition { (tp, _, _) =>
      partitionsInContext += tp
    }
    assertEquals(partitions, partitionsInContext.toSeq)
  }
}
