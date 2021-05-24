/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import kafka.utils.MockTime
import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{AuthenticationException, DisconnectException}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.{ArgumentMatchers, Mockito}

import java.util
import scala.collection.mutable

class InterBrokerSendThreadTest {
  private val time = new MockTime()
  private val networkClient: NetworkClient = EasyMock.createMock(classOf[NetworkClient])
  private val completionHandler = new StubCompletionHandler
  private val requestTimeoutMs = 1000

  class TestInterBrokerSendThread(networkClient: NetworkClient = networkClient,
                                  exceptionCallback: Throwable => Unit = t => throw t)
    extends InterBrokerSendThread("name", networkClient, requestTimeoutMs, time) {
    private val queue = mutable.Queue[RequestAndCompletionHandler]()

    def enqueue(request: RequestAndCompletionHandler): Unit = {
      queue += request
    }

    override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
      if (queue.isEmpty) {
        None
      } else {
        Some(queue.dequeue())
      }
    }
    override def pollOnce(maxTimeoutMs: Long): Unit = {
      try super.pollOnce(maxTimeoutMs)
      catch {
        case e: Throwable => exceptionCallback(e)
      }
    }

  }

  @Test
  def shutdownThreadShouldNotCauseException(): Unit = {
    val networkClient = Mockito.mock(classOf[NetworkClient])
    // InterBrokerSendThread#shutdown calls NetworkClient#initiateClose first so NetworkClient#poll
    // can throw DisconnectException when thread is running
    Mockito.when(networkClient.poll(ArgumentMatchers.anyLong, ArgumentMatchers.anyLong)).thenThrow(new DisconnectException())
    var exception: Throwable = null
    val thread = new TestInterBrokerSendThread(networkClient, e => exception = e)
    thread.shutdown()
    thread.pollOnce(100)
    assertNull(exception)
  }

  @Test
  def shouldNotSendAnythingWhenNoRequests(): Unit = {
    val sendThread = new TestInterBrokerSendThread()

    // poll is always called but there should be no further invocations on NetworkClient
    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
      .andReturn(new util.ArrayList())

    EasyMock.replay(networkClient)

    sendThread.doWork()

    EasyMock.verify(networkClient)
    assertFalse(completionHandler.executedWithDisconnectedResponse)
  }

  @Test
  def shouldCreateClientRequestAndSendWhenNodeIsReady(): Unit = {
    val request = new StubRequestBuilder()
    val node = new Node(1, "", 8080)
    val handler = RequestAndCompletionHandler(time.milliseconds(), node, request, completionHandler)
    val sendThread = new TestInterBrokerSendThread()

    val clientRequest = new ClientRequest("dest", request, 0, "1", 0, true, requestTimeoutMs, handler.handler)

    EasyMock.expect(networkClient.newClientRequest(
      EasyMock.eq("1"),
      EasyMock.same(handler.request),
      EasyMock.anyLong(),
      EasyMock.eq(true),
      EasyMock.eq(requestTimeoutMs),
      EasyMock.same(handler.handler)))
      .andReturn(clientRequest)

    EasyMock.expect(networkClient.ready(node, time.milliseconds()))
      .andReturn(true)

    EasyMock.expect(networkClient.send(clientRequest, time.milliseconds()))

    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
      .andReturn(new util.ArrayList())

    EasyMock.replay(networkClient)

    sendThread.enqueue(handler)
    sendThread.doWork()

    EasyMock.verify(networkClient)
    assertFalse(completionHandler.executedWithDisconnectedResponse)
  }

  @Test
  def shouldCallCompletionHandlerWithDisconnectedResponseWhenNodeNotReady(): Unit = {
    val request = new StubRequestBuilder
    val node = new Node(1, "", 8080)
    val handler = RequestAndCompletionHandler(time.milliseconds(), node, request, completionHandler)
    val sendThread = new TestInterBrokerSendThread()

    val clientRequest = new ClientRequest("dest", request, 0, "1", 0, true, requestTimeoutMs, handler.handler)

    EasyMock.expect(networkClient.newClientRequest(
      EasyMock.eq("1"),
      EasyMock.same(handler.request),
      EasyMock.anyLong(),
      EasyMock.eq(true),
      EasyMock.eq(requestTimeoutMs),
      EasyMock.same(handler.handler)))
      .andReturn(clientRequest)

    EasyMock.expect(networkClient.ready(node, time.milliseconds()))
      .andReturn(false)

    EasyMock.expect(networkClient.connectionDelay(EasyMock.anyObject(), EasyMock.anyLong()))
      .andReturn(0)

    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
      .andReturn(new util.ArrayList())

    EasyMock.expect(networkClient.connectionFailed(node))
      .andReturn(true)

    EasyMock.expect(networkClient.authenticationException(node))
      .andReturn(new AuthenticationException(""))

    EasyMock.replay(networkClient)

    sendThread.enqueue(handler)
    sendThread.doWork()

    EasyMock.verify(networkClient)
    assertTrue(completionHandler.executedWithDisconnectedResponse)
  }

  @Test
  def testFailingExpiredRequests(): Unit = {
    val request = new StubRequestBuilder()
    val node = new Node(1, "", 8080)
    val handler = RequestAndCompletionHandler(time.milliseconds(), node, request, completionHandler)
    val sendThread = new TestInterBrokerSendThread()

    val clientRequest = new ClientRequest("dest",
      request,
      0,
      "1",
      time.milliseconds(),
      true,
      requestTimeoutMs,
      handler.handler)
    time.sleep(1500)

    EasyMock.expect(networkClient.newClientRequest(
      EasyMock.eq("1"),
      EasyMock.same(handler.request),
      EasyMock.eq(handler.creationTimeMs),
      EasyMock.eq(true),
      EasyMock.eq(requestTimeoutMs),
      EasyMock.same(handler.handler)))
      .andReturn(clientRequest)

    // make the node unready so the request is not cleared
    EasyMock.expect(networkClient.ready(node, time.milliseconds()))
      .andReturn(false)

    EasyMock.expect(networkClient.connectionDelay(EasyMock.anyObject(), EasyMock.anyLong()))
      .andReturn(0)

    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
      .andReturn(new util.ArrayList())

    // rule out disconnects so the request stays for the expiry check
    EasyMock.expect(networkClient.connectionFailed(node))
      .andReturn(false)

    EasyMock.replay(networkClient)

    sendThread.enqueue(handler)
    sendThread.doWork()

    EasyMock.verify(networkClient)
    assertFalse(sendThread.hasUnsentRequests)
    assertTrue(completionHandler.executedWithDisconnectedResponse)
  }

  private class StubRequestBuilder extends AbstractRequest.Builder(ApiKeys.END_TXN) {
    override def build(version: Short): Nothing = ???
  }

  private class StubCompletionHandler extends RequestCompletionHandler {
    var executedWithDisconnectedResponse = false
    var response: ClientResponse = _
    override def onComplete(response: ClientResponse): Unit = {
      this.executedWithDisconnectedResponse = response.wasDisconnected()
      this.response = response
    }
  }

}
