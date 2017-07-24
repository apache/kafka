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

package kafka.controller

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch, TimeUnit}

import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.Logging
import org.apache.zookeeper.AsyncCallback.{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * ZookeeperClient is a zookeeper client that encourages pipelined requests to zookeeper.
  *
  * @param connectString comma separated host:port pairs, each corresponding to a zk server
  * @param sessionTimeoutMs session timeout in milliseconds
  * @param connectionTimeoutMs connection timeout in milliseconds
  * @param stateChangeHandler state change handler callbacks called by the underlying zookeeper client's EventThread.
  */
class ZookeeperClient(connectString: String, sessionTimeoutMs: Int, connectionTimeoutMs: Int, stateChangeHandler: StateChangeHandler) extends Logging {
  this.logIdent = "[ZookeeperClient]: "
  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]()
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]()

  info(s"Initializing a new session to $connectString.")
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZookeeperClientWatcher)
  waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)

  /**
    * Take an AsyncRequest and wait for its AsyncResponse.
    * @param request a single AsyncRequest to wait on.
    * @return the request's AsyncReponse.
    */
  def handle(request: AsyncRequest): AsyncResponse = {
    handle(Seq(request)).head
  }

  /**
    * Pipeline a sequence of AsyncRequests and wait for all of their AsyncResponses.
    * @param requests a sequence of AsyncRequests to wait on.
    * @return the AsyncResponses.
    */
  def handle(requests: Seq[AsyncRequest]): Seq[AsyncResponse] = inReadLock(initializationLock) {
    import scala.collection.JavaConverters._
    val countDownLatch = new CountDownLatch(requests.size)
    val responseQueue = new ArrayBlockingQueue[AsyncResponse](requests.size)
    requests.foreach {
      case CreateRequest(path, data, acl, createMode, ctx) => zooKeeper.create(path, data, acl.asJava, createMode, new StringCallback {
        override def processResult(rc: Int, path: String, ctx: Any, name: String) = {
          responseQueue.add(CreateResponse(rc, path, ctx, name))
          countDownLatch.countDown()
        }}, ctx)
      case DeleteRequest(path, version, ctx) => zooKeeper.delete(path, version, new VoidCallback {
        override def processResult(rc: Int, path: String, ctx: Any) = {
          responseQueue.add(DeleteResponse(rc, path, ctx))
          countDownLatch.countDown()
        }}, ctx)
      case ExistsRequest(path, ctx) => zooKeeper.exists(path, zNodeChangeHandlers.containsKey(path), new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(ExistsResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, ctx)
      case GetDataRequest(path, ctx) => zooKeeper.getData(path, zNodeChangeHandlers.containsKey(path), new DataCallback {
        override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat) = {
          responseQueue.add(GetDataResponse(rc, path, ctx, data, stat))
          countDownLatch.countDown()
        }}, ctx)
      case SetDataRequest(path, data, version, ctx) => zooKeeper.setData(path, data, version, new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(SetDataResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, ctx)
      case GetACLRequest(path, ctx) => zooKeeper.getACL(path, null, new ACLCallback {
        override def processResult(rc: Int, path: String, ctx: Any, acl: java.util.List[ACL], stat: Stat): Unit = {
          responseQueue.add(GetACLResponse(rc, path, ctx, Option(acl).map(_.asScala).orNull, stat))
          countDownLatch.countDown()
        }}, ctx)
      case SetACLRequest(path, acl, version, ctx) => zooKeeper.setACL(path, acl.asJava, version, new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(SetACLResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, ctx)
      case GetChildrenRequest(path, ctx) => zooKeeper.getChildren(path, zNodeChildChangeHandlers.containsKey(path), new Children2Callback {
        override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat) = {
          responseQueue.add(GetChildrenResponse(rc, path, ctx, Option(children).map(_.asScala).orNull, stat))
          countDownLatch.countDown()
        }}, ctx)
    }
    countDownLatch.await()
    responseQueue.asScala.toSeq
  }

  /**
    * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
    * @throws ZookeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
    * @throws ZookeeperClientExpiredException if the session expired either before or while waiting for connection.
    */
  def waitUntilConnected(): Unit = inLock(isConnectedOrExpiredLock) {
    waitUntilConnected(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  private def waitUntilConnected(timeout: Long, timeUnit: TimeUnit): Unit = {
    info("Waiting until connected.")
    var nanos = timeUnit.toNanos(timeout)
    inLock(isConnectedOrExpiredLock) {
      var state = zooKeeper.getState
      while (!state.isConnected && state.isAlive) {
        if (nanos <= 0) {
          throw new ZookeeperClientTimeoutException(s"Timed out waiting for connection while in state: $state")
        }
        nanos = isConnectedOrExpiredCondition.awaitNanos(nanos)
        state = zooKeeper.getState
      }
      if (state == States.AUTH_FAILED) {
        throw new ZookeeperClientAuthFailedException("Auth failed either before or while waiting for connection")
      } else if (state == States.CLOSED) {
        throw new ZookeeperClientExpiredException("Session expired either before or while waiting for connection")
      }
    }
    info("Connected.")
  }

  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): ExistsResponse = {
    registerZNodeChangeHandlers(Seq(zNodeChangeHandler)).head
  }

  def registerZNodeChangeHandlers(handlers: Seq[ZNodeChangeHandler]): Seq[ExistsResponse] = {
    handlers.foreach(handler => zNodeChangeHandlers.put(handler.path, handler))
    val asyncRequests = handlers.map(handler => ExistsRequest(handler.path, null))
    handle(asyncRequests).asInstanceOf[Seq[ExistsResponse]]
  }

  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): GetChildrenResponse = {
    registerZNodeChildChangeHandlers(Seq(zNodeChildChangeHandler)).head
  }

  def registerZNodeChildChangeHandlers(handlers: Seq[ZNodeChildChangeHandler]): Seq[GetChildrenResponse] = {
    handlers.foreach(handler => zNodeChildChangeHandlers.put(handler.path, handler))
    val asyncRequests = handlers.map(handler => GetChildrenRequest(handler.path, null))
    handle(asyncRequests).asInstanceOf[Seq[GetChildrenResponse]]
  }

  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove(path)
  }

  def close(): Unit = inWriteLock(initializationLock) {
    info("Closing.")
    zNodeChangeHandlers.clear()
    zNodeChildChangeHandlers.clear()
    zooKeeper.close()
    info("Closed.")
  }

  private def initialize(): Unit = {
    if (!zooKeeper.getState.isAlive) {
      info(s"Initializing a new session to $connectString.")
      var now = System.currentTimeMillis()
      val threshold = now + connectionTimeoutMs
      while (now < threshold) {
        try {
          zooKeeper.close()
          zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZookeeperClientWatcher)
          waitUntilConnected(threshold - now, TimeUnit.MILLISECONDS)
          return
        } catch {
          case _: Exception =>
            now = System.currentTimeMillis()
            if (now < threshold) {
              Thread.sleep(1000)
              now = System.currentTimeMillis()
            }
        }
      }
      info(s"Timed out waiting for connection during session initialization while in state: ${zooKeeper.getState}")
      stateChangeHandler.onConnectionTimeout
    }
  }

  private object ZookeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      debug("Received event: " + event)
      if (event.getPath == null) {
        inLock(isConnectedOrExpiredLock) {
          isConnectedOrExpiredCondition.signalAll()
        }
        if (event.getState == KeeperState.AuthFailed) {
          info("Auth failed.")
          stateChangeHandler.onAuthFailure
        } else if (event.getState == KeeperState.Expired) {
          inWriteLock(initializationLock) {
            info("Session expired.")
            stateChangeHandler.beforeInitializingSession
            initialize()
            stateChangeHandler.afterInitializingSession
          }
        }
      } else if (event.getType == EventType.NodeCreated) {
        Option(zNodeChangeHandlers.get(event.getPath)).foreach(_.handleCreation)
      } else if (event.getType == EventType.NodeDeleted) {
        Option(zNodeChangeHandlers.get(event.getPath)).foreach(_.handleDeletion)
      } else if (event.getType == EventType.NodeDataChanged) {
        Option(zNodeChangeHandlers.get(event.getPath)).foreach(_.handleDataChange)
      } else if (event.getType == EventType.NodeChildrenChanged) {
        Option(zNodeChildChangeHandlers.get(event.getPath)).foreach(_.handleChildChange)
      }
    }
  }
}

trait StateChangeHandler {
  def beforeInitializingSession: Unit
  def afterInitializingSession: Unit
  def onAuthFailure: Unit
  def onConnectionTimeout: Unit
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation: Unit
  def handleDeletion: Unit
  def handleDataChange: Unit
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange: Unit
}

sealed trait AsyncRequest {
  val path: String
  val ctx: Any
}
case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode, ctx: Any) extends AsyncRequest
case class DeleteRequest(path: String, version: Int, ctx: Any) extends AsyncRequest
case class ExistsRequest(path: String, ctx: Any) extends AsyncRequest
case class GetDataRequest(path: String, ctx: Any) extends AsyncRequest
case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Any) extends AsyncRequest
case class GetACLRequest(path: String, ctx: Any) extends AsyncRequest
case class SetACLRequest(path: String, acl: Seq[ACL], version: Int, ctx: Any) extends AsyncRequest
case class GetChildrenRequest(path: String, ctx: Any) extends AsyncRequest

sealed trait AsyncResponse {
  val rc: Int
  val path: String
  val ctx: Any
}
case class CreateResponse(rc: Int, path: String, ctx: Any, name: String) extends AsyncResponse
case class DeleteResponse(rc: Int, path: String, ctx: Any) extends AsyncResponse
case class ExistsResponse(rc: Int, path: String, ctx: Any, stat: Stat) extends AsyncResponse
case class GetDataResponse(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat) extends AsyncResponse
case class SetDataResponse(rc: Int, path: String, ctx: Any, stat: Stat) extends AsyncResponse
case class GetACLResponse(rc: Int, path: String, ctx: Any, acl: Seq[ACL], stat: Stat) extends AsyncResponse
case class SetACLResponse(rc: Int, path: String, ctx: Any, stat: Stat) extends AsyncResponse
case class GetChildrenResponse(rc: Int, path: String, ctx: Any, children: Seq[String], stat: Stat) extends AsyncResponse

class ZookeeperClientException(message: String) extends RuntimeException(message)
class ZookeeperClientExpiredException(message: String) extends ZookeeperClientException(message)
class ZookeeperClientAuthFailedException(message: String) extends ZookeeperClientException(message)
class ZookeeperClientTimeoutException(message: String) extends ZookeeperClientException(message)