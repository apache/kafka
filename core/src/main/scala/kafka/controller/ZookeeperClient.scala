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

import scala.collection.JavaConverters._
import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.Logging
import org.apache.zookeeper.AsyncCallback.{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher, ZooKeeper}

/**
 * ZookeeperClient is a zookeeper client that encourages pipelined requests to zookeeper.
 *
 * @param connectString comma separated host:port pairs, each corresponding to a zk server
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param stateChangeHandler state change handler callbacks called by the underlying zookeeper client's EventThread.
 */
class ZookeeperClient(connectString: String, sessionTimeoutMs: Int, connectionTimeoutMs: Int,
                      stateChangeHandler: StateChangeHandler) extends Logging {
  this.logIdent = "[ZookeeperClient] "
  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala

  info(s"Initializing a new session to $connectString.")
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZookeeperClientWatcher)
  waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)

  /**
   * Take an AsyncRequest and wait for its AsyncResponse. See handle(Seq[AsyncRequest]) for details.
   *
   * @param request a single AsyncRequest to wait on.
   * @return the request's AsyncResponse.
   */
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    handleRequests(Seq(request)).head
  }

  /**
   * Pipeline a sequence of AsyncRequests and wait for all of their AsyncResponses.
   *
   * The watch flag on each outgoing request will be set if we've already registered a handler for the
   * path associated with the AsyncRequest.
   *
   * @param requests a sequence of AsyncRequests to wait on.
   * @return the AsyncResponses.
   */
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = inReadLock(initializationLock) {
    import scala.collection.JavaConverters._
    if (requests.isEmpty) {
      return Seq.empty
    }
    val countDownLatch = new CountDownLatch(requests.size)
    val responseQueue = new ArrayBlockingQueue[Req#Response](requests.size)

    requests.foreach { request =>
      val processedRequest: Req = request match {
        case r: WatchableAsyncRequest if r.shouldWatch.isEmpty =>
          val shouldWatch = r match {
            case _: GetChildrenRequest => zNodeChildChangeHandlers.contains(r.path)
            case _: ExistsRequest | _: GetDataRequest => zNodeChangeHandlers.contains(r.path)
          }
          r.withWatch(Some(shouldWatch)).asInstanceOf[Req]
        case r => r
      }
      processedRequest.send(zooKeeper) { response =>
        responseQueue.add(response)
        countDownLatch.countDown()
      }
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

  /**
   * Register the handler to ZookeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
   * with either a GetDataRequest or ExistsRequest.
   *
   * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put(zNodeChangeHandler.path, zNodeChangeHandler)
  }

  /**
   * Unregister the handler from ZookeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  /**
   * Register the handler to ZookeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
   *
   * @param zNodeChildChangeHandler the handler to register
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path, zNodeChildChangeHandler)
  }

  /**
   * Unregister the handler from ZookeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
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

  def sessionId: Long = inReadLock(initializationLock) {
    zooKeeper.getSessionId
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
      stateChangeHandler.onReconnectionTimeout()
    }
  }

  private object ZookeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      debug("Received event: " + event)
      Option(event.getPath) match {
        case None =>
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }
          if (event.getState == KeeperState.AuthFailed) {
            info("Auth failed.")
            stateChangeHandler.onAuthFailure()
          } else if (event.getState == KeeperState.Expired) {
            inWriteLock(initializationLock) {
              info("Session expired.")
              stateChangeHandler.beforeInitializingSession()
              initialize()
              stateChangeHandler.afterInitializingSession()
            }
          }
        case Some(path) =>
          (event.getType: @unchecked) match {
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }
}

trait StateChangeHandler {
  def beforeInitializingSession(): Unit = {}
  def afterInitializingSession(): Unit = {}
  def onAuthFailure(): Unit = {}
  def onReconnectionTimeout(): Unit = {}
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation(): Unit = {}
  def handleDeletion(): Unit = {}
  def handleDataChange(): Unit = {}
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange(): Unit = {}
}

sealed trait AsyncRequest {
  type Response <: AsyncResponse
  def path: String
  def ctx: Option[Any]
  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit)
}

sealed trait WatchableAsyncRequest extends AsyncRequest {
  def shouldWatch: Option[Boolean]
  def withWatch(b: Option[Boolean]): WatchableAsyncRequest
}

case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = CreateResponse

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.create(path, data, acl.asJava, createMode, new StringCallback {
      override def processResult(rc: Int, path: String, ctx: Any, name: String): Unit =
        processResponse(CreateResponse(Code.get(rc), path, Option(ctx), name))
    }, ctx.orNull)
  }
}

case class DeleteRequest(path: String, version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = DeleteResponse

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.delete(path, version, new VoidCallback {
      override def processResult(rc: Int, path: String, ctx: Any): Unit =
        processResponse(DeleteResponse(Code.get(rc), path, Option(ctx)))
    }, ctx.orNull)
  }
}

case class ExistsRequest(path: String, shouldWatch: Option[Boolean] = None, ctx: Option[Any] = None) extends WatchableAsyncRequest {
  type Response = ExistsResponse

  def withWatch(b: Option[Boolean]): ExistsRequest = copy(shouldWatch = b)

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.exists(path, shouldWatch.getOrElse(false), new StatCallback {
      override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) =
        processResponse(ExistsResponse(Code.get(rc), path, Option(ctx), stat))
    }, ctx.orNull)
  }
}

case class GetDataRequest(path: String, shouldWatch: Option[Boolean] = None, ctx: Option[Any] = None) extends WatchableAsyncRequest {
  type Response = GetDataResponse

  def withWatch(b: Option[Boolean]): GetDataRequest = copy(shouldWatch = b)

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.getData(path, shouldWatch.getOrElse(false), new DataCallback {
      override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat) =
        processResponse(GetDataResponse(Code.get(rc), path, Option(ctx), data, stat))
    }, ctx.orNull)
  }
}

case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetDataResponse

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.setData(path, data, version, new StatCallback {
      override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) =
        processResponse(SetDataResponse(Code.get(rc), path, Option(ctx), stat))
    }, ctx.orNull)
  }
}

case class GetACLRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetACLResponse

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.getACL(path, null, new ACLCallback {
      override def processResult(rc: Int, path: String, ctx: Any, acl: java.util.List[ACL], stat: Stat): Unit = {
        processResponse(GetACLResponse(Code.get(rc), path, Option(ctx), Option(acl).map(_.asScala).getOrElse(Seq.empty),
          stat))
      }}, ctx.orNull)
  }
}

case class SetACLRequest(path: String, acl: Seq[ACL], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetACLResponse

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.setACL(path, acl.asJava, version, new StatCallback {
      override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) =
        processResponse(SetACLResponse(Code.get(rc), path, Option(ctx), stat))
      }, ctx.orNull)
  }
}

case class GetChildrenRequest(path: String, shouldWatch: Option[Boolean] = None, ctx: Option[Any] = None) extends WatchableAsyncRequest {
  type Response = GetChildrenResponse

  def withWatch(b: Option[Boolean]): GetChildrenRequest = copy(shouldWatch = b)

  def send(zooKeeper: ZooKeeper)(processResponse: Response => Unit): Unit = {
    zooKeeper.getChildren(path, shouldWatch.getOrElse(false), new Children2Callback {
      override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat) =
        processResponse(GetChildrenResponse(Code.get(rc), path, Option(ctx),
          Option(children).map(_.asScala).getOrElse(Seq.empty), stat))
    }, ctx.orNull)
  }
}

sealed trait AsyncResponse {
  val resultCode: Code
  val path: String
  val ctx: Option[Any]
  def resultException: Option[KeeperException] =
    if (resultCode == Code.OK) None else Some(KeeperException.create(resultCode, path))
}
case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String) extends AsyncResponse
case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any]) extends AsyncResponse
case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat) extends AsyncResponse
case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat) extends AsyncResponse
case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat) extends AsyncResponse
case class GetACLResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat) extends AsyncResponse
case class SetACLResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat) extends AsyncResponse
case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat) extends AsyncResponse

class ZookeeperClientException(message: String) extends RuntimeException(message)
class ZookeeperClientExpiredException(message: String) extends ZookeeperClientException(message)
class ZookeeperClientAuthFailedException(message: String) extends ZookeeperClientException(message)
class ZookeeperClientTimeoutException(message: String) extends ZookeeperClientException(message)
