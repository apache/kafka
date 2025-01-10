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

package kafka.zookeeper

import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.collection.Seq

case class ACL() {}
case class CreateMode() {}
case class OpResult() {}
object Code {
  val OK: Integer = 1
  val NONODE: Integer = 1
}
case class Code() {}
case class Stat() {}
case class KeeperException() extends RuntimeException {}

/**
 * A ZooKeeper client that encourages pipelined requests.
 *
 * @param connectString comma separated host:port pairs, each corresponding to a zk server
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param maxInFlightRequests maximum number of unacknowledged requests the client will send before blocking.
 * @param clientConfig ZooKeeper client configuration, for TLS configs if desired
 * @param name name of the client instance
 */
class ZooKeeperClient(connectString: String,
                      sessionTimeoutMs: Int,
                      connectionTimeoutMs: Int,
                      maxInFlightRequests: Int,
                      time: Time,
                      metricGroup: String,
                      metricType: String,
                      name: String) extends Logging {

  this.logIdent = s"[ZooKeeperClient $name] "

  /**
   * Send a request and wait for its response. See handle(Seq[AsyncRequest]) for details.
   *
   * @param request a single request to send and wait on.
   * @return an instance of the response with the specific type (e.g. CreateRequest -> CreateResponse).
   */
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    throw new UnsupportedOperationException()
  }

  /**
   * Send a pipelined sequence of requests and wait for all of their responses.
   *
   * The watch flag on each outgoing request will be set if we've already registered a handler for the
   * path associated with the request.
   *
   * @param requests a sequence of requests to send and wait on.
   * @return the responses for the requests. If all requests have the same type, the responses will have the respective
   * response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
   * will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
   */
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
   * with either a GetDataRequest or ExistsRequest.
   *
   * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
   *
   * @param zNodeChildChangeHandler the handler to register
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = {
    throw new UnsupportedOperationException()
  }

  def close(): Unit = {
    throw new UnsupportedOperationException()
  }

  def sessionId: Long = {
    throw new UnsupportedOperationException()
  }
}

trait StateChangeHandler {
  val name: String
  def beforeInitializingSession(): Unit = {}
  def afterInitializingSession(): Unit = {}
  def onAuthFailure(): Unit = {}
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

// Thin wrapper for zookeeper.Op
sealed trait ZkOp {
}

case class CreateOp(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode) extends ZkOp {
}

case class DeleteOp(path: String, version: Int) extends ZkOp {
}

case class SetDataOp(path: String, data: Array[Byte], version: Int) extends ZkOp {
}

case class CheckOp(path: String, version: Int) extends ZkOp {
}

case class ZkOpResult(zkOp: ZkOp, rawOpResult: OpResult)

sealed trait AsyncRequest {
  /**
   * This type member allows us to define methods that take requests and return responses with the correct types.
   * See ``ZooKeeperClient.handleRequests`` for example.
   */
  type Response <: AsyncResponse
  def path: String
  def ctx: Option[Any]
}

case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode,
                         ctx: Option[Any] = None) extends AsyncRequest {
  type Response = CreateResponse
}

case class DeleteRequest(path: String, version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = DeleteResponse
}

case class ExistsRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = ExistsResponse
}

case class GetDataRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetDataResponse
}

case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetDataResponse
}

case class GetAclRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetAclResponse
}

case class SetAclRequest(path: String, acl: Seq[ACL], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetAclResponse
}

case class GetChildrenRequest(path: String, registerWatch: Boolean, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetChildrenResponse
}

case class MultiRequest(zkOps: Seq[ZkOp], ctx: Option[Any] = None) extends AsyncRequest {
  type Response = MultiResponse

  override def path: String = null
}


sealed abstract class AsyncResponse {
  def resultCode: Code
  def path: String
  def ctx: Option[Any]

  def metadata: ResponseMetadata

  def resultException: Option[RuntimeException] = None
}

case class ResponseMetadata(sendTimeMs: Long, receivedTimeMs: Long) {
  def responseTimeMs: Long = receivedTimeMs - sendTimeMs
}

case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String,
                          metadata: ResponseMetadata) extends AsyncResponse
case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any],
                          metadata: ResponseMetadata) extends AsyncResponse
case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse
case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse
case class GetAclResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class SetAclResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat,
                               metadata: ResponseMetadata) extends AsyncResponse
case class MultiResponse(resultCode: Code, path: String, ctx: Option[Any], zkOpResults: Seq[ZkOpResult],
                         metadata: ResponseMetadata) extends AsyncResponse

case class ZooKeeperClientException(message: String) extends RuntimeException(message)