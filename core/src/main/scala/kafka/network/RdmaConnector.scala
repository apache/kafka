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
package kafka.network

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress}


import com.ibm.disni.verbs._

// we count the outstanding requests to avoid send Q exhaustion
// it uses one completion Q for all connections
class RdmaConnector(pd:IbvPd,  completion_queue_size: Int, sendSize: Int, receiveSize: Int, requestQuota:Int ) {

  val context: IbvContext = pd.getContext
  val cmChannel = RdmaEventChannel.createEventChannel

  // the comp channel is used for getting CQ events
  val sendrecvcompChannel = context.createCompChannel
  if (sendrecvcompChannel == null) throw new IOException("RdmaConnector::compChannel null")
  // let's create a completion queue
  val sendrecvcq = context.createCQ(sendrecvcompChannel, completion_queue_size, 0)
  if (sendrecvcq == null) throw new IOException("RdmaConnector::cq null")


  def getCq:IbvCQ={
    sendrecvcq
  }

  def getCqChannel:IbvCompChannel = {
    sendrecvcompChannel
  }

  @throws[Exception]
  private def PrepareConnection(hostname: String, port: Int): RdmaCmId = {
    val idPriv = cmChannel.createId(RdmaCm.RDMA_PS_TCP)
    if (idPriv == null) throw new IOException("VerbsClient::id null")
    // before connecting, we have to resolve addresses
    val _dst = InetAddress.getByName(hostname)
    val dst = new InetSocketAddress(_dst, port)
    idPriv.resolveAddr(null, dst, 2000)
    // resolve addr returns an event, we have to catch that event
    var cmEvent = cmChannel.getCmEvent(-1)
    if (cmEvent == null) throw new IOException("VerbsClient::cmEvent null")
    else if (cmEvent.getEvent != RdmaCmEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED.ordinal) throw new IOException("VerbsClient::wrong event received: " + cmEvent.getEvent)
    cmEvent.ackEvent()
    // we also have to resolve the route
    idPriv.resolveRoute(2000)
    // and catch that event too
    cmEvent = cmChannel.getCmEvent(-1)
    if (cmEvent == null) throw new IOException("VerbsClient::cmEvent null")
    else if (cmEvent.getEvent != RdmaCmEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED.ordinal) throw new IOException("VerbsClient::wrong event received: " + cmEvent.getEvent)
    cmEvent.ackEvent()
    idPriv
  }

  @throws[Exception]
  private def FinalizeConnection(idPriv: RdmaCmId): BrokerVerbsEP = {
    val attr = new IbvQPInitAttr
    attr.cap.setMax_recv_sge(1)
    attr.cap.setMax_recv_wr(receiveSize)
    attr.cap.setMax_send_sge(1)
    attr.cap.setMax_send_wr(sendSize)
    attr.setQp_type(IbvQP.IBV_QPT_RC)
    attr.setRecv_cq(sendrecvcq)
    attr.setSend_cq(sendrecvcq)

    // let's create a queue pair
    val qp = idPriv.createQP(pd, attr)
    if (qp == null) throw new IOException("VerbsClient::qp null")
    // now let's connect to the server
    val connParam = new RdmaConnParam
    connParam.setRetry_count(2.toByte)
    idPriv.connect(connParam)
    // wait until we are really connected
    val cmEvent = cmChannel.getCmEvent(-1)
    if (cmEvent == null) throw new IOException("VerbsClient::cmEvent null")
    else if (cmEvent.getEvent != RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal) throw new IOException("VerbsClient::wrong event received: " + cmEvent.getEvent)
    cmEvent.ackEvent()
    System.out.println("Connected Replication rdma")
    new BrokerVerbsEP(idPriv,qp,receiveSize, sendSize,requestQuota)
  }

  def connect(hostname: String, port: Int): BrokerVerbsEP ={

    val idPriv = PrepareConnection(hostname, port)
    return FinalizeConnection(idPriv)
  }
}
