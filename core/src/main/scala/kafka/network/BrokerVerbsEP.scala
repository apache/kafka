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


import com.ibm.disni.verbs._
import java.util.LinkedList
import java.util


// todo it makes sense to count the outstanding requests to avoid send Q exhaustion
class BrokerVerbsEP(connId:RdmaCmId, qp: IbvQP, val recvSize: Int, val maxSend: Int, val requestQuota: Int ) extends Ordered[BrokerVerbsEP]{

  override def compare(that: BrokerVerbsEP): Int = this.my_qpnum.compareTo(that.my_qpnum)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: BrokerVerbsEP => this.my_qpnum == that.my_qpnum
      case _ => false
    }
  }

  private val my_qpnum = qp.getQp_num()

  val wrList_recv = new LinkedList[IbvRecvWR]()
  val recvWR = new IbvRecvWR
  recvWR.setWr_id(my_qpnum)
  wrList_recv.add(recvWR)
  val postercv = qp.postRecv(wrList_recv, null)

  private var canIssueRdma: Int = maxSend
  private var canSendRequests: Int = requestQuota


  var delayedWrites: util.LinkedList[IbvSendWR] = new LinkedList[IbvSendWR]()

  def getQpNum(): Int = my_qpnum

  def markCompletedSends(num:Int) = {
    canIssueRdma +=num
  }

  def markCompletedRequests(num:Int) = {
    canSendRequests +=num
  }

  def postRecvs(num: Int): Unit = {
    for( _ <- 0 until num){
      postercv.execute()
    }
  }

  /*
  It returns true if there are delayed request left
  true -> hasMore
  false -> nothing to send
  */
  def triggerSend(): Boolean = {
    if (delayedWrites.isEmpty) {
      return false
    }

    val canSendNow = scala.math.min(canIssueRdma, canSendRequests)
    if (canSendNow == 0) {
      return true
    }

    val toSendNow = new LinkedList[IbvSendWR]()
    val willSendNow = scala.math.min(canSendNow, delayedWrites.size())

    for (i <- 0 until willSendNow) {
      toSendNow.addLast(delayedWrites.pollFirst())
    }

    canIssueRdma-=willSendNow
    canSendRequests-=willSendNow

    qp.postSend(toSendNow,null).execute().free()

    return !delayedWrites.isEmpty
  }


  def postRequests(wrs: util.List[IbvSendWR]): Unit = {
    delayedWrites.addAll(wrs)
  }

  def postSendWithImm(immdata: Int): Unit = {
    val sendWR: IbvSendWR = new IbvSendWR
    sendWR.setWr_id(666);
    sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM) // also can IbvSendWR.IBV_WR_SEND_WITH_IMM
    sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED)
    sendWR.setImm_data(immdata)
    delayedWrites.add(sendWR)
  }

}

// todo it makes sense to count the outstanding requests to avoid send Q exhaustion
class BrokerVerbsEPNoQuota(connId:RdmaCmId, qp: IbvQP, val recvSize: Int, val maxSend: Int) extends Ordered[BrokerVerbsEPNoQuota]{

  override def compare(that: BrokerVerbsEPNoQuota): Int = this.my_qpnum.compareTo(that.my_qpnum)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: BrokerVerbsEPNoQuota => this.my_qpnum == that.my_qpnum
      case _ => false
    }
  }

  private val my_qpnum = qp.getQp_num()

  val wrList_recv = new LinkedList[IbvRecvWR]()
  val recvWR = new IbvRecvWR
  recvWR.setWr_id(my_qpnum)
  wrList_recv.add(recvWR)
  val postercv = qp.postRecv(wrList_recv, null)

  private var canIssueRdma: Int = maxSend

  var delayedWrites: util.LinkedList[IbvSendWR] = new LinkedList[IbvSendWR]()

  def getQpNum(): Int = my_qpnum

  def markCompletedSends(num:Int) = {
    canIssueRdma +=num
  }

  def postRecvs(num: Int): Unit = {
    for( _ <- 0 until num){
      postercv.execute()
    }
  }

  /*
  It returns true if there are delayed request left
  true -> hasMore
  false -> nothing to send
  */
  def triggerSend(): Boolean = {
    if (delayedWrites.isEmpty) {
      return false
    }

    if (canIssueRdma == 0) {
      return true
    }

    val toSendNow = new LinkedList[IbvSendWR]()
    val willSendNow = scala.math.min(canIssueRdma, delayedWrites.size())

    for (i <- 0 until willSendNow) {
      toSendNow.addLast(delayedWrites.pollFirst())
    }

    canIssueRdma-=willSendNow

    qp.postSend(toSendNow,null).execute().free()

    return !delayedWrites.isEmpty
  }


  def postRequests(wrs: util.List[IbvSendWR]): Unit = {
    delayedWrites.addAll(wrs)
  }

  def postSendWithImm(immdata: Int): Unit = {
    val sendWR: IbvSendWR = new IbvSendWR
    sendWR.setWr_id(666);
    sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM) // also can IbvSendWR.IBV_WR_SEND_WITH_IMM
    sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED)
    sendWR.setImm_data(immdata)
    delayedWrites.add(sendWR)
  }

}

