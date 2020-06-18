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

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch, LinkedBlockingDeque, TimeUnit}

import kafka.cluster.{BrokerEndPoint, Partition, Replica}
import kafka.log.AddressReplicateInfo
import kafka.metrics.KafkaMetricsGroup
import org.slf4j.event.Level
import kafka.network.{BrokerVerbsEP, RdmaConnector}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{KafkaThread,Time}
import com.ibm.disni.verbs._
import org.apache.kafka.clients.{ClientResponse,  RequestCompletionHandler}
import org.apache.kafka.common.requests.{RDMAProduceAddressRequest, RDMAProduceAddressResponse}
import sun.nio.ch.DirectBuffer

import scala.collection.{Map, mutable}

class LeaderPushManager(brokerConfig: KafkaConfig,
                          protected val replicaManager: ReplicaManager,
                          metrics: Metrics,
                          time: Time,
                          rdmaManager: RDMAManager,
                          threadNamePrefix: Option[String] = None )  extends Logging with KafkaMetricsGroup {

  val numberOfReplicationProcessors =  brokerConfig.numReplicationPushThreads

  private val topicPartitionFollowers = new ConcurrentHashMap[TopicPartition, Set[Int] ]() // Int stands for brokerId


  private val replicationProcessors: Vector[ReplicationProcessor] =
    Vector.fill(numberOfReplicationProcessors)(new ReplicationProcessor(brokerConfig,metrics,time,"client",rdmaManager ))


  replicationProcessors.foreach{ processor =>
    KafkaThread.nonDaemon( s"rdma-kafka-replication-thread-",processor).start()
  }


  def addToReplicate( tp: TopicPartition, logReadResultToInvoke:LogReadResult, fileInfo:AddressReplicateInfo) = {

    val endPoints = topicPartitionFollowers.getOrDefault(tp,Set.empty)

    endPoints.foreach{ brokerId =>
       val id = brokerId.hashCode() % numberOfReplicationProcessors
       replicationProcessors(id).submit(ReplicationEndpoint(tp, brokerId), logReadResultToInvoke, fileInfo)
    }

  //  endPoints.map( _.hashCode() % numberOfReplicationProcessors).toSet.foreach{ id:Int => replicationProcessors(id).wakeup() }
  }



  def addPusherForPartitions(partitionAndEndPoints: Map[Partition, Set[(BrokerEndPoint, Replica)]]) {

    partitionAndEndPoints.foreach{ case (partition, endPoints) =>

      val existingEndpoints = topicPartitionFollowers.computeIfAbsent(partition.topicPartition, (t:TopicPartition) => endPoints.map(_._1.id) )

      println(s"New pusher for parition ${partition.topicPartition} with leader ${partition.leaderReplicaIdOpt.get} and followers ${existingEndpoints}   ")

      endPoints.foreach{ case (endPoint,replica) =>
          val id = endPoint.id.hashCode() % numberOfReplicationProcessors
          replicationProcessors(id).register( partition, replica, endPoint )
      }
    }

  }

  def shutdown() {
    info("shutting LeaderPushManager down")
    synchronized {
      replicationProcessors.foreach {
        _.shutdown()
      }
    }
    info("shutdown LeaderPushManager completed")
  }
}



/**
  * A base class with some helper variables and methods
  */
abstract class AbstractThread extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
    * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
    */
  def shutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
    shutdownLatch.await()
  }

  /**
    * Wait for the thread to completely start up
    */
  def awaitStartup(): Unit = startupLatch.await

  /**
    * Record that the thread startup is complete
    */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
    * Record that the thread shutdown is complete
    */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
    * Is the server still running?
    */
  protected def isRunning: Boolean = alive.get

}



case class CompletionIdentifier(immdata: Int, qpn: Int){
  override def equals(any: Any): Boolean = {
    any match {
      case that: CompletionIdentifier => this.immdata == that.immdata && this.qpn == that.qpn
      case _ => false
    }
  }

  override def hashCode(): Int = {
    return 31 * immdata.hashCode() + qpn.hashCode()
  }
}


case class ReplicationEndpoint(tp: TopicPartition, brokerId: Int){
  override def equals(any: Any): Boolean = {
    any match {
      case that: ReplicationEndpoint => this.tp == that.tp && this.brokerId == that.brokerId
      case _ => false
    }
  }

  override def hashCode(): Int = {
    return 31 * tp.hashCode() + brokerId.hashCode()
  }
}

case class ReplicationRequest(replicationEndpoint: ReplicationEndpoint, logReadResultToInvoke:LogReadResult, fileInfo:AddressReplicateInfo)

case class CompletedReceive( status: Int, opcode: Int,  len: Int,  immdata: Int,  qpnum : Int )
case class CompletedSend( status: Int, opcode: Int, qpnum : Int )


import scala.collection.mutable.HashMap

class ReplicationProcessor( brokerConfig: KafkaConfig,
                            metrics: Metrics,
                            time: Time,
                            clientId: String,
                            manager: RDMAManager
                          ) extends AbstractThread  {

  private val wcBatch: Int = brokerConfig.RdmaPusherWcBatch;
  private val maxQSize:Int = brokerConfig.RdmaPusherCompletionQueueSize;
  private val sendSize:Int =  brokerConfig.RdmaPusherSendSize;
  private val receiveSize:Int = brokerConfig.RdmaPusherReceiveSize;
  private val requestQuota:Int= brokerConfig.RdmaPusherRequestQuota;
  private val maxBatchSize:Int= brokerConfig.RdmaPusherMaxBatchSizeInBytes;
  private val maxReplicationHandlerBatch:Int = brokerConfig.RdmaPusherMaxReplicationHandlerBatch;
  private val retryTimeout:Int =  brokerConfig.RdmaPusherrRetryTimeout;


  private val networkClient = new LeaderPusherNetworkClient(brokerConfig,metrics,time,clientId)
  private val rdmaConnector = new RdmaConnector(manager.rdmaPD.get,maxQSize, sendSize, receiveSize, requestQuota)

  private val newReplicasToRegister = new ArrayBlockingQueue[ (Partition,Replica,BrokerEndPoint) ](100) // pair (id,ep)
  private val requestQueue = new LinkedBlockingDeque[ReplicationRequest]()
  private val replicationHandlers =  new HashMap[ReplicationEndpoint,ReplicationHandler]()
  private val completedReceives = new util.LinkedList[CompletedReceive]()
  private val completedSends = new util.LinkedList[CompletedSend]()
  private val replicationHandlersFromIdentifiers =  new HashMap[CompletionIdentifier,ReplicationHandler]()


  private val toTrigger = new java.util.LinkedList[BrokerVerbsEP]()
  private val completedReplications = new util.LinkedList[CompletionIdentifier]()
  private val rdmaRequests = new HashMap[ReplicationHandler, util.LinkedList[IbvSendWR] ]() // QPN -> List


  protected val qpnumToConnection = new HashMap[Int, BrokerVerbsEP]()
  protected val idToConnection = new HashMap[Int, BrokerVerbsEP]()


  private val sendrecvCq: IbvCQ =  rdmaConnector.getCq

  private val wcList = new Array[IbvWC](wcBatch)
  for ( i <- 0 until wcBatch){
    wcList(i) = new IbvWC()
  }
  private val pollCqCall = sendrecvCq.poll(wcList, wcBatch)

  private var numOfPendingReplications: Int  = 0



  def run = {
    startupComplete()
    try {
      while (isRunning) {
        try {
          configureNewReplications
          processNewReplicationRequests
          sendGetAddressRequests
          createRdmaReplicationRequests
          sendReplicationRequests
          triggerSends
          networkClient.poll()
          pollCq
          processCompletedSends
          processCompletedReceives
          processCompletedReplications



        } catch {
          case e: Throwable =>  error(s"ReplicationProcessor run failed", e)
        }
      }
    } finally {
      debug(s"Closing ReplicationProcessor - rdma processor ")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }


  override def wakeup(): Unit = {}


  // step 1
  private def processNewReplicationRequests() {
    var currentRequest: ReplicationRequest = null
    var counter:Int = this.maxReplicationHandlerBatch // avoid infinite loop

    while ({currentRequest = dequeueReplicationRequest(); currentRequest != null}) {
      counter-=1
      numOfPendingReplications+=1
      replicationHandlers.get(currentRequest.replicationEndpoint).foreach{ handler =>
          handler.submit(currentRequest.logReadResultToInvoke,currentRequest.fileInfo);
      }
      if(counter == 0)
        return
    }

  }

  // step 2 - create rdma requests
  private def sendGetAddressRequests() {
    val addressRequests = new HashMap[Node, (util.List[TopicPartition],util.List[TopicPartition])]

    val timeNow =  time.milliseconds
    replicationHandlers.foreach{ case (replicationEndpoint, handler) =>

      val needToGetAddress: Boolean = handler.needToGetAddress(timeNow)
      val needToUpdateAddress: Boolean = handler.needToUpdateAddress(timeNow)
      if(needToGetAddress || needToUpdateAddress) {

        val node: Node = handler.getNode
      //  println(s"We need to send request to ${node}")
        val twolists = addressRequests.getOrElseUpdate(node,(new util.LinkedList[TopicPartition](), new util.LinkedList[TopicPartition]()) )
        if(needToGetAddress)     twolists._1.add(replicationEndpoint.tp)
        if(needToUpdateAddress)  twolists._1.add(replicationEndpoint.tp)
      }
    }

    addressRequests.foreach { case (node,(newTopicPartitions,rollTopicPartitions)) =>
      val callback = new RequestCompletionHandler() {
        def onComplete(response: ClientResponse) {
          handleAddressResponse(response)
        }
      }
      val requestBuilder = new RDMAProduceAddressRequest.Builder(1, newTopicPartitions, rollTopicPartitions, 1000, true)
      networkClient.sendRequest(node, requestBuilder, callback)
    }
  }

  private def handleAddressResponse(response: ClientResponse): Unit = {
    val lor = response.responseBody.asInstanceOf[RDMAProduceAddressResponse]
    try {
       val brokerId = response.destination.toInt
       if(!idToConnection.contains(brokerId)){
         val ep: BrokerVerbsEP = this.rdmaConnector.connect(lor.rdmaHost, lor.rdmaPort)
         ep.postRecvs(ep.recvSize)
         idToConnection += (brokerId -> ep)
         qpnumToConnection += (ep.getQpNum -> ep)
       }
       lor.responses().forEach{ case (tp,addressInfo ) =>
         replicationHandlers.get(ReplicationEndpoint(tp,brokerId)).foreach{ handler =>
           handler.updateMetadata(addressInfo)
         }
       }

    } catch {
      case e: Exception => println(s"Error on handleAddressResponse ${e}")

    }

  }

  // step 2 - create rdma requests
  private def createRdmaReplicationRequests() {
    replicationHandlers.foreach{ case (_ , handler) =>
      val requestOpt: Option[IbvSendWR] = handler.getRdmaRequest(time)
      requestOpt.foreach{ request =>
        rdmaRequests.getOrElseUpdate(handler, new util.LinkedList[IbvSendWR] ).add(request)
      }
    }
  }



  // step 3 - to free send and receive queues
  private def pollCq() {
    val res: Int = pollCqCall.execute().getPolls

    for (i <- 0 until res) {
      val wc = wcList(i)

      if (wc.getOpcode >= IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode) {
        // here receive completions
        completedReceives.addLast(CompletedReceive(wc.getStatus, wc.getOpcode, wc.getByte_len, wc.getImm_data, wc.getQp_num))
      }else{
        completedSends.addLast(CompletedSend(wc.getStatus, wc.getOpcode,wc.getQp_num))

      }
    }
  }



  // step 4 - send rdma requests
  private def sendReplicationRequests() {

    rdmaRequests.foreach{ case (handler,requests) =>
      idToConnection.get(handler.getBrokerId).foreach{ ep =>

        requests.forEach{ request =>
          val completionIdentifier = CompletionIdentifier(request.getImm_data,ep.getQpNum)
          replicationHandlersFromIdentifiers.getOrElseUpdate(completionIdentifier,handler)
        }
        ep.postRequests(requests)
        toTrigger.add(ep)
      }
    }
    rdmaRequests.clear()
  }



  private def triggerSends(): Unit ={
    if(toTrigger.isEmpty){
      return
    }

    val temp  = new java.util.TreeSet[BrokerVerbsEP]()

    toTrigger.forEach{ ep =>
      val hasMore = ep.triggerSend()
      if(hasMore) temp.add(ep)
    }

    toTrigger.clear()
    toTrigger.addAll(temp)
  }


  // step 4 - process completed receives and rearm receives
  private def processCompletedReceives() {
    if(completedReceives.isEmpty)
      return


    val rearm = scala.collection.mutable.Map[Int, Int]()
    while(!completedReceives.isEmpty){
      //     info(s"have new requests")
      val receive = completedReceives.pollFirst()
     // val nowNanos = time.nanoseconds()
      val completionIdentifier = CompletionIdentifier(receive.immdata,receive.qpnum)
      completedReplications.addLast(completionIdentifier)

      rearm += rearm.get(receive.qpnum).map(x => receive.qpnum -> (x + 1)).getOrElse(receive.qpnum -> 1)
    }

    rearm.foreach( {case (qpnum, num) =>
      val ep = qpnumToConnection.get(qpnum)
      ep.foreach{ _.postRecvs(num)}
      ep.foreach{ _.markCompletedRequests(num)} // we remember completed replications
    })
  }

  // step 4 - process completed receives and rearm receives
  private def processCompletedSends() {
    if(completedSends.isEmpty)
      return


    val rearm = scala.collection.mutable.Map[Int, Int]()
    while(!completedSends.isEmpty){
      //     info(s"have new requests")
      val send = completedSends.pollFirst()
      // val nowNanos = time.nanoseconds()
      if(send.status!= IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()){
        println(s"Failed send ${send.status}")
      }
      rearm += rearm.get(send.qpnum).map(x => send.qpnum -> (x + 1)).getOrElse(send.qpnum -> 1)
    }

    rearm.foreach( {case (qpnum, num) =>
      val ep = qpnumToConnection.get(qpnum)
      ep.foreach{ _.markCompletedSends(num)} // we remember completed sends
    })

  }


  // step 5 - process completed replications
  private def processCompletedReplications() {
    if(completedReplications.isEmpty)
      return

    while(!completedReplications.isEmpty){
      //     info(s"have new requests")
      val completionIdentifier = completedReplications.pollFirst()
      replicationHandlersFromIdentifiers(completionIdentifier).completeOneReplication()
      numOfPendingReplications-=1
      // val nowNanos = time.nanoseconds()
    }
  }


  private def dequeueReplicationRequest():  ReplicationRequest = {
    val timeout = if(numOfPendingReplications == 0) 1000 else 0
    val request = requestQueue.poll(timeout,TimeUnit.MILLISECONDS)
    request
  }


  private def configureNewReplications() = {

    while (!newReplicasToRegister.isEmpty) {
      val (partition,replica,brokerEndPoint) = newReplicasToRegister.poll()
      replicationHandlers += (ReplicationEndpoint(partition.topicPartition,brokerEndPoint.id) ->
          new ReplicationHandler(partition,replica, new Node(brokerEndPoint.id,brokerEndPoint.host,brokerEndPoint.port),
            partition.localReplicaOrException.logEndOffset,manager,maxBatchSize,retryTimeout) )

      debug(s"configureNewReplications to new connection from ${replica}")
    }

  }

  def register( partition:Partition, replica: Replica, endPoint: BrokerEndPoint) = {
    newReplicasToRegister.add( (partition,replica,endPoint) )
    //println(newReplicasToRegister)
  }

  def submit(replicationEndpoint: ReplicationEndpoint,logReadResultToInvoke:LogReadResult, fileInfo:AddressReplicateInfo) = {
    requestQueue.addLast( ReplicationRequest(replicationEndpoint,logReadResultToInvoke,fileInfo)  )
  }

  /**
    * Close the selector and all open connections
    */
  private def closeAll() {

  }

}


case class toReplicateFullRequest(info: AddressReplicateInfo ,result: LogReadResult) extends Ordered[toReplicateFullRequest]{
  override def compare(that: toReplicateFullRequest): Int = this.info.offset.compareTo(that.info.offset)
}


class ReplicationHandler(partition: Partition, replica:Replica, val node: Node, offset: Long, manager: RDMAManager, maxBatchSize: Int, retryTimeout: Int ){

  val inflightRequests = new util.LinkedList[LogReadResult]()

  val topicPartition: TopicPartition = partition.topicPartition

  val toReplicate = new java.util.TreeSet[toReplicateFullRequest]()

  private var lastUpdateRequested = 0L
  private var lastNewInfoRequested = 0L

  private var currentOffset = offset
  private var currentAddress = -1L
  private var lastAddress = -1L

  private var baseOffset = -1L
  private var rkey = -1
  private var immdata = -1

  private var needNewFile = false


  private var cachedBaseAddr:Long = -1
  private var cachedLkey:Int = -1


  // rdma patch

  def getNode: Node = {
    node
  }


  def getBrokerId(): Int = {
    node.id
  }


  def needToUpdateAddress(nowMs: Long): Boolean = {
    if(needNewFile && nowMs - lastUpdateRequested > retryTimeout){
      lastUpdateRequested = nowMs
      return needNewFile
    }
    false
  }

  def needToGetAddress(nowMs: Long): Boolean = {
    if(nowMs - lastNewInfoRequested > retryTimeout){
      lastNewInfoRequested = nowMs
      return currentAddress == (-1L)
    }
    false
  }


  def submit(logReadResultToInvoke:LogReadResult, fileInfo:AddressReplicateInfo):Unit = {
    toReplicate.add(toReplicateFullRequest(fileInfo,logReadResultToInvoke))
  }

  def updateMetadata(response: RDMAProduceAddressResponse.PartitionResponse): Unit ={
    if (response.baseOffset == this.baseOffset) {
      println("Received the same metadata twice")
      return
    }
    this.baseOffset=response.baseOffset
    this.currentAddress = response.address
    this.immdata = response.immdata
    this.rkey = response.rkey
    this.lastAddress = response.address + response.length
    this.needNewFile = false
  }


  /*
  It batches the entries if it can
  */
  def getRdmaRequest(time: Time):Option[IbvSendWR]={



    if(needNewFile || currentAddress == (-1L) || toReplicate.isEmpty)
      return None


    val firstElem = toReplicate.first()
    if(firstElem.info.offset != currentOffset){


      return None
    }
    var fulllength:Int = (firstElem.info.endPosition - firstElem.info.startPosition).toInt

    if(currentAddress+fulllength > lastAddress){
      needNewFile = true
      return None
    }

    val sgeSend = new IbvSge
    val firstBaseAddr: Long = firstElem.info.bytebuffer.asInstanceOf[DirectBuffer].address

    sgeSend.setAddr(firstBaseAddr+firstElem.info.startPosition)


    val sendWR: IbvSendWR = new IbvSendWR
    val sgeList = new util.LinkedList[IbvSge]
    sgeList.add(sgeSend)
    sendWR.setSg_list(sgeList)
    sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM)
    sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED)
    sendWR.setImm_data(immdata)

    val rdmapart = sendWR.getRdma
    rdmapart.setRemote_addr(currentAddress)
    rdmapart.setRkey(rkey)

    val startTime = time.nanoseconds()
    if(firstBaseAddr!=cachedBaseAddr){
      val mr = manager.getIbvBuffer(firstElem.info.bytebuffer) // it can register memory as well if it is not registered
      cachedBaseAddr = firstBaseAddr
      cachedLkey = mr.getLkey
    }
    sgeSend.setLkey(cachedLkey)


    currentOffset = firstElem.result.leaderLogEndOffset
    currentAddress += fulllength
    var toInvoke = firstElem.result
    toReplicate.pollFirst()

    // Check whether we can batch multiple rdma writes
    while( !toReplicate.isEmpty && (fulllength < this.maxBatchSize)  ){ // 5 microseconds && (startTime + 5000 > time.nanoseconds())
      val elem = toReplicate.first()
      val length:Int = (elem.info.endPosition - elem.info.startPosition).toInt
      val baseAddr: Long = elem.info.bytebuffer.asInstanceOf[DirectBuffer].address

      if(elem.info.offset != currentOffset ||
        currentAddress+length > lastAddress ||
        firstBaseAddr != baseAddr)
      {
        sgeSend.setLength(fulllength)
        inflightRequests.addLast(toInvoke)
        return Some(sendWR)
      }

      currentOffset = elem.result.leaderLogEndOffset
      toInvoke = elem.result
      fulllength+=length
      currentAddress+=length
      toReplicate.pollFirst()
    }

    sgeSend.setLength(fulllength)
    inflightRequests.addLast(toInvoke)
    return Some(sendWR)
  }

  def completeOneReplication() ={
    val logReadRes = inflightRequests.pollFirst()
    partition.updateReplicaLogReadResult(replica,logReadRes)
  }


  override def equals(any: Any): Boolean = {
    any match {
      case that: ReplicationHandler => this.topicPartition == that.topicPartition && this.node.id() == that.node.id()
      case _ => false
    }
  }

  override def hashCode(): Int = {
    return 31 * topicPartition.hashCode() + node.id().hashCode()
  }
}



