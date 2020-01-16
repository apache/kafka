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

import java.util.concurrent._
import java.util.concurrent.atomic._

import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable
import com.ibm.disni.verbs.RdmaEventChannel
import com.ibm.disni.verbs.RdmaCm
import com.ibm.disni.verbs.RdmaCmEvent
import com.ibm.disni.verbs.RdmaCmId
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util

import com.ibm.disni.verbs._


/**
  * So far I assume that we meed at most one acceptor and at most one processor.
  */
class RDMAServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, requestChannel: RequestChannel) extends Logging with KafkaMetricsGroup  {
  private val logContext = new LogContext(s"[RDMAServer brokerId=${config.brokerId}] ")
  this.logIdent = logContext.logPrefix

  private var acceptor :RDMAAcceptor  = null
 // private[network] val RDMAPlaneAcceptors = new ConcurrentHashMap[EndPoint, RDMAAcceptor]()
  private val rdmaPlaneProcessors = new ConcurrentHashMap[Int, RdmaProcessor]()

  private var nextProcessorId = 0

  /**
    * Start the RDMA server. Acceptors for all the listeners are started.
    */
  def startup(startupProcessors: Boolean = true) {
    this.synchronized {
      //connectionQuotas = new ConnectionQuotas(config.maxConnectionsPerIp, config.maxConnectionsPerIpOverrides)
      createRDMAAcceptorsAndProcessors(config.numRdmaNetworkThreads, config.RDMAListener)
      if (startupProcessors) {
        startRDMAProcessors()
      }
    }
  }

  def getPD(): Option[IbvPd] = {
    return if(acceptor == null ) None else Some(acceptor.getPD())
  }

  def isWithRdma(): Boolean = return (acceptor != null)

  def getHostName(): String = return if(acceptor == null ) "" else acceptor.getHostName

  def getPort(): Int = return if(acceptor == null ) 0 else acceptor.getPort


  private def addRDMAPlaneProcessors(acceptor: RDMAAcceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = synchronized {

    val rdmaProcessors = new ArrayBuffer[RdmaProcessor]()
    for (_ <- 0 until newProcessorsPerListener) {
      val processor = newProcessor(nextProcessorId,config.maxCompletionQueueSize,acceptor)
      rdmaProcessors += processor
      requestChannel.addRdmaProcessor(processor)
      nextProcessorId += 1
    }
    rdmaProcessors.foreach(p => rdmaPlaneProcessors.put(p.id, p))
    acceptor.addProcessors(rdmaProcessors)
    //info(s"Not implemented : $endpoint")
  }
  // `protected` for test usage
  protected[network] def newProcessor(id: Int,maxQSize: Int,
                                      acceptor: RDMAAcceptor): RdmaProcessor = {
    new RdmaProcessor(id,
      time,maxQSize, config.maxRdmaSendSize, config.maxRdmaRecvSize, acceptor.getContext(),acceptor.getPD(),requestChannel, config.maxRdmaWcBatch
    )
  }

  private def createRDMAAcceptorsAndProcessors(RDMAProcessorsPerListener: Int,
                                               endpointOpt: Option[EndPoint]): Unit = synchronized {
    endpointOpt.foreach { endpoint =>
      val RDMAPlaneAcceptor = createRDMAAcceptor(endpoint, "rdma-plane")
      addRDMAPlaneProcessors(RDMAPlaneAcceptor, endpoint, RDMAProcessorsPerListener)
      KafkaThread.nonDaemon(s"rdma-plane-kafka-rdma-acceptor-${endpoint.listenerName}-${endpoint.port}", RDMAPlaneAcceptor).start()
      RDMAPlaneAcceptor.awaitStartup()
      //RDMAPlaneAcceptors.put(endpoint, RDMAPlaneAcceptor)
      acceptor = RDMAPlaneAcceptor
      info(s"Created rdma-plane acceptor and processors for endpoint : $endpoint")
    }
  }

  private def createRDMAAcceptor(endPoint: EndPoint, metricPrefix: String) : RDMAAcceptor = synchronized {
    //val sendBufferSize = config.socketSendBufferBytes
    //val recvBufferSize = config.socketReceiveBufferBytes
    val brokerId = config.brokerId
    new RDMAAcceptor(config.numIoThreads, endPoint,  brokerId, "rdma-plane")
  }

  /**
    * Starts processors of all the RDMA-plane acceptors of this server if they have not already been started.
    */
  def startRDMAProcessors(): Unit = synchronized {
    if(acceptor != null){
      acceptor.startProcessors()
      info(s"Started rdma-plane processors for a single acceptor")
    }else{
      info(s"No rdma-plane processors")
    }
   // RDMAPlaneAcceptors.values.asScala.foreach { _.startProcessors() }

  }

}



/**
  * A base class with some helper variables and methods
  */
private[kafka] abstract class AbstractRDMAServerThread extends Runnable with Logging {

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


/**
  * Thread that accepts and configures new connections. There is one of these per endpoint.
  */
private[kafka] class RDMAAcceptor(numberOfIoProcessors: Int,
                                   val endPoint: EndPoint,
                              brokerId: Int,
                              metricPrefix: String) extends AbstractRDMAServerThread with KafkaMetricsGroup {

 // private val allEP = ListBuffer[VerbsEP]()
  private val cmChannel = RdmaEventChannel.createEventChannel()
  private val idPriv =  cmChannel.createId(RdmaCm.RDMA_PS_TCP)
  info(s"launching cm processor, cmChannel ${cmChannel.getFd}" )

  val _src = InetAddress.getByName(endPoint.host)
  val src = new InetSocketAddress(_src, endPoint.port)
  idPriv.bindAddr(src)
  idPriv.listen(2);
  private val context = idPriv.getVerbs
  // listen on the id
  private val pd = context.allocPd

  private val port = idPriv.getSource().asInstanceOf[InetSocketAddress].getPort

  private val processors = new ArrayBuffer[RdmaProcessor]()
  private val processorsStarted = new AtomicBoolean


  def getContext(): IbvContext = {
    return context
  }

  def getPD(): IbvPd = {
    return pd
  }

  def getPort(): Int = {
    return port
  }

  def getHostName: String = return _src.getHostAddress
  /**
    * Accept loop that checks for new connection attempts
    */
  def run(): Unit = {


    startupComplete()
    try {
      var currentProcessorIndex = 0
      while (isRunning) {
        try {
          val cmEvent = cmChannel.getCmEvent(500)
          if (cmEvent != null) {
            val eventType = cmEvent.getEvent
            val connId = cmEvent.getConnIdPriv
            cmEvent.ackEvent
            if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST.ordinal())
            {


              val processor = synchronized {
                // adjust the index (if necessary) and retrieve the processor atomically for
                // correct behaviour in case the number of processors is reduced dynamically
                currentProcessorIndex = currentProcessorIndex % processors.length
                processors(currentProcessorIndex)
              }
              currentProcessorIndex += 1
              assignNewConnection(connId, processor)



            }else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal()){
              info(s"RDMA is ready to use ${eventType}")
            }else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()){
              info(s"RDMA is disconnected ${eventType}")
            } else {
              info(s"[ERROR] Unknown RDMA CM event ${eventType}")
            }
          }


        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing RDMA socket and RDMA cm.")
      //CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      //CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private def assignNewConnection(id: RdmaCmId, processor: RdmaProcessor): Boolean = {
    //allEP += ep;
    processor.accept(id)
    true
  }


  /**
    * Wakeup the thread for selection. Not used for RDMA
    */
  @Override
  def wakeup  = {}

  private[network] def addProcessors(newProcessors: Buffer[RdmaProcessor]): Unit = synchronized {
    processors ++= newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors)
  }

  private[network] def startProcessors(): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors)
    }
  }

  private def startProcessors(processors: Seq[RdmaProcessor]): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(s"rdma-kafka-network-thread-$brokerId",processor).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int): Unit = synchronized {

    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.shutdown())

  }

  override def shutdown(): Unit = {
    super.shutdown()
    synchronized {
      processors.foreach(_.shutdown())
    }
  }
}


case class CompletedReceive( status: Int, opcode: Int,  len: Int,  immdata: Int,  qpnum : Int )
case class CompletedSend( status: Int, opcode: Int, qpnum : Int )


private[kafka] class RdmaProcessor(val id: Int,
                               time: Time,
                               maxQSize: Int,
                               maxSendSize: Int,
                               maxRecvSize: Int,
                               context: IbvContext,
                               pd: IbvPd,
                               requestChannel:    RequestChannel,
                               wcBatch:Int, // number of wc we try to poll at a time
                               connectionQueueSize: Int  = 128 ) extends AbstractRDMAServerThread{


  private val sendrecvCompChannel: IbvCompChannel = context.createCompChannel
  private val sendrecvCq: IbvCQ = context.createCQ(sendrecvCompChannel, maxQSize, 0)
  private val reqNotif = sendrecvCq.reqNotification(false)


  private val attr = new IbvQPInitAttr
  attr.cap.setMax_recv_sge(1) // it is ignored in Disni as of July 2019
  attr.cap.setMax_recv_wr(maxRecvSize)
  attr.cap.setMax_send_sge(1) // it is ignored in Disni as of July 2019
  attr.cap.setMax_send_wr(maxSendSize)
  attr.setQp_type(IbvQP.IBV_QPT_RC)
  attr.setRecv_cq(sendrecvCq)
  attr.setSend_cq(sendrecvCq)

  private var existPendingReqNotif = false

  private val wcList = new Array[IbvWC](wcBatch)
  for ( i <- 0 until wcBatch){
    wcList(i) = new IbvWC()
  }

  private val pollCqCall= sendrecvCq.poll(wcList, wcBatch)

  val connParam = new RdmaConnParam
  connParam.setRetry_count(2.toByte)

  private val newConnections = new ArrayBlockingQueue[RdmaCmId](connectionQueueSize)

  protected val qpnumToConnection: util.Map[Integer, BrokerVerbsEPNoQuota] = new util.HashMap[Integer, BrokerVerbsEPNoQuota]


  private val responseQueue = new LinkedBlockingDeque[RequestChannel.RdmaResponse]()

  private val completedReceives = new util.LinkedList[CompletedReceive]()
  private val completedSends = new util.LinkedList[CompletedSend]()


  private val toTrigger = new java.util.LinkedList[BrokerVerbsEPNoQuota]()


  private var pendingRequests:Int = 0

  // ktaranov: here happens all networking.
  override def run() {
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          configureNewConnections()
          // register any new responses for writing
          processNewResponses()
          poll()
          triggerSends()
          processCompletedReceives()
          processCompletedSends()
          processDisconnected()



        } catch {

          case e: Throwable =>  error(s"RdmaProcessor $id run failed", e)
        }
      }
    } finally {
      debug(s"Closing selector - rdma processor $id")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }


  private def processNewResponses() {
    var currentResponse: RequestChannel.RdmaResponse = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {

      try {
        val qpnum: Int = currentResponse.request.qpnum
        val immdata: Int = currentResponse.immdata
        val ep: BrokerVerbsEPNoQuota = qpnumToConnection.get(qpnum)

        ep.postSendWithImm(immdata)
        toTrigger.add(ep)
      } catch {
        case e: Throwable => println(s"Error when tried to send ${e}")

      }
    }
  }

  private def triggerSends(): Unit ={
    if(toTrigger.isEmpty){
      return
    }

    val temp  = new java.util.TreeSet[BrokerVerbsEPNoQuota]()

    toTrigger.forEach{ ep =>
        val hasMore = ep.triggerSend()
        if(hasMore) temp.add(ep)
    }

    toTrigger.clear()
    toTrigger.addAll(temp)
  }



  private def poll() {
    val pollTimeout = if (toTrigger.isEmpty && newConnections.isEmpty && pendingRequests==0) 100 else 0 // todo can be tuned

    if(existPendingReqNotif){
      val newevents = sendrecvCompChannel.getCqEvent(sendrecvCq, pollTimeout)
      if(newevents){
        sendrecvCq.ackEvents(1)
        existPendingReqNotif=false
      }else{
        return
      }
    }

    val res: Int = pollCqCall.execute().getPolls

    if(res < 0){
      error(s"Processor $id poll Cq failed")
      return
    }

    for( i <- 0 until res){
      val wc = wcList(i)

      if(wc.getOpcode >= IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode){
        // here receive completions
        completedReceives.addLast(CompletedReceive(wc.getStatus, wc.getOpcode,wc.getByte_len,wc.getImm_data,wc.getQp_num)  )
      } else{
        // here send completions
        completedSends.addLast(CompletedSend(wc.getStatus,wc.getOpcode,wc.getQp_num))
      }
    }

    if(pollTimeout==0 || res > 0)
      return

    assert(existPendingReqNotif == false)
    reqNotif.execute()
    existPendingReqNotif = true
  }

  private def processCompletedReceives() {
    if(completedReceives.isEmpty)
      return

    val rearm = scala.collection.mutable.Map[Int, Int]()
    while(!completedReceives.isEmpty ){
 //     info(s"have new requests")
      pendingRequests+=1
      val receive = completedReceives.pollFirst()
      val nowNanos = time.nanoseconds()
      val req = new RequestChannel.RdmaRequest(receive.qpnum, processor = id,  startTimeNanos = nowNanos,
        receive.immdata, receive.len, null )
      requestChannel.sendRequest(req);

      rearm += rearm.get(receive.qpnum).map(x => receive.qpnum -> (x + 1)).getOrElse(receive.qpnum -> 1)
    }

    rearm.foreach( {case (qpnum, num) =>
    //  info(s"repost ${num} for ${qpnum}")
      val ep = qpnumToConnection.get(qpnum)
      ep.postRecvs(num)
    })
  }



  private def processCompletedSends() {
    if(completedSends.isEmpty) {
      return
    }
    val rearm = scala.collection.mutable.Map[Int, Int]()
    while(!completedSends.isEmpty ){
      pendingRequests-=1
      val send = completedSends.pollFirst()
      if(send.status != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()){
        println(s"Failed send ${send.status}")
      }
      rearm += rearm.get(send.qpnum).map(x => send.qpnum -> (x + 1)).getOrElse(send.qpnum -> 1)
    }

    rearm.foreach( {case (qpnum, num) =>
      //  info(s"repost ${num} for ${qpnum}")
      val ep = qpnumToConnection.get(qpnum)
      ep.markCompletedSends(num)
    })

  }


  private def processDisconnected() {

  }



  /**
    * Queue up a new connection for reading
    */
  def accept(id: RdmaCmId): Boolean = {
    newConnections.put(id)
    true
  }

  /**
    * Register any new connections that have been queued up. The number of connections processed
    * in each iteration is limited to ensure that traffic and connection close notifications of
    * existing channels are handled promptly.
    */
  private def configureNewConnections() = {

    var connectionsProcessed = 0

    while (!newConnections.isEmpty) {
      val connId = newConnections.poll()
      try {
        debug(s"RdmaProcessor $id listening to new connection from ${connId}")
        val qp: IbvQP = connId.createQP(pd, attr)
        val ep = new BrokerVerbsEPNoQuota(connId,qp,maxRecvSize,maxSendSize) // here request is not supported. it is jsut big number
        connId.accept(connParam)
        connectionsProcessed += 1
        qpnumToConnection.put(ep.getQpNum(), ep)

        ep.postRecvs(maxRecvSize)
      } catch {
       // We explicitly catch all exceptions and close the socket to avoid a socket leak.
       case e: Throwable =>  debug(s"Error RdmaProcessor $id with new connection ${connId}")
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {

  }


  private[network] def enqueueResponse(response: RequestChannel.RdmaResponse): Unit = {
     responseQueue.put(response)
     wakeup()
  }

  private def dequeueResponse(): RequestChannel.RdmaResponse = {
   val response = responseQueue.poll()
  // if (response != null)
  //    response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
   response
  }

  private[network] def responseQueueSize = responseQueue.size




  /**
   * Wakeup the thread for selection.
   */
  override def wakeup() = {
   // disni does not support wakeup on completion event
   // sendrecvCompChannel.wakeup()
  }

  override def shutdown(): Unit = {
    this.closeAll()
    super.shutdown()
  }

}



