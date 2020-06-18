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

import java.util.concurrent.atomic.AtomicLong

import kafka.network._
import kafka.utils._
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.ibm.disni.verbs.{IbvMr, IbvPd}
import org.apache.kafka.common.TopicPartition
import sun.nio.ch.DirectBuffer

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer



case class RegisteredSegment(tp: TopicPartition, baseOffset: Long){
  override def equals(any: Any): Boolean = {
    any match {
      case that: RegisteredSegment => this.tp == that.tp && this.baseOffset == that.baseOffset
      case _ => false
    }
  }

  override def hashCode(): Int = {
    return 31 * tp.hashCode() + baseOffset.hashCode()
  }

}



// The class must be thread-safe! Probably can be optimized. Right now all methods are synchronized!
/**
 * A thread that answers kafka requests.
  *
  *
  * todo add timer to elements
 */
class RDMAManager(brokerId: Int, rdmaServer: RDMAServer, maxNumberOfConsumers: Int, segmentsPerConsumer: Int) extends Logging {
  this.logIdent = "[RDMAManager on Broker " + brokerId + "], "
  //private val shutdownComplete = new CountDownLatch(1)
  //private val ibvBuffers: ConcurrentNavigableMap[java.lang.Long, IbvMr] = new ConcurrentSkipListMap[java.lang.Long, IbvMr]()
  private val ibvBuffers =  new ConcurrentHashMap[java.lang.Long, IbvMr]()
  val rdmaPD: Option[IbvPd] = rdmaServer.getPD()

  private val consumerBuffer: ByteBuffer = ByteBuffer.allocateDirect(maxNumberOfConsumers*segmentsPerConsumer*SlotSize.get)
  val access: Int = IbvMr.IBV_ACCESS_REMOTE_READ | IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE
  private val ibvConsumerBuffer: IbvMr = rdmaPD.map( pd => pd.regMr(consumerBuffer,access).execute().free().getMr()).getOrElse(null)

  /* ClientId ->  Map [ Segment -> slotId ]*/
  private val mapClientToSlotId =  new HashMap[String, HashMap[RegisteredSegment,Int]]()
  /* ClientId -> ClientRow*/
  private val mapClientIdToRow =  new HashMap[String, Int]()
  /* Segment -> Slot*/
  private val mapSegmentToSlot =  new HashMap[RegisteredSegment , ListBuffer[Slot]]()


  // it is returned if the slot is requested for sealed file
  val fakeSlot: FakeSlot = new FakeSlot


  def isWithRdma(): Boolean = rdmaServer.isWithRdma

  def getHostName: String = rdmaServer.getHostName

  def getPort: Int = rdmaServer.getPort


  def getConsumerRkey: Int = {
      return ibvConsumerBuffer.getRkey
  }

  def getSlot(tp: TopicPartition, baseOffset: Long ) : ListBuffer[Slot]  = this.synchronized {
    // A single TopicPartition can be registered multiple times by different clients
    val toFind = RegisteredSegment(tp,baseOffset)

    val slotOpt = mapSegmentToSlot.get(toFind)
    return slotOpt match {
      case Some(slots) => slots
      case None => new ListBuffer[Slot]()
    }

  }

  // if it slot does not exist then return None. If it does not we create it and return
  def createNewSlotOrNothing(clientId: String,  tp: TopicPartition, baseOffset: Long) : Option[Slot] = this.synchronized{
    val toFind = RegisteredSegment(tp,baseOffset)

    if (mapSegmentToSlot.contains(toFind)) {

      val optSlot = mapSegmentToSlot(toFind).find( p => p.clientId == clientId)
      optSlot match {
        case Some(slot) => return None
        case None => // continue
      }
    }
    // Slot has not been found. Create it
    Some(doCreateSlot(clientId,tp,baseOffset))
  }

  def removeSlot(clientId: String,  tp: TopicPartition, baseOffset: Long): Unit = this.synchronized {
    debug("remove slot")
    val toFind = RegisteredSegment(tp,baseOffset)
    // TODO must remove only one Slot, now it removes everything
    mapSegmentToSlot.remove(toFind)
    mapClientToSlotId.get(clientId).foreach( slotMap =>
      slotMap.remove(toFind).foreach({ slotId: Int =>
        val toRemove = mapSegmentToSlot.get(toFind).map(
          _.filter( slot => slot.slotId == slotId &&  slot.clientId.equals(clientId) )
        ).getOrElse(new ListBuffer[Slot] )
        toRemove.foreach( _.makeInvalid)

        mapSegmentToSlot.get(toFind).map( _--= toRemove)
      }
      )
    )
  }

  def getOptSlot(clientId: String,  tp: TopicPartition, baseOffset: Long) : Option[Slot] = this.synchronized{
    val toFind = RegisteredSegment(tp,baseOffset)
    return mapSegmentToSlot.get(toFind) match {
      case None => None
      case Some(value) => value.find( p => p.clientId == clientId)
    }
  }


  def doCreateSlot(clientId: String,  tp: TopicPartition, baseOffset: Long) : Slot = this.synchronized{
    val toFind = RegisteredSegment(tp,baseOffset)

    val segToSlotId   = mapClientToSlotId.get(clientId) match {
        case Some(segToSlotId) => segToSlotId
        case None => {
            // new client
            val v = new HashMap[RegisteredSegment,Int]()
            mapClientToSlotId.put(clientId,v)

            val existingVals = mapClientIdToRow.values.toSet
            val possibleVals = 0 until maxNumberOfConsumers toSet
            val diff = possibleVals.diff(existingVals)

            if(diff.isEmpty)
              // todo remove clients with zero slots
              throw new IllegalStateException("No slots are available 1")
            else {
              mapClientIdToRow.put(clientId, diff.min)
            }

            v
        }
    }


    val existingVals = segToSlotId.values.toSet
    val possibleVals = 0 until segmentsPerConsumer toSet
    val diff = possibleVals.diff(existingVals)

    val slotId =   if(diff.isEmpty)
      throw new IllegalStateException("No slots are available 2")
    else {
      val assignedSlot = diff.min
      segToSlotId.put(toFind, assignedSlot)
      assignedSlot
    }

    val clientRowId = mapClientIdToRow(clientId)

    val offsetInBuffer: Int = clientRowId*(segmentsPerConsumer*SlotSize.get) + slotId*SlotSize.get
    val slotBuf:ByteBuffer = consumerBuffer.duplicate().position(offsetInBuffer).limit(offsetInBuffer+SlotSize.get).asInstanceOf[ByteBuffer].slice()
    val slot = new Slot(slotBuf,clientId,slotId,baseOffset)


    val list = mapSegmentToSlot.getOrElseUpdate(toFind,new ListBuffer[Slot]())
    list += slot

    return slot
  }


  /* Optimized memory registration. We register only if the region is not registered yet. */
  def getIbvBuffer(buffer: ByteBuffer): IbvMr  = {
    buffer.position(0)
    val address = buffer.asInstanceOf[DirectBuffer].address()

    val mr = ibvBuffers.computeIfAbsent(address, (k: java.lang.Long) => {
        val length = buffer.limit()
        val access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ
        val mr: IbvMr = rdmaPD.map(pd => pd.regMr(address, length, access).execute().free().getMr()).getOrElse(null)
        mr
      }
    )
    mr
  }
}


class ProducerRdmaManager{

  /* ConsumerId -> Segment*/
  private val mapProducerIdToSegment =  new HashMap[Int,ProducerSegment]()

  def GetSegmentFromProducerId(imm_data: Int): ProducerSegment = synchronized{
    return mapProducerIdToSegment(imm_data)
  }

  def createProducerIdForSegment(tp: TopicPartition, baseOffset: Long,
                                 position: Long, clientId: String,
                                 buffer: ByteBuffer,  request: RequestChannel.Request,
                                 acks: Short,  timeout: Long, isFromLeader: Boolean): Int = synchronized {


    val found = mapProducerIdToSegment.find( {case (id,segment) => segment.baseOffset == baseOffset && segment.tp == tp } )
    if (found.isDefined) {
      // entry already exists
      return found.get._1;
    }

    // remove old imm_datas for old files belonging to the current TopicPartition.
    mapProducerIdToSegment.retain( {case (id,segment) => (segment.tp != tp) } )

    // otherwise add it
    val target = new ProducerSegment(tp,baseOffset,new AtomicLong(position),clientId,buffer,request,acks,timeout,isFromLeader)
    val existingVals = mapProducerIdToSegment.keySet

    // todo: replace with the first available ID
    val consumerImmId: Int =
      if (existingVals.isEmpty) {
        1
      }else{
        existingVals.max + 1
      }

    mapProducerIdToSegment.put(consumerImmId,target)
    return consumerImmId
  }

}

object SlotSize {
  def get: Int = 5*8
}

class ProducerSegment (val tp: TopicPartition, val baseOffset: Long,
                       val position: AtomicLong, val clientId: String,
                       val buffer: ByteBuffer, val request: RequestChannel.Request,
                       val acks: Short, val timeout: Long, val isFromLeader:Boolean
                      )
{

  override def equals(any: Any): Boolean = {
    any match {
      case that: ProducerSegment => this.tp == that.tp && this.baseOffset == that.baseOffset
      case _ => false
    }
  }

  override def hashCode(): Int = {
    return 31 * tp.hashCode() + baseOffset.hashCode()
  }

}



class FakeSlot( ) extends Slot(null,"",-1,-1L) {

  override def getAddress:Long = 0L
  override def setWrittenPosition(position: Long): Unit  = {}
  override def isSealed: Boolean = true
  override def getWrittenPosition:Long = 0L
  override def getWatermarkPosition:Long = 0L
  override def getLSOOffset:Long = 0L

}



/*
  * | WritePos and Sealed   | Watermark position | LSO position  | Watermark offset | LSO offset    |
  * |   8 bytes             |  8 bytes           | 8 Bytes       | 8 bytes          | 8 Bytes       |
 */
class Slot(val buffer: ByteBuffer,val clientId: String, val slotId: Int, val baseOffset: Long)   {


  @volatile var valid: Boolean = true

  // TODO probably protect with lock
  def makeInvalid(): Unit = {
    valid = false
  }

  def getAddress( ): Long = {
    return buffer.asInstanceOf[DirectBuffer].address()
  }

  def setWrittenPosition(position: Long): Unit = {
    buffer.asLongBuffer().put(0,position)
    buffer.rewind()
  }

  def isSealed(): Boolean = {
    val value =  buffer.asLongBuffer().get(0)
    buffer.rewind()
    return (value < 0)
  }

  def setSealed( ): Unit = {
    val value =  buffer.asLongBuffer().get(0)
    buffer.asLongBuffer().put(0,-value)
    buffer.rewind()
  }

  def getWrittenPosition( ): Long = {
    val value =  buffer.asLongBuffer().get(0)
    buffer.rewind()
    return value
  }

  def setWatermarkPosition(position: Long): Unit = {

    buffer.asLongBuffer().put(1,position)
    buffer.rewind()
  }

  def getWatermarkPosition(): Long = {
    val value =  buffer.asLongBuffer().get(1)
    buffer.rewind()
    return value
  }

  def setLSOPosition(position: Long): Unit = {


    buffer.asLongBuffer().put( 2,position)
    buffer.rewind()
  }

  def getLSOPosition(): Long = {
    val value =  buffer.asLongBuffer().get(2)
    buffer.rewind()
    return value
  }



  def setWatermarkOffset(offset: Long): Unit = {

    buffer.asLongBuffer().put(3,offset)
    buffer.rewind()
  }

  def getWatermarkOffset(): Long = {
    val value =  buffer.asLongBuffer().get(3)
    buffer.rewind()
    return value
  }


  def setLSOOffset(offset: Long): Unit = {


    buffer.asLongBuffer().put( 4,offset)
    buffer.rewind()
  }

  def getLSOOffset(): Long = {
    val value =  buffer.asLongBuffer().get(4)
    buffer.rewind()
    return value
  }




  override def toString = {

    val wp = getWrittenPosition
    val wm = getWatermarkPosition
    val lso = getLSOPosition

    s"Slot[$slotId] for client[$clientId]:  $wp,$wm,$lso"
  }

}

