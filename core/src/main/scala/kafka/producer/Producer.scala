/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.producer

import async.{CallbackHandler, EventHandler}
import org.apache.log4j.Logger
import kafka.serializer.Encoder
import kafka.utils._
import java.util.Properties
import kafka.cluster.{Partition, Broker}
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.ProducerRequest
import kafka.common.{NoBrokersForPartitionException, InvalidConfigException, InvalidPartitionException}

class Producer[K,V](config: ProducerConfig,
                    partitioner: Partitioner[K],
                    producerPool: ProducerPool[V],
                    populateProducerPool: Boolean,
                    private var brokerPartitionInfo: BrokerPartitionInfo) /* for testing purpose only. Applications should ideally */
                                                          /* use the other constructor*/
{
  private val logger = Logger.getLogger(classOf[Producer[K, V]])
  private val hasShutdown = new AtomicBoolean(false)
  if(!Utils.propertyExists(config.zkConnect) && !Utils.propertyExists(config.brokerPartitionInfo))
    throw new InvalidConfigException("At least one of zk.connect or broker.list must be specified")
  private val random = new java.util.Random
  // check if zookeeper based auto partition discovery is enabled
  private val zkEnabled = Utils.propertyExists(config.zkConnect)
  if(brokerPartitionInfo == null) {
    zkEnabled match {
      case true =>
        val zkProps = new Properties()
        zkProps.put("zk.connect", config.zkConnect)
        zkProps.put("zk.sessiontimeout.ms", config.zkSessionTimeoutMs.toString)
        zkProps.put("zk.connectiontimeout.ms", config.zkConnectionTimeoutMs.toString)
        zkProps.put("zk.synctime.ms", config.zkSyncTimeMs.toString)
        brokerPartitionInfo = new ZKBrokerPartitionInfo(new ZKConfig(zkProps), producerCbk)
      case false =>
        brokerPartitionInfo = new ConfigBrokerPartitionInfo(config)
    }
  }
  // pool of producers, one per broker
  if(populateProducerPool) {
    val allBrokers = brokerPartitionInfo.getAllBrokerInfo
    allBrokers.foreach(b => producerPool.addProducer(new Broker(b._1, b._2.host, b._2.host, b._2.port)))
  }

/**
 * This constructor can be used when all config parameters will be specified through the
 * ProducerConfig object
 * @param config Producer Configuration object
 */
  def this(config: ProducerConfig) =  this(config, Utils.getObject(config.partitionerClass),
    new ProducerPool[V](config, Utils.getObject(config.serializerClass)), true, null)

  /**
   * This constructor can be used to provide pre-instantiated objects for all config parameters
   * that would otherwise be instantiated via reflection. i.e. encoder, partitioner, event handler and
   * callback handler. If you use this constructor, encoder, eventHandler, callback handler and partitioner
   * will not be picked up from the config.
   * @param config Producer Configuration object
   * @param encoder Encoder used to convert an object of type V to a kafka.message.Message. If this is null it
   * throws an InvalidConfigException
   * @param eventHandler the class that implements kafka.producer.async.IEventHandler[T] used to
   * dispatch a batch of produce requests, using an instance of kafka.producer.SyncProducer. If this is null, it
   * uses the DefaultEventHandler
   * @param cbkHandler the class that implements kafka.producer.async.CallbackHandler[T] used to inject
   * callbacks at various stages of the kafka.producer.AsyncProducer pipeline. If this is null, the producer does
   * not use the callback handler and hence does not invoke any callbacks
   * @param partitioner class that implements the kafka.producer.Partitioner[K], used to supply a custom
   * partitioning strategy on the message key (of type K) that is specified through the ProducerData[K, T]
   * object in the  send API. If this is null, producer uses DefaultPartitioner
   */
  def this(config: ProducerConfig,
           encoder: Encoder[V],
           eventHandler: EventHandler[V],
           cbkHandler: CallbackHandler[V],
           partitioner: Partitioner[K]) =
    this(config, if(partitioner == null) new DefaultPartitioner[K] else partitioner,
         new ProducerPool[V](config, encoder, eventHandler, cbkHandler), true, null)
  /**
   * Sends the data, partitioned by key to the topic using either the
   * synchronous or the asynchronous producer
   * @param producerData the producer data object that encapsulates the topic, key and message data
   */
  def send(producerData: ProducerData[K,V]*) {
    val producerPoolRequests = producerData.map { pd =>
    // find the number of broker partitions registered for this topic
      logger.debug("Getting the number of broker partitions registered for topic: " + pd.getTopic)
      val numBrokerPartitions = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic).toSeq
      logger.debug("Broker partitions registered for topic: " + pd.getTopic + " = " + numBrokerPartitions)
      val totalNumPartitions = numBrokerPartitions.length
      if(totalNumPartitions == 0) throw new NoBrokersForPartitionException("Partition = " + pd.getKey)

      var brokerIdPartition: Partition = null
      var partition: Int = 0
      if(zkEnabled) {
        // get the partition id
        val partitionId = getPartition(pd.getKey, totalNumPartitions)
        brokerIdPartition = numBrokerPartitions(partitionId)
        val brokerInfo = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.brokerId).get
        logger.debug("Sending message to broker " + brokerInfo.host + ":" + brokerInfo.port +
                " on partition " + brokerIdPartition.partId)
        partition = brokerIdPartition.partId
      }else {
        // randomly select a broker
        val randomBrokerId = random.nextInt(totalNumPartitions)
        brokerIdPartition = numBrokerPartitions(randomBrokerId)
        val brokerInfo = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.brokerId).get

        logger.debug("Sending message to broker " + brokerInfo.host + ":" + brokerInfo.port +
                " on a randomly chosen partition")
        partition = ProducerRequest.RandomPartition
      }
      producerPool.getProducerPoolData(pd.getTopic,
                                       new Partition(brokerIdPartition.brokerId, partition),
                                       pd.getData)
    }
    producerPool.send(producerPoolRequests: _*)
  }

  /**
   * Retrieves the partition id and throws an InvalidPartitionException if
   * the value of partition is not between 0 and numPartitions-1
   * @param key the partition key
   * @param numPartitions the total number of available partitions
   * @returns the partition id
   */
  private def getPartition(key: K, numPartitions: Int): Int = {
    if(numPartitions <= 0)
      throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions +
              "\n Valid values are > 0")
    val partition = if(key == null) random.nextInt(numPartitions)
                    else partitioner.partition(key , numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new InvalidPartitionException("Invalid partition id : " + partition +
              "\n Valid values are in the range inclusive [0, " + (numPartitions-1) + "]")
    partition
  }
  
  /**
   * Callback to add a new producer to the producer pool. Used by ZKBrokerPartitionInfo
   * on registration of new broker in zookeeper
   * @param bid the id of the broker
   * @param host the hostname of the broker
   * @param port the port of the broker
   */
  private def producerCbk(bid: Int, host: String, port: Int) =  {
    if(populateProducerPool) producerPool.addProducer(new Broker(bid, host, host, port))
    else logger.debug("Skipping the callback since populateProducerPool = false")
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close() = {
    val canShutdown = hasShutdown.compareAndSet(false, true)
    if(canShutdown) {
      producerPool.close
      brokerPartitionInfo.close
    }
  }
}
