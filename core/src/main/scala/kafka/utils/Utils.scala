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

package kafka.utils

import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._
import java.lang.management._
import java.util.zip.CRC32
import javax.management._
import java.util.Properties
import scala.collection._
import scala.collection.mutable
import kafka.message.{NoCompressionCodec, CompressionCodec}
import org.I0Itec.zkclient.ZkClient
import joptsimple.{OptionSpec, OptionSet, OptionParser}
import util.parsing.json.JSON

/**
 * Helper functions!
 */
object Utils extends Logging {
  /**
   * Wrap the given function in a java.lang.Runnable
   * @param fun A function
   * @return A Runnable that just executes the function
   */
  def runnable(fun: () => Unit): Runnable = 
    new Runnable() {
      def run() = fun()
    }
  
  /**
   * Wrap the given function in a java.lang.Runnable that logs any errors encountered
   * @param fun A function
   * @return A Runnable that just executes the function
   */
  def loggedRunnable(fun: () => Unit): Runnable =
    new Runnable() {
      def run() = {
        try {
          fun()
        }
        catch {
          case t =>
            // log any error and the stack trace
            error("error in loggedRunnable", t)
        }
      }
    }

  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  def daemonThread(name: String, runnable: Runnable): Thread = 
    newThread(name, runnable, true)
  
  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param fun The runction to execute in the thread
   * @return The unstarted thread
   */
  def daemonThread(name: String, fun: () => Unit): Thread = 
    daemonThread(name, runnable(fun))
  
  /**
   * Create a new thread
   * @param name The name of the thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  def newThread(name: String, runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable, name) 
    thread.setDaemon(daemon)
    thread
  }
   
  /**
   * Read a byte array from the given offset and size in the buffer
   * TODO: Should use System.arraycopy
   */
  def readBytes(buffer: ByteBuffer, offset: Int, size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    var i = 0
    while(i < size) {
      bytes(i) = buffer.get(offset + i)
      i += 1
    }
    bytes
  }
  
  /**
   * Read size prefixed string where the size is stored as a 2 byte short.
   * @param buffer The buffer to read from
   * @param encoding The encoding in which to read the string
   */
  def readShortString(buffer: ByteBuffer, encoding: String): String = {
    val size: Int = buffer.getShort()
    if(size < 0)
      return null
    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    new String(bytes, encoding)
  }
  
  /**
   * Write a size prefixed string where the size is stored as a 2 byte short
   * @param buffer The buffer to write to
   * @param string The string to write
   * @param encoding The encoding in which to write the string
   */
  def writeShortString(buffer: ByteBuffer, string: String, encoding: String): Unit = {
    if(string == null) {
      buffer.putShort(-1)
    } else if(string.length > Short.MaxValue) {
      throw new IllegalArgumentException("String exceeds the maximum size of " + Short.MaxValue + ".")
    } else {
      buffer.putShort(string.length.asInstanceOf[Short])
      buffer.put(string.getBytes(encoding))
    }
  }
  
  /**
   * Read a properties file from the given path
   * @param filename The path of the file to read
   */
  def loadProps(filename: String): Properties = {
    val propStream = new FileInputStream(filename)
    val props = new Properties()
    props.load(propStream)
    props
  }
  
  /**
   * Read a required integer property value or throw an exception if no such property is found
   */
  def getInt(props: Properties, name: String): Int = {
    if(props.containsKey(name))
      return getInt(props, name, -1)
    else
      throw new IllegalArgumentException("Missing required property '" + name + "'")
  }
  
  /**
   * Read an integer from the properties instance
   * @param props The properties to read from
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @return the integer value
   */
  def getInt(props: Properties, name: String, default: Int): Int = 
    getIntInRange(props, name, default, (Int.MinValue, Int.MaxValue))
  
  /**
   * Read an integer from the properties instance. Throw an exception 
   * if the value is not in the given range (inclusive)
   * @param props The properties to read from
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @param range The range in which the value must fall (inclusive)
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the integer value
   */
  def getIntInRange(props: Properties, name: String, default: Int, range: (Int, Int)): Int = {
    val v = 
      if(props.containsKey(name))
        props.getProperty(name).toInt
      else
        default
    if(v < range._1 || v > range._2)
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + range + ".")
    else
      v
  }
  
  /**
   * Read a required long property value or throw an exception if no such property is found
   */
  def getLong(props: Properties, name: String): Long = {
    if(props.containsKey(name))
      return getLong(props, name, -1)
    else
      throw new IllegalArgumentException("Missing required property '" + name + "'")
  }

  /**
   * Read an long from the properties instance
   * @param props The properties to read from
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @return the long value
   */
  def getLong(props: Properties, name: String, default: Long): Long = 
    getLongInRange(props, name, default, (Long.MinValue, Long.MaxValue))

  /**
   * Read an long from the properties instance. Throw an exception 
   * if the value is not in the given range (inclusive)
   * @param props The properties to read from
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @param range The range in which the value must fall (inclusive)
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the long value
   */
  def getLongInRange(props: Properties, name: String, default: Long, range: (Long, Long)): Long = {
    val v = 
      if(props.containsKey(name))
        props.getProperty(name).toLong
      else
        default
    if(v < range._1 || v > range._2)
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + range + ".")
    else
      v
  }

  /**
   * Read a boolean value from the properties instance
   * @param props The properties to read from
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @return the boolean value
   */
  def getBoolean(props: Properties, name: String, default: Boolean): Boolean = {
    if(!props.containsKey(name))
      default
    else if("true" == props.getProperty(name))
      true
    else if("false" == props.getProperty(name))
      false
    else
      throw new IllegalArgumentException("Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false" )
  }
  
  /**
   * Get a string property, or, if no such property is defined, return the given default value
   */
  def getString(props: Properties, name: String, default: String): String = {
    if(props.containsKey(name))
      props.getProperty(name)
    else
      default
  }
  
  /**
   * Get a string property or throw and exception if no such property is defined.
   */
  def getString(props: Properties, name: String): String = {
    if(props.containsKey(name))
      props.getProperty(name)
    else
      throw new IllegalArgumentException("Missing required property '" + name + "'")
  }

  /**
   * Get a property of type java.util.Properties or throw and exception if no such property is defined.
   */
  def getProps(props: Properties, name: String): Properties = {
    if(props.containsKey(name)) {
      val propString = props.getProperty(name)
      val propValues = propString.split(",")
      val properties = new Properties
      for(i <- 0 until propValues.length) {
        val prop = propValues(i).split("=")
        if(prop.length != 2)
          throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues(i) + "'")
        properties.put(prop(0), prop(1))
      }
      properties
    }
    else
      throw new IllegalArgumentException("Missing required property '" + name + "'")
  }

  /**
   * Get a property of type java.util.Properties or return the default if no such property is defined
   */
  def getProps(props: Properties, name: String, default: Properties): Properties = {
    if(props.containsKey(name)) {
      val propString = props.getProperty(name)
      val propValues = propString.split(",")
      if(propValues.length < 1)
        throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'")
      val properties = new Properties
      for(i <- 0 until propValues.length) {
        val prop = propValues(i).split("=")
        if(prop.length != 2)
          throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues(i) + "'")
        properties.put(prop(0), prop(1))
      }
      properties
    }
    else
      default
  }

  /**
   * Open a channel for the given file
   */
  def openChannel(file: File, mutable: Boolean): FileChannel = {
    if(mutable)
      new RandomAccessFile(file, "rw").getChannel()
    else
      new FileInputStream(file).getChannel()
  }
  
  /**
   * Do the given action and log any exceptions thrown without rethrowing them
   * @param log The log method to use for logging. E.g. logger.warn
   * @param action The action to execute
   */
  def swallow(log: (Object, Throwable) => Unit, action: => Unit) = {
    try {
      action
    } catch {
      case e: Throwable => log(e.getMessage(), e)
    }
  }
  
  /**
   * Test if two byte buffers are equal. In this case equality means having
   * the same bytes from the current position to the limit
   */
  def equal(b1: ByteBuffer, b2: ByteBuffer): Boolean = {
    // two byte buffers are equal if their position is the same,
    // their remaining bytes are the same, and their contents are the same
    if(b1.position != b2.position)
      return false
    if(b1.remaining != b2.remaining)
      return false
    for(i <- 0 until b1.remaining)
      if(b1.get(i) != b2.get(i))
        return false
    return true
  }
  
  /**
   * Translate the given buffer into a string
   * @param buffer The buffer to translate
   * @param encoding The encoding to use in translating bytes to characters
   */
  def toString(buffer: ByteBuffer, encoding: String): String = {
    val bytes = new Array[Byte](buffer.remaining)
    buffer.get(bytes)
    new String(bytes, encoding)
  }
  
  /**
   * Print an error message and shutdown the JVM
   * @param message The error message
   */
  def croak(message: String) {
    System.err.println(message)
    System.exit(1)
  }
  
  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   * @param file The root file at which to begin deleting
   */
  def rm(file: String): Unit = rm(new File(file))
  
  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   * @param file The root file at which to begin deleting
   */
  def rm(file: File): Unit = {
    if(file == null) {
      return
    } else if(file.isDirectory) {
      val files = file.listFiles()
      if(files != null) {
        for(f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }
  
  /**
   * Register the given mbean with the platform mbean server,
   * unregistering any mbean that was there before. Note,
   * this method will not throw an exception if the registration
   * fails (since there is nothing you can do and it isn't fatal),
   * instead it just returns false indicating the registration failed.
   * @param mbean The object to register as an mbean
   * @param name The name to register this mbean with
   * @return true if the registration succeeded
   */
  def registerMBean(mbean: Object, name: String): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer()
      mbs synchronized {
        val objName = new ObjectName(name)
        if(mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception => {
        error("Failed to register Mbean " + name, e)
        false
      }
    }
  }
  
  /**
   * Unregister the mbean with the given name, if there is one registered
   * @param name The mbean name to unregister
   */
  def unregisterMBean(name: String) {
    val mbs = ManagementFactory.getPlatformMBeanServer()
    mbs synchronized {
      val objName = new ObjectName(name)
      if(mbs.isRegistered(objName))
        mbs.unregisterMBean(objName)
    }
  }
  
  /**
   * Read an unsigned integer from the current position in the buffer, 
   * incrementing the position by 4 bytes
   * @param buffer The buffer to read from
   * @return The integer read, as a long to avoid signedness
   */
  def getUnsignedInt(buffer: ByteBuffer): Long = 
    buffer.getInt() & 0xffffffffL
  
  /**
   * Read an unsigned integer from the given position without modifying the buffers
   * position
   * @param The buffer to read from
   * @param index the index from which to read the integer
   * @return The integer read, as a long to avoid signedness
   */
  def getUnsignedInt(buffer: ByteBuffer, index: Int): Long = 
    buffer.getInt(index) & 0xffffffffL
  
  /**
   * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
   * @param buffer The buffer to write to
   * @param value The value to write
   */
  def putUnsignedInt(buffer: ByteBuffer, value: Long): Unit = 
    buffer.putInt((value & 0xffffffffL).asInstanceOf[Int])
  
  /**
   * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
   * @param buffer The buffer to write to
   * @param index The position in the buffer at which to begin writing
   * @param value The value to write
   */
  def putUnsignedInt(buffer: ByteBuffer, index: Int, value: Long): Unit = 
    buffer.putInt(index, (value & 0xffffffffL).asInstanceOf[Int])
  
  /**
   * Compute the CRC32 of the byte array
   * @param bytes The array to compute the checksum for
   * @return The CRC32
   */
  def crc32(bytes: Array[Byte]): Long = crc32(bytes, 0, bytes.length)
  
  /**
   * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
   * @param bytes The bytes to checksum
   * @param the offset at which to begin checksumming
   * @param the number of bytes to checksum
   * @return The CRC32
   */
  def crc32(bytes: Array[Byte], offset: Int, size: Int): Long = {
    val crc = new CRC32()
    crc.update(bytes, offset, size)
    crc.getValue()
  }
  
  /**
   * Compute the hash code for the given items
   */
  def hashcode(as: Any*): Int = {
    if(as == null)
      return 0
    var h = 1
    var i = 0
    while(i < as.length) {
      if(as(i) != null) {
        h = 31 * h + as(i).hashCode
        i += 1
      }
    }
    return h
  }
  
  /**
   * Group the given values by keys extracted with the given function
   */
  def groupby[K,V](vals: Iterable[V], f: V => K): Map[K,List[V]] = {
    val m = new mutable.HashMap[K, List[V]]
    for(v <- vals) {
      val k = f(v)
      m.get(k) match {
        case Some(l: List[V]) => m.put(k, v :: l)
        case None => m.put(k, List(v))
      }
    } 
    m
  }
  
  /**
   * Read some bytes into the provided buffer, and return the number of bytes read. If the 
   * channel has been closed or we get -1 on the read for any reason, throw an EOFException
   */
  def read(channel: ReadableByteChannel, buffer: ByteBuffer): Int = {
    channel.read(buffer) match {
      case -1 => throw new EOFException("Received -1 when reading from channel, socket has likely been closed.")
      case n: Int => n
    }
  } 
  
  def notNull[V](v: V) = {
    if(v == null)
      throw new IllegalArgumentException("Value cannot be null.")
    else
      v
  }

  def getHostPort(hostport: String) : Tuple2[String, Int] = {
    val splits = hostport.split(":")
    (splits(0), splits(1).toInt)
  }

  def getTopicPartition(topicPartition: String) : Tuple2[String, Int] = {
    val index = topicPartition.lastIndexOf('-')
    (topicPartition.substring(0,index), topicPartition.substring(index+1).toInt)
  }

  def stackTrace(e: Throwable): String = {
    val sw = new StringWriter;
    val pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    sw.toString();
  }

  /**
   * This method gets comma separated values which contains key,value pairs and returns a map of
   * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
   */
  private def getCSVMap[K, V](allCSVals: String, exceptionMsg:String, successMsg:String) :Map[K, V] = {
    val map = new mutable.HashMap[K, V]
    if("".equals(allCSVals))
      return map
    val csVals = allCSVals.split(",")
    for(i <- 0 until csVals.length)
    {
     try{
      val tempSplit = csVals(i).split(":")
      info(successMsg + tempSplit(0) + " : " + Integer.parseInt(tempSplit(1).trim))
      map += tempSplit(0).asInstanceOf[K] -> Integer.parseInt(tempSplit(1).trim).asInstanceOf[V]
      } catch {
          case _ =>  error(exceptionMsg + ": " + csVals(i))
        }
    }
    map
  }

  def getCSVList(csvList: String): Seq[String] = {
    if(csvList == null)
      Seq.empty[String]
    else {
      csvList.split(",").filter(v => !v.equals(""))
    }
  }

  def getTopicRetentionHours(retentionHours: String) : Map[String, Int] = {
    val exceptionMsg = "Malformed token for topic.log.retention.hours in server.properties: "
    val successMsg =  "The retention hours for "
    val map: Map[String, Int] = getCSVMap(retentionHours, exceptionMsg, successMsg)
    map.foreach{case(topic, hrs) =>
                  require(hrs > 0, "Log retention hours value for topic " + topic + " is " + hrs +
                                   " which is not greater than 0.")}
    map
  }

  def getTopicRollHours(rollHours: String) : Map[String, Int] = {
    val exceptionMsg = "Malformed token for topic.log.roll.hours in server.properties: "
    val successMsg =  "The roll hours for "
    val map: Map[String, Int] = getCSVMap(rollHours, exceptionMsg, successMsg)
    map.foreach{case(topic, hrs) =>
                  require(hrs > 0, "Log roll hours value for topic " + topic + " is " + hrs +
                                   " which is not greater than 0.")}
    map
  }

  def getTopicFileSize(fileSizes: String): Map[String, Int] = {
    val exceptionMsg = "Malformed token for topic.log.file.size in server.properties: "
    val successMsg =  "The log file size for "
    val map: Map[String, Int] = getCSVMap(fileSizes, exceptionMsg, successMsg)
    map.foreach{case(topic, size) =>
                  require(size > 0, "Log file size value for topic " + topic + " is " + size +
                                   " which is not greater than 0.")}
    map
  }

  def getTopicRetentionSize(retentionSizes: String): Map[String, Long] = {
    val exceptionMsg = "Malformed token for topic.log.retention.size in server.properties: "
    val successMsg =  "The log retention size for "
    val map: Map[String, Long] = getCSVMap(retentionSizes, exceptionMsg, successMsg)
    map.foreach{case(topic, size) =>
                 require(size > 0, "Log retention size value for topic " + topic + " is " + size +
                                   " which is not greater than 0.")}
    map
  }

  def getTopicFlushIntervals(allIntervals: String) : Map[String, Int] = {
    val exceptionMsg = "Malformed token for topic.flush.Intervals.ms in server.properties: "
    val successMsg =  "The flush interval for "
    val map: Map[String, Int] = getCSVMap(allIntervals, exceptionMsg, successMsg)
    map.foreach{case(topic, interval) =>
                  require(interval > 0, "Flush interval value for topic " + topic + " is " + interval +
                                        " ms which is not greater than 0.")}
    map
  }

  def getTopicPartitions(allPartitions: String) : Map[String, Int] = {
    val exceptionMsg = "Malformed token for topic.partition.counts in server.properties: "
    val successMsg =  "The number of partitions for topic  "
    val map: Map[String, Int] = getCSVMap(allPartitions, exceptionMsg, successMsg)
    map.foreach{case(topic, count) =>
                  require(count > 0, "The number of partitions for topic " + topic + " is " + count +
                                     " which is not greater than 0.")}
    map
  }

  def getConsumerTopicMap(consumerTopicString: String) : Map[String, Int] = {
    val exceptionMsg = "Malformed token for embeddedconsumer.topics in consumer.properties: "
    val successMsg =  "The number of consumer threads for topic  "
    val map: Map[String, Int] = getCSVMap(consumerTopicString, exceptionMsg, successMsg)
    map.foreach{case(topic, count) =>
                  require(count > 0, "The number of consumer threads for topic " + topic + " is " + count +
                                     " which is not greater than 0.")}
    map
  }

  def getObject[T<:AnyRef](className: String): T = {
    className match {
      case null => null.asInstanceOf[T]
      case _ =>
        val clazz = Class.forName(className)
        val clazzT = clazz.asInstanceOf[Class[T]]
        val constructors = clazzT.getConstructors
        require(constructors.length == 1)
        constructors.head.newInstance().asInstanceOf[T]
    }
  }

  def propertyExists(prop: String): Boolean = {
    if(prop == null)
      false
    else if(prop.compareTo("") == 0)
      false
    else true
  }

  def getCompressionCodec(props: Properties, codec: String): CompressionCodec = {
    val codecValueString = props.getProperty(codec)
    if(codecValueString == null)
      NoCompressionCodec
    else
      CompressionCodec.getCompressionCodec(codecValueString.toInt)
  }

  def tryCleanupZookeeper(zkUrl: String, groupId: String) {
    try {
      val dir = "/consumers/" + groupId
      logger.info("Cleaning up temporary zookeeper data under " + dir + ".")
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _ => // swallow
    }
  }

  def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*) {
    for(arg <- required) {
      if(!options.has(arg)) {
        error("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
  }

  /**
   * Create a circular (looping) iterator over a collection.
   * @param coll An iterable over the underlying collection.
   * @return A circular iterator over the collection.
   */
  def circularIterator[T](coll: Iterable[T]) = {
    val stream: Stream[T] =
      for (forever <- Stream.continually(1); t <- coll) yield t
    stream.iterator
  }
}

class SnapshotStats(private val monitorDurationNs: Long = 600L * 1000L * 1000L * 1000L) {
  private val time: Time = SystemTime

  private val complete = new AtomicReference(new Stats())
  private val current = new AtomicReference(new Stats())
  private val total = new AtomicLong(0)
  private val numCumulatedRequests = new AtomicLong(0)

  def recordRequestMetric(requestNs: Long) {
    val stats = current.get
    stats.add(requestNs)
    total.getAndAdd(requestNs)
    numCumulatedRequests.getAndAdd(1)
    val ageNs = time.nanoseconds - stats.start
    // if the current stats are too old it is time to swap
    if(ageNs >= monitorDurationNs) {
      val swapped = current.compareAndSet(stats, new Stats())
      if(swapped) {
        complete.set(stats)
        stats.end.set(time.nanoseconds)
      }
    }
  }

  def recordThroughputMetric(data: Long) {
    val stats = current.get
    stats.addData(data)
    val ageNs = time.nanoseconds - stats.start
    // if the current stats are too old it is time to swap
    if(ageNs >= monitorDurationNs) {
      val swapped = current.compareAndSet(stats, new Stats())
      if(swapped) {
        complete.set(stats)
        stats.end.set(time.nanoseconds)
      }
    }
  }

  def getNumRequests(): Long = numCumulatedRequests.get

  def getRequestsPerSecond: Double = {
    val stats = complete.get
    stats.numRequests / stats.durationSeconds
  }

  def getThroughput: Double = {
    val stats = complete.get
    stats.totalData / stats.durationSeconds
  }

  def getAvgMetric: Double = {
    val stats = complete.get
    if (stats.numRequests == 0) {
      0
    }
    else {
      stats.totalRequestMetric / stats.numRequests
    }
  }

  def getTotalMetric: Long = total.get

  def getMaxMetric: Double = complete.get.maxRequestMetric

  class Stats {
    val start = time.nanoseconds
    var end = new AtomicLong(-1)
    var numRequests = 0
    var totalRequestMetric: Long = 0L
    var maxRequestMetric: Long = 0L
    var totalData: Long = 0L
    private val lock = new Object()

    def addData(data: Long) {
      lock synchronized {
        totalData += data
      }
    }

    def add(requestNs: Long) {
      lock synchronized {
        numRequests +=1
        totalRequestMetric += requestNs
        maxRequestMetric = scala.math.max(maxRequestMetric, requestNs)
      }
    }

    def durationSeconds: Double = (end.get - start) / (1000.0 * 1000.0 * 1000.0)

    def durationMs: Double = (end.get - start) / (1000.0 * 1000.0)
  }
}

/**
 *  A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
object SyncJSON extends Logging {
  val myConversionFunc = {input : String => input.toInt}
  JSON.globalNumberParser = myConversionFunc
  val lock = new Object

  def parseFull(input: String): Option[Any] = {
    lock synchronized {
      try {
        JSON.parseFull(input)
      } catch {
        case t =>
          throw new RuntimeException("Can't parse json string: %s".format(input), t)
      }
    }
  }
}