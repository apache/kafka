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
import java.util.concurrent.locks.{Lock, ReadWriteLock}
import java.lang.management._
import java.util.{Base64, Properties, UUID}
import com.typesafe.scalalogging.Logger

import javax.management._
import scala.collection._
import scala.collection.{Seq, mutable}
import kafka.cluster.EndPoint
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.slf4j.event.Level

import scala.annotation.nowarn

/**
 * General helper functions!
 *
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 *
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object CoreUtils {
  private val logger = Logger(getClass)

  /**
   * Return the smallest element in `iterable` if it is not empty. Otherwise return `ifEmpty`.
   */
  def min[A, B >: A](iterable: Iterable[A], ifEmpty: A)(implicit cmp: Ordering[B]): A =
    if (iterable.isEmpty) ifEmpty else iterable.min(cmp)

  /**
    * Do the given action and log any exceptions thrown without rethrowing them.
    *
    * @param action The action to execute.
    * @param logging The logging instance to use for logging the thrown exception.
    * @param logLevel The log level to use for logging.
    */
  def swallow(action: => Unit, logging: Logging, logLevel: Level = Level.WARN): Unit = {
    try {
      action
    } catch {
      case e: Throwable => logLevel match {
        case Level.ERROR => logger.error(e.getMessage, e)
        case Level.WARN => logger.warn(e.getMessage, e)
        case Level.INFO => logger.info(e.getMessage, e)
        case Level.DEBUG => logger.debug(e.getMessage, e)
        case Level.TRACE => logger.trace(e.getMessage, e)
      }
    }
  }

  /**
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param files sequence of files to be deleted
   */
  def delete(files: Seq[String]): Unit = files.foreach(f => Utils.delete(new File(f)))

  /**
   * Invokes every function in `all` even if one or more functions throws an exception.
   *
   * If any of the functions throws an exception, the first one will be rethrown at the end with subsequent exceptions
   * added as suppressed exceptions.
   */
  // Note that this is a generalised version of `Utils.closeAll`. We could potentially make it more general by
  // changing the signature to `def tryAll[R](all: Seq[() => R]): Seq[R]`
  def tryAll(all: Seq[() => Unit]): Unit = {
    var exception: Throwable = null
    all.foreach { element =>
      try element.apply()
      catch {
        case e: Throwable =>
          if (exception != null)
            exception.addSuppressed(e)
          else
            exception = e
      }
    }
    if (exception != null)
      throw exception
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
        if (mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to register Mbean $name", e)
        false
    }
  }

  /**
   * Unregister the mbean with the given name, if there is one registered
   * @param name The mbean name to unregister
   */
  def unregisterMBean(name: String): Unit = {
    val mbs = ManagementFactory.getPlatformMBeanServer()
    mbs synchronized {
      val objName = new ObjectName(name)
      if (mbs.isRegistered(objName))
        mbs.unregisterMBean(objName)
    }
  }

  /**
   * Read some bytes into the provided buffer, and return the number of bytes read. If the
   * channel has been closed or we get -1 on the read for any reason, throw an EOFException
   */
  def read(channel: ReadableByteChannel, buffer: ByteBuffer): Int = {
    channel.read(buffer) match {
      case -1 => throw new EOFException("Received -1 when reading from channel, socket has likely been closed.")
      case n => n
    }
  }

  /**
   * This method gets comma separated values which contains key,value pairs and returns a map of
   * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
   * Also supports strings with multiple ":" such as IpV6 addresses, taking the last occurrence
   * of the ":" in the pair as the split, eg a:b:c:val1, d:e:f:val2 => a:b:c -> val1, d:e:f -> val2
   */
  def parseCsvMap(str: String): Map[String, String] = {
    val map = new mutable.HashMap[String, String]
    if ("".equals(str))
      return map
    val keyVals = str.split("\\s*,\\s*").map(s => {
      val lio = s.lastIndexOf(":")
      (s.substring(0,lio).trim, s.substring(lio + 1).trim)
    })
    keyVals.toMap
  }

  /**
   * Parse a comma separated string into a sequence of strings.
   * Whitespace surrounding the comma will be removed.
   */
  def parseCsvList(csvList: String): Seq[String] = {
    if (csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
  }

  /**
   * Create an instance of the class with the given class name
   */
  def createObject[T <: AnyRef](className: String, args: AnyRef*): T = {
    val klass = Class.forName(className, true, Utils.getContextOrKafkaClassLoader()).asInstanceOf[Class[T]]
    val constructor = klass.getConstructor(args.map(_.getClass): _*)
    constructor.newInstance(args: _*)
  }

  /**
   * Create a circular (looping) iterator over a collection.
   * @param coll An iterable over the underlying collection.
   * @return A circular iterator over the collection.
   */
  def circularIterator[T](coll: Iterable[T]) =
    for (_ <- Iterator.continually(1); t <- coll) yield t

  /**
   * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
   */
  def replaceSuffix(s: String, oldSuffix: String, newSuffix: String): String = {
    if(!s.endsWith(oldSuffix))
      throw new IllegalArgumentException("Expected string to end with '%s' but string is '%s'".format(oldSuffix, s))
    s.substring(0, s.length - oldSuffix.length) + newSuffix
  }

  /**
   * Execute the given function inside the lock
   */
  def inLock[T](lock: Lock)(fun: => T): T = {
    lock.lock()
    try {
      fun
    } finally {
      lock.unlock()
    }
  }

  def inReadLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.readLock)(fun)

  def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.writeLock)(fun)

  /**
   * Returns a list of duplicated items
   */
  def duplicates[T](s: Iterable[T]): Iterable[T] = {
    s.groupBy(identity)
      .map { case (k, l) => (k, l.size)}
      .filter { case (_, l) => l > 1 }
      .keys
  }

  def listenerListToEndPoints(listeners: String, securityProtocolMap: Map[ListenerName, SecurityProtocol]): Seq[EndPoint] = {
    listenerListToEndPoints(listeners, securityProtocolMap, true)
  }

  def listenerListToEndPoints(listeners: String, securityProtocolMap: Map[ListenerName, SecurityProtocol], requireDistinctPorts: Boolean): Seq[EndPoint] = {
    def validate(endPoints: Seq[EndPoint]): Unit = {
      // filter port 0 for unit tests
      val portsExcludingZero = endPoints.map(_.port).filter(_ != 0)
      val distinctListenerNames = endPoints.map(_.listenerName).distinct

      require(distinctListenerNames.size == endPoints.size, s"Each listener must have a different name, listeners: $listeners")
      if (requireDistinctPorts) {
        val distinctPorts = portsExcludingZero.distinct
        require(distinctPorts.size == portsExcludingZero.size, s"Each listener must have a different port, listeners: $listeners")
      }
    }

    val endPoints = try {
      val listenerList = parseCsvList(listeners)
      listenerList.map(EndPoint.createEndPoint(_, Some(securityProtocolMap)))
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Error creating broker listeners from '$listeners': ${e.getMessage}", e)
    }
    validate(endPoints)
    endPoints
  }

  def generateUuidAsBase64(): String = {
    val uuid = UUID.randomUUID()
    Base64.getUrlEncoder.withoutPadding.encodeToString(getBytesFromUuid(uuid))
  }

  def getBytesFromUuid(uuid: UUID): Array[Byte] = {
    // Extract bytes for uuid which is 128 bits (or 16 bytes) long.
    val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
    uuidBytes.putLong(uuid.getMostSignificantBits)
    uuidBytes.putLong(uuid.getLeastSignificantBits)
    uuidBytes.array
  }

  def propsWith(key: String, value: String): Properties = {
    propsWith((key, value))
  }

  def propsWith(props: (String, String)*): Properties = {
    val properties = new Properties()
    props.foreach { case (k, v) => properties.put(k, v) }
    properties
  }

  /**
   * Atomic `getOrElseUpdate` for concurrent maps. This is optimized for the case where
   * keys often exist in the map, avoiding the need to create a new value. `createValue`
   * may be invoked more than once if multiple threads attempt to insert a key at the same
   * time, but the same inserted value will be returned to all threads.
   *
   * In Scala 2.12, `ConcurrentMap.getOrElse` has the same behaviour as this method, but JConcurrentMapWrapper that
   * wraps Java maps does not.
   */
  def atomicGetOrUpdate[K, V](map: concurrent.Map[K, V], key: K, createValue: => V): V = {
    map.get(key) match {
      case Some(value) => value
      case None =>
        val value = createValue
        map.putIfAbsent(key, value).getOrElse(value)
    }
  }

  @nowarn("cat=unused") // see below for explanation
  def groupMapReduce[T, K, B](elements: Iterable[T])(key: T => K)(f: T => B)(reduce: (B, B) => B): Map[K, B] = {
    // required for Scala 2.12 compatibility, unused in Scala 2.13 and hence we need to suppress the unused warning
    import scala.collection.compat._
    elements.groupMapReduce(key)(f)(reduce)
  }

}
