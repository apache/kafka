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
import charset.Charset
import java.nio.channels._
import java.lang.management._
import javax.management._
import scala.collection._
import mutable.ListBuffer
import scala.collection.mutable
import java.util.Properties
import kafka.common.KafkaException
import kafka.common.KafkaStorageException


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
   * Create a daemon thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  def daemonThread(runnable: Runnable): Thread =
    newThread(runnable, true)

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
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      } 
    })
    thread
  }
   
  /**
   * Create a new thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  def newThread(runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      }
    })
    thread
  }
  
  /**
   * Read the given byte buffer into a byte array
   */
  def readBytes(buffer: ByteBuffer): Array[Byte] = readBytes(buffer, 0, buffer.limit)

  /**
   * Read a byte array from the given offset and size in the buffer
   */
  def readBytes(buffer: ByteBuffer, offset: Int, size: Int): Array[Byte] = {
    val dest = new Array[Byte](size)
    if(buffer.hasArray) {
      System.arraycopy(buffer.array, buffer.arrayOffset() + offset, dest, 0, size)
    } else {
      buffer.mark()
      buffer.get(dest)
      buffer.reset()
    }
    dest
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
  def swallow(log: (Object, Throwable) => Unit, action: => Unit) {
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
  def readString(buffer: ByteBuffer, encoding: String = Charset.defaultCharset.toString): String = {
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
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param a sequence of files to be deleted
   */
  def rm(files: Seq[String]): Unit = files.map(f => rm(new File(f)))
  
  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   * @param file The root file at which to begin deleting
   */
  def rm(file: File) {
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
  def readUnsignedInt(buffer: ByteBuffer): Long = 
    buffer.getInt() & 0xffffffffL
  
  /**
   * Read an unsigned integer from the given position without modifying the buffers
   * position
   * @param buffer the buffer to read from
   * @param index the index from which to read the integer
   * @return The integer read, as a long to avoid signedness
   */
  def readUnsignedInt(buffer: ByteBuffer, index: Int): Long = 
    buffer.getInt(index) & 0xffffffffL
  
  /**
   * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
   * @param buffer The buffer to write to
   * @param value The value to write
   */
  def writetUnsignedInt(buffer: ByteBuffer, value: Long): Unit = 
    buffer.putInt((value & 0xffffffffL).asInstanceOf[Int])
  
  /**
   * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
   * @param buffer The buffer to write to
   * @param index The position in the buffer at which to begin writing
   * @param value The value to write
   */
  def writeUnsignedInt(buffer: ByteBuffer, index: Int, value: Long): Unit = 
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
   * @param offset the offset at which to begin checksumming
   * @param size the number of bytes to checksum
   * @return The CRC32
   */
  def crc32(bytes: Array[Byte], offset: Int, size: Int): Long = {
    val crc = new Crc32()
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
  
  /**
   * Throw an exception if the given value is null, else return it. You can use this like:
   * val myValue = Utils.notNull(expressionThatShouldntBeNull)
   */
  def notNull[V](v: V) = {
    if(v == null)
      throw new KafkaException("Value cannot be null.")
    else
      v
  }

  /**
   * Parse a host and port out of a string
   */
  def parseHostPort(hostport: String) : (String, Int) = {
    val splits = hostport.split(":")
    (splits(0), splits(1).toInt)
  }

  /**
   * Get the stack trace from an exception as a string
   */
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
  def parseCsvMap(str: String): Map[String, String] = {
    val map = new mutable.HashMap[String, String]
    if("".equals(str))
      return map    
    val keyVals = str.split("\\s*,\\s*").map(s => s.split("\\s*:\\s*"))
    keyVals.map(pair => (pair(0), pair(1))).toMap
  }
  
  /**
   * Parse a comma separated string into a sequence of strings.
   * Whitespace surrounding the comma will be removed.
   */
  def parseCsvList(csvList: String): Seq[String] = {
    if(csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else {
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
    }
  }

  /**
   * Create an instance of the class with the given class name
   */
  def createObject[T<:AnyRef](className: String, args: AnyRef*): T = {
    val klass = Class.forName(className).asInstanceOf[Class[T]]
    val constructor = klass.getConstructor(args.map(_.getClass): _*)
    constructor.newInstance(args: _*).asInstanceOf[T]
  }

  /**
   * Is the given string null or empty ("")?
   */
  def nullOrEmpty(s: String): Boolean = s == null || s.equals("")

  /**
   * Merge JSON fields of the format "key" : value/object/array.
   */
  def mergeJsonFields(objects: Seq[String]): String = {
    val builder = new StringBuilder
    builder.append("{ ")
    builder.append(objects.sorted.map(_.trim).mkString(", "))
    builder.append(" }")
    builder.toString
  }

 /**
   * Format a Map[String, String] as JSON object.
   */
  def mapToJsonFields(jsonDataMap: Map[String, String], valueInQuotes: Boolean): Seq[String] = {
    val jsonFields: mutable.ListBuffer[String] = ListBuffer()
    val builder = new StringBuilder
    for ((key, value) <- jsonDataMap.toList.sorted) {
      builder.append("\"" + key + "\":")
      if (valueInQuotes)
        builder.append("\"" + value + "\"")
      else
        builder.append(value)
      jsonFields += builder.toString
      builder.clear()
    }
    jsonFields
  }

  /**
   * Format a Map[String, String] as JSON object.
   */
  def mapToJson(jsonDataMap: Map[String, String], valueInQuotes: Boolean): String = {
    mergeJsonFields(mapToJsonFields(jsonDataMap, valueInQuotes))
  }

   /**
   * Format a Seq[String] as JSON array.
   */
  def seqToJson(jsonData: Seq[String], valueInQuotes: Boolean): String = {
    val builder = new StringBuilder
    builder.append("[ ")
    if (valueInQuotes)
      builder.append(jsonData.map("\"" + _ + "\"")).mkString(", ")
    else
      builder.append(jsonData.mkString(", "))
    builder.append(" ]")
    builder.toString
  }

  /**
   * Format a Map[String, Seq[Int]] as JSON
   */

  def mapWithSeqValuesToJson(jsonDataMap: Map[String, Seq[Int]]): String = {
    mergeJsonFields(mapToJsonFields(jsonDataMap.map(e => (e._1 -> seqToJson(e._2.map(_.toString), valueInQuotes = false))),
                                    valueInQuotes = false))
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

  /**
   * Attempt to read a file as a string
   */
  def readFileAsString(path: String, charset: Charset = Charset.defaultCharset()): String = {
    val stream = new FileInputStream(new File(path))
    try {
      val fc = stream.getChannel()
      val bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
      charset.decode(bb).toString()
    }
    finally {
      stream.close()
    }
  }
  
  /**
   * Get the absolute value of the given number. If the number is Int.MinValue return 0.
   * This is different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
   */
  def abs(n: Int) = n & 0x7fffffff

  /**
   * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
   */
  def replaceSuffix(s: String, oldSuffix: String, newSuffix: String): String = {
    if(!s.endsWith(oldSuffix))
      throw new IllegalArgumentException("Expected string to end with '%s' but string is '%s'".format(oldSuffix, s))
    s.substring(0, s.length - oldSuffix.length) + newSuffix
  }

  /**
   * Create a file with the given path
   * @param path The path to create
   * @throw KafkaStorageException If the file create fails
   * @return The created file
   */
  def createFile(path: String): File = {
    val f = new File(path)
    val created = f.createNewFile()
    if(!created)
      throw new KafkaStorageException("Failed to create file %s.".format(path))
    f
  }
  
  /**
   * Turn a properties map into a string
   */
  def asString(props: Properties): String = {
    val writer = new StringWriter()
    props.store(writer, "")
    writer.toString
  }
  
  /**
   * Read some properties with the given default values
   */
  def readProps(s: String, defaults: Properties): Properties = {
    val reader = new StringReader(s)
    val props = new Properties(defaults)
    props.load(reader)
    props
  }
  
  /**
   * Read a big-endian integer from a byte array
   */
  def readInt(bytes: Array[Byte], offset: Int): Int = {
    ((bytes(offset) & 0xFF) << 24) |
    ((bytes(offset + 1) & 0xFF) << 16) |
    ((bytes(offset + 2) & 0xFF) << 8) |
    (bytes(offset + 3) & 0xFF)
  }
  
}
