package kafka.api

import java.nio._
import kafka.common._

/**
 * Helper functions specific to parsing or serializing requests and responses
 */
object ApiUtils {
  
  val ProtocolEncoding = "UTF-8"

    /**
   * Read size prefixed string where the size is stored as a 2 byte short.
   * @param buffer The buffer to read from
   */
  def readShortString(buffer: ByteBuffer): String = {
    val size: Int = buffer.getShort()
    if(size < 0)
      return null
    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    new String(bytes, ProtocolEncoding)
  }
  
  /**
   * Write a size prefixed string where the size is stored as a 2 byte short
   * @param buffer The buffer to write to
   * @param string The string to write
   */
  def writeShortString(buffer: ByteBuffer, string: String) {
    if(string == null) {
      buffer.putShort(-1)
    } else {
      val encodedString = string.getBytes(ProtocolEncoding)
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
      } else {
        buffer.putShort(encodedString.length.asInstanceOf[Short])
        buffer.put(encodedString)
      }
    }
  }
  
  /**
   * Return size of a size prefixed string where the size is stored as a 2 byte short
   * @param string The string to write
   */
  def shortStringLength(string: String): Int = {
    if(string == null) {
      2
    } else {
      val encodedString = string.getBytes(ProtocolEncoding)
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
      } else {
        2 + encodedString.length
      }
    }
  }
  
  /**
   * Read an integer out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   */
  def readIntInRange(buffer: ByteBuffer, name: String, range: (Int, Int)): Int = {
    val value = buffer.getInt
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
    else value
  }

  /**
   * Read a short out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   */
  def readShortInRange(buffer: ByteBuffer, name: String, range: (Short, Short)): Short = {
    val value = buffer.getShort
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
    else value
  }

  /**
   * Read a long out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   */
  def readLongInRange(buffer: ByteBuffer, name: String, range: (Long, Long)): Long = {
    val value = buffer.getLong
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
    else value
  }
  
}
