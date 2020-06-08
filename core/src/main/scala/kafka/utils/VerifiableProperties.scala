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

import java.util.Properties
import java.util.Collections
import scala.collection._
import kafka.message.{CompressionCodec, NoCompressionCodec}
import scala.jdk.CollectionConverters._


class VerifiableProperties(val props: Properties) extends Logging {
  private val referenceSet = mutable.HashSet[String]()

  def this() = this(new Properties)

  def containsKey(name: String): Boolean = {
    props.containsKey(name)
  }

  def getProperty(name: String): String = {
    val value = props.getProperty(name)
    referenceSet.add(name)
    if(value == null) value else value.trim()
  }

  /**
   * Read a required integer property value or throw an exception if no such property is found
   */
  def getInt(name: String): Int = getString(name).toInt

  def getIntInRange(name: String, range: (Int, Int)): Int = {
    require(containsKey(name), "Missing required property '" + name + "'")
    getIntInRange(name, -1, range)
  }

  /**
   * Read an integer from the properties instance
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @return the integer value
   */
  def getInt(name: String, default: Int): Int =
    getIntInRange(name, default, (Int.MinValue, Int.MaxValue))

  def getShort(name: String, default: Short): Short =
    getShortInRange(name, default, (Short.MinValue, Short.MaxValue))

  /**
   * Read an integer from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @param range The range in which the value must fall (inclusive)
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the integer value
   */
  def getIntInRange(name: String, default: Int, range: (Int, Int)): Int = {
    val v =
      if(containsKey(name))
        getProperty(name).toInt
      else
        default
    require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
    v
  }

 def getShortInRange(name: String, default: Short, range: (Short, Short)): Short = {
    val v =
      if(containsKey(name))
        getProperty(name).toShort
      else
        default
    require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
    v
  }

  /**
   * Read a required long property value or throw an exception if no such property is found
   */
  def getLong(name: String): Long = getString(name).toLong

  /**
   * Read an long from the properties instance
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @return the long value
   */
  def getLong(name: String, default: Long): Long =
    getLongInRange(name, default, (Long.MinValue, Long.MaxValue))

  /**
   * Read an long from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @param range The range in which the value must fall (inclusive)
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the long value
   */
  def getLongInRange(name: String, default: Long, range: (Long, Long)): Long = {
    val v =
      if(containsKey(name))
        getProperty(name).toLong
      else
        default
    require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
    v
  }

  /**
   * Get a required argument as a double
   * @param name The property name
   * @return the value
   * @throws IllegalArgumentException If the given property is not present
   */
  def getDouble(name: String): Double = getString(name).toDouble

  /**
   * Get an optional argument as a double
   * @param name The property name
   * @param default The default value for the property if not present
   */
  def getDouble(name: String, default: Double): Double = {
    if(containsKey(name))
      getDouble(name)
    else
      default
  }

  /**
   * Read a boolean value from the properties instance
   * @param name The property name
   * @param default The default value to use if the property is not found
   * @return the boolean value
   */
  def getBoolean(name: String, default: Boolean): Boolean = {
    if(!containsKey(name))
      default
    else {
      val v = getProperty(name)
      require(v == "true" || v == "false", "Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false")
      v.toBoolean
    }
  }

  def getBoolean(name: String) = getString(name).toBoolean

  /**
   * Get a string property, or, if no such property is defined, return the given default value
   */
  def getString(name: String, default: String): String = {
    if(containsKey(name))
      getProperty(name)
    else
      default
  }

  /**
   * Get a string property or throw and exception if no such property is defined.
   */
  def getString(name: String): String = {
    require(containsKey(name), "Missing required property '" + name + "'")
    getProperty(name)
  }

  /**
   * Get a Map[String, String] from a property list in the form k1:v2, k2:v2, ...
   */
  def getMap(name: String, valid: String => Boolean = _ => true): Map[String, String] = {
    try {
      val m = CoreUtils.parseCsvMap(getString(name, ""))
      m.foreach {
        case(key, value) => 
          if(!valid(value))
            throw new IllegalArgumentException("Invalid entry '%s' = '%s' for property '%s'".format(key, value, name))
      }
      m
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(name, e.getMessage))
    }
  }

  /**
   * Parse compression codec from a property list in either. Codecs may be specified as integers, or as strings.
   * See [[kafka.message.CompressionCodec]] for more details.
   * @param name The property name
   * @param default Default compression codec
   * @return compression codec
   */
  def getCompressionCodec(name: String, default: CompressionCodec) = {
    val prop = getString(name, NoCompressionCodec.name)
    try {
      CompressionCodec.getCompressionCodec(prop.toInt)
    }
    catch {
      case _: NumberFormatException =>
        CompressionCodec.getCompressionCodec(prop)
    }
  }

  def verify(): Unit = {
    info("Verifying properties")
    val propNames = Collections.list(props.propertyNames).asScala.map(_.toString).sorted
    for(key <- propNames) {
      if (!referenceSet.contains(key) && !key.startsWith("external"))
        warn("Property %s is not valid".format(key))
      else
        info("Property %s is overridden to %s".format(key, props.getProperty(key)))
    }
  }
  
  override def toString: String = props.toString
 
}
