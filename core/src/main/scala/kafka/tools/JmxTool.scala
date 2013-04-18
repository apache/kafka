/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package kafka.tools

import java.util.Date
import java.text.SimpleDateFormat
import javax.management._
import javax.management.remote._
import joptsimple.OptionParser
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.math._
import kafka.utils.Logging

object JmxTool extends Logging {

  def main(args: Array[String]) {
    // Parse command line
    val parser = new OptionParser
    val objectNameOpt =
      parser.accepts("object-name", "A JMX object name to use as a query. This can contain wild cards, and this option " +
        "can be given multiple times to specify more than one query. If no objects are specified " +
        "all objects will be queried.")
        .withRequiredArg
        .describedAs("name")
        .ofType(classOf[String])
    val attributesOpt =
      parser.accepts("attributes", "The whitelist of attributes to query. This is a comma-separated list. If no " +
        "attributes are specified all objects will be queried.")
        .withRequiredArg
        .describedAs("name")
        .ofType(classOf[String])
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval in MS with which to poll jmx stats.")
      .withRequiredArg
      .describedAs("ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(2000)
    val helpOpt = parser.accepts("help", "Print usage information.")
    val dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
      "See java.text.SimpleDateFormat for options.")
      .withRequiredArg
      .describedAs("format")
      .ofType(classOf[String])
    val jmxServiceUrlOpt =
      parser.accepts("jmx-url", "The url to connect to to poll JMX data. See Oracle javadoc for JMXServiceURL for details.")
        .withRequiredArg
        .describedAs("service-url")
        .ofType(classOf[String])
        .defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi")

    val options = parser.parse(args : _*)

    if(options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    val url = new JMXServiceURL(options.valueOf(jmxServiceUrlOpt))
    val interval = options.valueOf(reportingIntervalOpt).intValue
    val attributesWhitelistExists = options.has(attributesOpt)
    val attributesWhitelist = if(attributesWhitelistExists) Some(options.valueOf(attributesOpt).split(",")) else None
    val dateFormatExists = options.has(dateFormatOpt)
    val dateFormat = if(dateFormatExists) Some(new SimpleDateFormat(options.valueOf(dateFormatOpt))) else None
    val jmxc = JMXConnectorFactory.connect(url, null)
    val mbsc = jmxc.getMBeanServerConnection()

    val queries: Iterable[ObjectName] =
      if(options.has(objectNameOpt))
        options.valuesOf(objectNameOpt).map(new ObjectName(_))
      else
        List(null)

    val names = queries.map((name: ObjectName) => asSet(mbsc.queryNames(name, null))).flatten
    val allAttributes: Iterable[(ObjectName, Array[String])] =
      names.map((name: ObjectName) => (name, mbsc.getMBeanInfo(name).getAttributes().map(_.getName)))


    val numExpectedAttributes: Map[ObjectName, Int] =
      attributesWhitelistExists match {
        case true => queries.map((_, attributesWhitelist.get.size)).toMap
        case false => names.map((name: ObjectName) =>
          (name, mbsc.getMBeanInfo(name).getAttributes().map(_.getName).size)).toMap
      }

    // print csv header
    val keys = List("time") ++ queryAttributes(mbsc, names, attributesWhitelist).keys.toArray.sorted
    if(keys.size == numExpectedAttributes.map(_._2).foldLeft(0)(_ + _) + 1)
      println(keys.map("\"" + _ + "\"").mkString(","))

    while(true) {
      val start = System.currentTimeMillis
      val attributes = queryAttributes(mbsc, names, attributesWhitelist)
      attributes("time") = dateFormat match {
        case Some(dFormat) => dFormat.format(new Date)
        case None => System.currentTimeMillis().toString
      }
      if(attributes.keySet.size == numExpectedAttributes.map(_._2).foldLeft(0)(_ + _) + 1)
        println(keys.map(attributes(_)).mkString(","))
      val sleep = max(0, interval - (System.currentTimeMillis - start))
      Thread.sleep(sleep)
    }
  }

  def queryAttributes(mbsc: MBeanServerConnection, names: Iterable[ObjectName], attributesWhitelist: Option[Array[String]]) = {
    var attributes = new mutable.HashMap[String, Any]()
    for(name <- names) {
      val mbean = mbsc.getMBeanInfo(name)
      for(attrObj <- mbsc.getAttributes(name, mbean.getAttributes.map(_.getName))) {
        val attr = attrObj.asInstanceOf[Attribute]
        attributesWhitelist match {
          case Some(allowedAttributes) =>
            if(allowedAttributes.contains(attr.getName))
              attributes(name + ":" + attr.getName) = attr.getValue
          case None => attributes(name + ":" + attr.getName) = attr.getValue
        }
      }
    }
    attributes
  }

}