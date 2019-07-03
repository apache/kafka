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

import java.util.{Date, Objects}
import java.text.SimpleDateFormat
import javax.management._
import javax.management.remote._
import javax.rmi.ssl.SslRMIClientSocketFactory

import joptsimple.OptionParser

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math._
import kafka.utils.{CommandLineUtils, Exit, Logging}


/**
  * A program for reading JMX metrics from a given endpoint.
  *
  * This tool only works reliably if the JmxServer is fully initialized prior to invoking the tool. See KAFKA-4620 for
  * details.
  */
object JmxTool extends Logging {

  def main(args: Array[String]) {
    // Parse command line
    val parser = new OptionParser(false)
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
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval in MS with which to poll jmx stats; default value is 2 seconds. " +
      "Value of -1 equivalent to setting one-time to true")
      .withRequiredArg
      .describedAs("ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(2000)
    val oneTimeOpt = parser.accepts("one-time", "Flag to indicate run once only.")
      .withRequiredArg
      .describedAs("one-time")
      .ofType(classOf[java.lang.Boolean])
      .defaultsTo(false)
    val dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
      "See java.text.SimpleDateFormat for options.")
      .withRequiredArg
      .describedAs("format")
      .ofType(classOf[String])
    val jmxServiceUrlOpt =
      parser.accepts("jmx-url", "The url to connect to poll JMX data. See Oracle javadoc for JMXServiceURL for details.")
        .withRequiredArg
        .describedAs("service-url")
        .ofType(classOf[String])
        .defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi")
    val reportFormatOpt = parser.accepts("report-format", "output format name: either 'original', 'properties', 'csv', 'tsv' ")
      .withRequiredArg
      .describedAs("report-format")
      .ofType(classOf[java.lang.String])
      .defaultsTo("original")
    val jmxAuthPropOpt = parser.accepts("jmx-auth-prop", "A mechanism to pass property in the form 'username=password' " +
      "when enabling remote JMX with password authentication.")
      .withRequiredArg
      .describedAs("jmx-auth-prop")
      .ofType(classOf[String])
    val jmxSslEnableOpt = parser.accepts("jmx-ssl-enable", "Flag to enable remote JMX with SSL.")
      .withRequiredArg
      .describedAs("ssl-enable")
      .ofType(classOf[java.lang.Boolean])
      .defaultsTo(false)
    val waitOpt = parser.accepts("wait", "Wait for requested JMX objects to become available before starting output. " +
      "Only supported when the list of objects is non-empty and contains no object name patterns.")
    val helpOpt = parser.accepts("help", "Print usage information.")


    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Dump JMX values to standard output.")

    val options = parser.parse(args : _*)

    if(options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      Exit.exit(0)
    }

    val url = new JMXServiceURL(options.valueOf(jmxServiceUrlOpt))
    val interval = options.valueOf(reportingIntervalOpt).intValue
    val oneTime = interval < 0 || options.has(oneTimeOpt)
    val attributesWhitelistExists = options.has(attributesOpt)
    val attributesWhitelist = if(attributesWhitelistExists) Some(options.valueOf(attributesOpt).split(",").filterNot(_.equals(""))) else None
    val dateFormatExists = options.has(dateFormatOpt)
    val dateFormat = if(dateFormatExists) Some(new SimpleDateFormat(options.valueOf(dateFormatOpt))) else None
    val wait = options.has(waitOpt)

    val reportFormat = parseFormat(options.valueOf(reportFormatOpt).toLowerCase)
    val reportFormatOriginal = reportFormat.equals("original")

    val enablePasswordAuth = options.has(jmxAuthPropOpt)
    val enableSsl = options.has(jmxSslEnableOpt)

    var jmxc: JMXConnector = null
    var mbsc: MBeanServerConnection = null
    var connected = false
    val connectTimeoutMs = 10000
    val connectTestStarted = System.currentTimeMillis
    do {
      try {
        System.err.println(s"Trying to connect to JMX url: $url.")
        val env = new java.util.HashMap[String, AnyRef]
        // ssl enable
        if (enableSsl) {
          val csf = new SslRMIClientSocketFactory
          env.put("com.sun.jndi.rmi.factory.socket", csf)
        }
        // password authentication enable
        if (enablePasswordAuth) {
          val credentials = options.valueOf(jmxAuthPropOpt).split("=", 2)
          env.put(JMXConnector.CREDENTIALS, credentials)
        }
        jmxc = JMXConnectorFactory.connect(url, env)
        mbsc = jmxc.getMBeanServerConnection
        connected = true
      } catch {
        case e : Exception =>
          System.err.println(s"Could not connect to JMX url: $url. Exception ${e.getMessage}.")
          e.printStackTrace()
          Thread.sleep(100)
      }
    } while (System.currentTimeMillis - connectTestStarted < connectTimeoutMs && !connected)

    if (!connected) {
      System.err.println(s"Could not connect to JMX url $url after $connectTimeoutMs ms.")
      System.err.println("Exiting.")
      sys.exit(1)
    }

    val queries: Iterable[ObjectName] =
      if(options.has(objectNameOpt))
        options.valuesOf(objectNameOpt).asScala.map(new ObjectName(_))
      else
        List(null)

    val hasPatternQueries = queries.filterNot(Objects.isNull).exists((name: ObjectName) => name.isPattern)

    var names: Iterable[ObjectName] = null
    def namesSet = Option(names).toSet.flatten
    def foundAllObjects = queries.toSet == namesSet
    val waitTimeoutMs = 10000
    if (!hasPatternQueries) {
      val start = System.currentTimeMillis
      do {
        if (names != null) {
          System.err.println("Could not find all object names, retrying")
          Thread.sleep(100)
        }
        names = queries.flatMap((name: ObjectName) => mbsc.queryNames(name, null).asScala)
      } while (wait && System.currentTimeMillis - start < waitTimeoutMs && !foundAllObjects)
    }

    if (wait && !foundAllObjects) {
      val missing = (queries.toSet - namesSet).mkString(", ")
      System.err.println(s"Could not find all requested object names after $waitTimeoutMs ms. Missing $missing")
      System.err.println("Exiting.")
      sys.exit(1)
    }

    val numExpectedAttributes: Map[ObjectName, Int] =
      if (!attributesWhitelistExists)
        names.map{name: ObjectName =>
          val mbean = mbsc.getMBeanInfo(name)
          (name, mbsc.getAttributes(name, mbean.getAttributes.map(_.getName)).size)}.toMap
      else {
        if (!hasPatternQueries)
          names.map{name: ObjectName =>
            val mbean = mbsc.getMBeanInfo(name)
            val attributes = mbsc.getAttributes(name, mbean.getAttributes.map(_.getName))
            val expectedAttributes = attributes.asScala.asInstanceOf[mutable.Buffer[Attribute]]
              .filter(attr => attributesWhitelist.get.contains(attr.getName))
            (name, expectedAttributes.size)}.toMap.filter(_._2 > 0)
        else
          queries.map((_, attributesWhitelist.get.length)).toMap
      }

    if(numExpectedAttributes.isEmpty) {
      CommandLineUtils.printUsageAndDie(parser, s"No matched attributes for the queried objects $queries.")
    }

    // print csv header
    val keys = List("time") ++ queryAttributes(mbsc, names, attributesWhitelist).keys.toArray.sorted
    if(reportFormatOriginal && keys.size == numExpectedAttributes.values.sum + 1) {
      println(keys.map("\"" + _ + "\"").mkString(","))
    }

    var keepGoing = true
    while (keepGoing) {
      val start = System.currentTimeMillis
      val attributes = queryAttributes(mbsc, names, attributesWhitelist)
      attributes("time") = dateFormat match {
        case Some(dFormat) => dFormat.format(new Date)
        case None => System.currentTimeMillis().toString
      }
      if(attributes.keySet.size == numExpectedAttributes.values.sum + 1) {
        if(reportFormatOriginal) {
          println(keys.map(attributes(_)).mkString(","))
        }
        else if(reportFormat.equals("properties")) {
          keys.foreach( k => { println(k + "=" + attributes(k) ) } )
        }
        else if(reportFormat.equals("csv")) {
          keys.foreach( k => { println(k + ",\"" + attributes(k) + "\"" ) } )
        }
        else { // tsv
          keys.foreach( k => { println(k + "\t" + attributes(k) ) } )
        }
      }

      if (oneTime) {
        keepGoing = false
      }
      else {
        val sleep = max(0, interval - (System.currentTimeMillis - start))
        Thread.sleep(sleep)
      }
    }
  }

  def queryAttributes(mbsc: MBeanServerConnection, names: Iterable[ObjectName], attributesWhitelist: Option[Array[String]]): mutable.Map[String, Any] = {
    val attributes = new mutable.HashMap[String, Any]()
    for (name <- names) {
      val mbean = mbsc.getMBeanInfo(name)
      for (attrObj <- mbsc.getAttributes(name, mbean.getAttributes.map(_.getName)).asScala) {
        val attr = attrObj.asInstanceOf[Attribute]
        attributesWhitelist match {
          case Some(allowedAttributes) =>
            if (allowedAttributes.contains(attr.getName))
              attributes(name + ":" + attr.getName) = attr.getValue
          case None => attributes(name + ":" + attr.getName) = attr.getValue
        }
      }
    }
    attributes
  }

  def parseFormat(reportFormatOpt : String): String = reportFormatOpt match {
    case "properties" => "properties"
    case "csv" => "csv"
    case "tsv" => "tsv"
    case _ => "original"
  }
}
