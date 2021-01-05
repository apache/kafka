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

import java.{lang, util}
import java.util.{Date}
import java.text.SimpleDateFormat

import javax.management._
import javax.management.remote._
import javax.rmi.ssl.SslRMIClientSocketFactory
import joptsimple.{ArgumentAcceptingOptionSpec, OptionParser, OptionSpecBuilder}

import scala.jdk.CollectionConverters._
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

  case class ToolOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val objectNameOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("object-name", "A JMX object name to use as a query. This can contain wild cards, and this option " +
        "can be given multiple times to specify more than one query. If no objects are specified " +
        "all objects will be queried.")
        .withRequiredArg
        .describedAs("name")
        .ofType(classOf[String])
    val attributesOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("attributes", "The list of attributes to include in the query. This is a comma-separated list. If no " +
        "attributes are specified all attributes will be reported.")
        .withRequiredArg
        .describedAs("name")
        .ofType(classOf[String])
    val reportingIntervalOpt: ArgumentAcceptingOptionSpec[Integer] =
      parser.accepts("reporting-interval", "Interval in MS with which to poll jmx stats; default value is 2 seconds. " +
        "Value of -1 equivalent to setting one-time to true")
        .withRequiredArg
        .describedAs("ms")
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(2000)
    val oneTimeOpt: ArgumentAcceptingOptionSpec[lang.Boolean] =
      parser.accepts("one-time", "Flag to indicate run once only.")
        .withRequiredArg
        .describedAs("one-time")
        .ofType(classOf[java.lang.Boolean])
        .defaultsTo(false)
    val dateFormatOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("date-format", "The date format to use for formatting the time field. " +
        "See java.text.SimpleDateFormat for options.")
        .withRequiredArg
        .describedAs("format")
        .ofType(classOf[String])
    val jmxServiceUrlOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("jmx-url", "The url to connect to poll JMX data. See Oracle javadoc for JMXServiceURL for details.")
        .withRequiredArg
        .describedAs("service-url")
        .ofType(classOf[String])
        .defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi")
    val reportFormatOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("report-format", "output format name: either 'original', 'properties', 'csv', 'tsv' ")
        .withRequiredArg
        .describedAs("report-format")
        .ofType(classOf[java.lang.String])
        .defaultsTo("original")
    val jmxAuthPropOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("jmx-auth-prop", "A mechanism to pass property in the form 'username=password' " +
        "when enabling remote JMX with password authentication.")
        .withRequiredArg
        .describedAs("jmx-auth-prop")
        .ofType(classOf[String])
    val jmxSslEnableOpt: ArgumentAcceptingOptionSpec[lang.Boolean] =
      parser.accepts("jmx-ssl-enable", "Flag to enable remote JMX with SSL.")
        .withRequiredArg
        .describedAs("ssl-enable")
        .ofType(classOf[java.lang.Boolean])
        .defaultsTo(false)
    val waitOpt: OptionSpecBuilder = parser.accepts("wait",
      "Wait for requested JMX objects to become available before starting output. " +
      "Each of the given names or patterns must select at least one MBean before reporting starts.")
    val helpOpt: OptionSpecBuilder = parser.accepts("help", "Print usage information.")
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Dump JMX values to standard output.")

    val options = parser.parse(args : _*)

  }

  def main(args: Array[String]): Unit = {
    val toolOptions = ToolOptions(args)
    connectAndExecute(toolOptions) match {
      case Some(false -> msg) => Exit.exit(1, Some(msg))
      case Some(true -> msg) => CommandLineUtils.printUsageAndDie(toolOptions.parser, msg)
      case None =>
    }
  }

  def connectAndExecute(toolOptions: ToolOptions): Option[(Boolean, String)] = {
    if(toolOptions.options.has(toolOptions.helpOpt)) {
      toolOptions.parser.printHelpOn(System.out)
      return None
    } else if (toolOptions.options.valueOf(toolOptions.oneTimeOpt) && toolOptions.options.valueOf(toolOptions.reportingIntervalOpt) != -1) {
      return Some(true, s"--one-time=true is incompatible with --reporting-interval=${toolOptions.options.valueOf(toolOptions.reportingIntervalOpt)}")
    }
    val url = new JMXServiceURL(toolOptions.options.valueOf(toolOptions.jmxServiceUrlOpt))
    val enablePasswordAuth = toolOptions.options.has(toolOptions.jmxAuthPropOpt)
    val enableSsl = toolOptions.options.has(toolOptions.jmxSslEnableOpt)

    var jmxc: JMXConnector = null
    var mbsc: MBeanServerConnection = null
    var connected = false
    val connectTimeoutMs = 10000
    val connectTestStarted = System.currentTimeMillis
    do {
      try {
        Console.err.println(s"Trying to connect to JMX url: $url.")
        val env = new util.HashMap[String, AnyRef]
        // ssl enable
        if (enableSsl) {
          val csf = new SslRMIClientSocketFactory
          env.put("com.sun.jndi.rmi.factory.socket", csf)
        }
        // password authentication enable
        if (enablePasswordAuth) {
          val credentials = toolOptions.options.valueOf(toolOptions.jmxAuthPropOpt).split("=", 2)
          env.put(JMXConnector.CREDENTIALS, credentials)
        }
        jmxc = JMXConnectorFactory.connect(url, env)
        mbsc = jmxc.getMBeanServerConnection
        connected = true
      } catch {
        case e : Exception =>
          Console.err.println(s"Could not connect to JMX url: $url. Exception ${e.getMessage}.")
          e.printStackTrace()
          Thread.sleep(100)
      }
    } while (System.currentTimeMillis - connectTestStarted < connectTimeoutMs && !connected)

    if (!connected) {
      Console.err.println(s"Could not connect to JMX url $url after $connectTimeoutMs ms.")
      return Some(false -> "Exiting.")
    }

    execute(toolOptions, mbsc)
  }

  def execute(toolOptions: ToolOptions, mbsc: MBeanServerConnection, waitTimeoutMs: Long = 10000): Option[(Boolean, String)] = {
    val interval = toolOptions.options.valueOf(toolOptions.reportingIntervalOpt).intValue
    val attributesIncludeExists = toolOptions.options.has(toolOptions.attributesOpt)
    val dateFormatExists = toolOptions.options.has(toolOptions.dateFormatOpt)
    val oneTime = interval < 0 || toolOptions.options.has(toolOptions.oneTimeOpt)
    val attributesInclude = if (attributesIncludeExists) Some(toolOptions.options.valueOf(toolOptions.attributesOpt).split(",").filterNot(_.equals(""))) else None
    val dateFormat = if (dateFormatExists) Some(new SimpleDateFormat(toolOptions.options.valueOf(toolOptions.dateFormatOpt))) else None
    val wait = toolOptions.options.has(toolOptions.waitOpt)
    val reportFormat = parseFormat(toolOptions.options.valueOf(toolOptions.reportFormatOpt).toLowerCase)
    val reportFormatOriginal = reportFormat.equals("original")
    val queries: Iterable[ObjectName] =
      if (toolOptions.options.has(toolOptions.objectNameOpt))
        toolOptions.options.valuesOf(toolOptions.objectNameOpt).asScala.map(new ObjectName(_))
      else
        List(null)

    var selectedNames: Map[ObjectName, Set[ObjectName]] = null
    var foundAllObjects = false

    val start = System.currentTimeMillis
    do {
      if (selectedNames != null) {
        Console.err.println(s"Could not find all object names, retrying")
        Thread.sleep(100)
      }
      selectedNames = queries.map((name: ObjectName) => name -> mbsc.queryNames(name, null).asScala.toSet).toMap
      foundAllObjects = selectedNames.forall{ case(_, ns) => ns.nonEmpty}
    } while (wait && System.currentTimeMillis - start < waitTimeoutMs && !foundAllObjects)

    if (!foundAllObjects) {
      val missing = selectedNames.filter{ case _ -> ns => ns.isEmpty}.keys.mkString(", ")
      val msg = s"Could not find any objects names matching $missing${if (wait) s" after $waitTimeoutMs ms" else ""}."
      Console.err.println(msg)
      return Some(false -> "Exiting.")
    }

    val names = selectedNames.values.flatten
    val numExpectedAttributes: Map[ObjectName, Int] =
      if (!attributesIncludeExists)
        names.map{ name: ObjectName =>
          val mbean = mbsc.getMBeanInfo(name)
          (name, mbsc.getAttributes(name, mbean.getAttributes.map(_.getName)).size)}.toMap
      else {
        names.map { name: ObjectName =>
          val mbean = mbsc.getMBeanInfo(name)
          val attributes = mbsc.getAttributes(name, mbean.getAttributes.map(_.getName))
          val expectedAttributes = attributes.asScala.asInstanceOf[mutable.Buffer[Attribute]]
            .filter(attr => attributesInclude.get.contains(attr.getName))
          (name, expectedAttributes.size)
        }.toMap.filter(_._2 > 0)
      }

    if(numExpectedAttributes.isEmpty) {
      return Some(true -> s"No matched attributes for the queried objects ${queries.mkString(",")}.")
    }

    // print csv header
    val keys = List("time") ++ queryAttributes(mbsc, names, attributesInclude).keys.toArray.sorted
    if(reportFormatOriginal && keys.size == numExpectedAttributes.values.sum + 1) {
      println(keys.map("\"" + _ + "\"").mkString(","))
    }

    var keepGoing = true
    while (keepGoing) {
      val start = System.currentTimeMillis
      val attributes = queryAttributes(mbsc, names, attributesInclude)
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
    None
  }

  def queryAttributes(mbsc: MBeanServerConnection, names: Iterable[ObjectName], attributesInclude: Option[Array[String]]): mutable.Map[String, Any] = {
    val attributes = new mutable.HashMap[String, Any]()
    for (name <- names) {
      val mbean = mbsc.getMBeanInfo(name)
      for (attrObj <- mbsc.getAttributes(name, mbean.getAttributes.map(_.getName)).asScala) {
        val attr = attrObj.asInstanceOf[Attribute]
        attributesInclude match {
          case Some(allowedAttributes) =>
            if (allowedAttributes.contains(attr.getName))
              attributes(name.toString + ":" + attr.getName) = attr.getValue
          case None => attributes(name.toString + ":" + attr.getName) = attr.getValue
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
