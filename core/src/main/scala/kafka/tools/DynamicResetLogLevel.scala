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

import javax.management.remote.{JMXConnector, JMXConnectorFactory, JMXServiceURL}
import javax.management.{MBeanServerConnection, ObjectName}
import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, Exit}

/**
 * A program for dynamically alter log4j levels at runtime.
 */
object DynamicResetLogLevel {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val jmxServiceUrlOpt =
      parser.accepts("jmx-url", "The url to connect to JMX. See Oracle javadoc for JMXServiceURL for details.")
        .withRequiredArg
        .describedAs("service-url")
        .ofType(classOf[String])
        .defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi")
    val mbeanNameOpt =
      parser.accepts("mbean-name", "The MBean managed by JMX.")
        .withRequiredArg
        .describedAs("mbean-name")
        .ofType(classOf[String])
        .defaultsTo("kafka:type=kafka.Log4jController")
    val logNameOpt =
      parser.accepts("logName", "The loggerName  of resetting log level.")
        .withRequiredArg
        .describedAs("logName")
        .ofType(classOf[String])
    val logLevelOpt =
      parser.accepts("logLevel", "To set log level.")
        .withRequiredArg
        .describedAs("logLevel")
        .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print usage information.")

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Dynamically alter log4j levels.")

    val options = parser.parse(args: _*)
    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      Exit.exit(0)
    }

    val logName = options.valueOf(logNameOpt)
    val logLevel = options.valueOf(logLevelOpt)
    val jmxServiceUrl = options.valueOf(jmxServiceUrlOpt)
    val mbeanName = options.valueOf(mbeanNameOpt)

    val url = new JMXServiceURL(jmxServiceUrl)
    val mbeanNameObject: ObjectName = new ObjectName(mbeanName)
    var jmxc: JMXConnector = null
    var mbsc: MBeanServerConnection = null

    try {
      jmxc = JMXConnectorFactory.connect(url, null)
      mbsc = jmxc.getMBeanServerConnection
    } catch {
      case e: Throwable =>
        System.err.println(s"Could not connect to JMX url: $url. Exception ${e.getMessage}.")
    }

    try {
      val logReSetResult = mbsc.invoke(mbeanNameObject, "setLogLevel", Array[AnyRef](logName, logLevel), Array[String]("java.lang.String", "java.lang.String"))
      val logLevelAfterChanged = mbsc.invoke(mbeanNameObject, "getLogLevel", Array[AnyRef](logName), Array[String]("java.lang.String"))
      println(s"The result of the setLogLevel is ${logReSetResult}, the logLevel of LogName ${logName}  is ${logLevelAfterChanged} after changing. ")
    } catch {
      case e: Throwable =>
        println(s"The invoke throw exception ${e.getMessage}")
    }
  }
}
