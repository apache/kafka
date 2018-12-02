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


import java.lang.management.ManagementFactory
import javax.management.ObjectName

/**
 * If mx4j-tools is in the classpath call maybeLoad to load the HTTP interface of mx4j.
 *
 * The default port is 8082. To override that provide e.g. -Dmx4jport=8083
 * The default listen address is 0.0.0.0. To override that provide -Dmx4jaddress=127.0.0.1
 * This feature must be enabled with -Dmx4jenable=true
 *
 * This is a Scala port of org.apache.cassandra.utils.Mx4jTool written by Ran Tavory for CASSANDRA-1068
 * */
object Mx4jLoader extends Logging {

  def maybeLoad(): Boolean = {
    val props = new VerifiableProperties(System.getProperties())
    if (!props.getBoolean("kafka_mx4jenable", false))
      return false
    val address = props.getString("mx4jaddress", "0.0.0.0")
    val port = props.getInt("mx4jport", 8082)
    try {
      debug("Will try to load MX4j now, if it's in the classpath")

      val mbs = ManagementFactory.getPlatformMBeanServer()
      val processorName = new ObjectName("Server:name=XSLTProcessor")

      val httpAdaptorClass = Class.forName("mx4j.tools.adaptor.http.HttpAdaptor")
      val httpAdaptor = httpAdaptorClass.getDeclaredConstructor().newInstance()
      httpAdaptorClass.getMethod("setHost", classOf[String]).invoke(httpAdaptor, address.asInstanceOf[AnyRef])
      httpAdaptorClass.getMethod("setPort", Integer.TYPE).invoke(httpAdaptor, port.asInstanceOf[AnyRef])

      val httpName = new ObjectName("system:name=http")
      mbs.registerMBean(httpAdaptor, httpName)

      val xsltProcessorClass = Class.forName("mx4j.tools.adaptor.http.XSLTProcessor")
      val xsltProcessor = xsltProcessorClass.getDeclaredConstructor().newInstance()
      httpAdaptorClass.getMethod("setProcessor", Class.forName("mx4j.tools.adaptor.http.ProcessorMBean")).invoke(httpAdaptor, xsltProcessor.asInstanceOf[AnyRef])
      mbs.registerMBean(xsltProcessor, processorName)
      httpAdaptorClass.getMethod("start").invoke(httpAdaptor)
      info("mx4j successfully loaded")
      return true
    }
    catch {
      case _: ClassNotFoundException =>
        info("Will not load MX4J, mx4j-tools.jar is not in the classpath")
      case e: Throwable =>
        warn("Could not start register mbean in JMX", e)
    }
    false
  }
}
