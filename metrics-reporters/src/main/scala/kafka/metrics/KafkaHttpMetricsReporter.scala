/**
 *
 *
 *
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

package kafka.metrics

import java.net.InetSocketAddress

import com.yammer.metrics.reporting._
import kafka.utils.{Logging, VerifiableProperties}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}


private trait KafkaHttpMetricsReporterMBean extends KafkaMetricsReporterMBean

private class KafkaHttpMetricsServer extends Logging {

  private var server: Server = null

  def init(bindAddress: String, port: Int) {
    info("Initializing Kafka Http Metrics Reporter")

    // creating the socket address for binding to the specified address and port
    val inetSocketAddress: InetSocketAddress = new InetSocketAddress(bindAddress, port)

    // create new Jetty server
    server = new Server(inetSocketAddress)

    // creating the servlet context handler
    val servletContextHandler = new ServletContextHandler
    servletContextHandler.addEventListener(new MetricsServletContextListener)
    servletContextHandler.setContextPath("/*")

    servletContextHandler.addServlet(new ServletHolder(new AdminServlet()), "/monitor")
    servletContextHandler.addServlet(new ServletHolder(new MetricsServlet), "/monitor/metrics")
    servletContextHandler.addServlet(new ServletHolder(new HealthCheckServlet), "/monitor/healthcheck")
    servletContextHandler.addServlet(new ServletHolder(new ThreadDumpServlet), "/monitor/threads")
    server.setHandler(servletContextHandler)

    // adding the configured servlet context handler to the Jetty Server
    server.setHandler(servletContextHandler)
    info("Started Kafka Http Metrics server on: " + bindAddress + ":" + port)
  }

  def start() {
    try {
      server.start()
    }
    catch {
      case e: Exception => error("Cannot start KafkaHttpMetricsServer", e)
    }
  }

  def shutdown() {
    try {
      info("Stopping Kafka Http Metrics Server")
      server.stop()
    }
    catch {
      case e: Exception => error("Cannot shutdown KafkaHttpMetricsServer", e)
    }
  }

}

private class KafkaHttpMetricsReporter extends KafkaMetricsReporter
                              with KafkaHttpMetricsReporterMBean
                              with Logging {

  private var metricsServer: KafkaHttpMetricsServer = null
  private var running = false
  private var initialized = false
  private var bindAddress: String = null
  private var port = 0

  override def getMBeanName = "kafka:type=kafka.metrics.KafkaHttpMetricsReporter"

  override def init(props: VerifiableProperties) {

    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaMetricsConfig(props)
        bindAddress = props.getString("kafka.http.metrics.host", "localhost")
        port = props.getInt("kafka.http.metrics.port")
        metricsServer = new KafkaHttpMetricsServer()
        metricsServer.init(bindAddress, port)
        if (props.getBoolean("kafka.http.metrics.reporter.enabled", default = false)) {
          initialized = true
          startReporter(metricsConfig.pollingIntervalSecs)
        } else {
          error("Kafka Http Metrics Reporter already initialized")
        }
      }
    }
  }

  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        metricsServer.start()
        running = true
      }
    }
  }

  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        metricsServer.shutdown()
        running = false
        metricsServer = new KafkaHttpMetricsServer()
        metricsServer.init(bindAddress, port)
      }
    }
  }

}
