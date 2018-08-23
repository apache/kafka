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

import kafka.utils.{VerifiableProperties, CoreUtils}

object KafkaMetricsConfig {

  val KafkaMetricsReporterClassesProp = "kafka.metrics.reporters"
  val KafkaMetricsPollingIntervalSecondsProp = "kafka.metrics.polling.interval.secs"

  val KafkaMetricsReporterClassesDoc: String = "A list of classes to use as Yammer metrics custom reporters." +
    " The reporters should implement <code>kafka.metrics.KafkaMetricsReporter</code> trait. If a client wants" +
    " to expose JMX operations on a custom reporter, the custom reporter needs to additionally implement an MBean" +
    " trait that extends <code>kafka.metrics.KafkaMetricsReporterMBean</code> trait so that the registered MBean is compliant with" +
    " the standard MBean convention."

  val KafkaMetricsPollingIntervalSecondsDoc: String = s"The metrics polling interval (in seconds) which can be used" +
    s" in $KafkaMetricsReporterClassesProp implementations."
  val KafkaMetricsPollingIntervalSeconds = 10
}

class KafkaMetricsConfig(props: VerifiableProperties) {

  /**
   * Comma-separated list of reporter types. These classes should be on the
   * classpath and will be instantiated at run-time.
   */
  val reporters = CoreUtils.parseCsvList(props.getString(KafkaMetricsConfig.KafkaMetricsReporterClassesProp, ""))

  /**
   * The metrics polling interval (in seconds).
   */
  val pollingIntervalSecs = props.getInt(KafkaMetricsConfig.KafkaMetricsPollingIntervalSecondsProp,
    KafkaMetricsConfig.KafkaMetricsPollingIntervalSeconds)
}
