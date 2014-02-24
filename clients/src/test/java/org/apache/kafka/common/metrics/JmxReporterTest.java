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
package org.apache.kafka.common.metrics;


import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Total;
import org.junit.Test;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() throws Exception {
        Metrics metrics = new Metrics();
        metrics.addReporter(new JmxReporter());
        Sensor sensor = metrics.sensor("kafka.requests");
        sensor.add("pack.bean1.avg", new Avg());
        sensor.add("pack.bean2.total", new Total());
        Sensor sensor2 = metrics.sensor("kafka.blah");
        sensor2.add("pack.bean1.some", new Total());
        sensor2.add("pack.bean2.some", new Total());
    }
}
