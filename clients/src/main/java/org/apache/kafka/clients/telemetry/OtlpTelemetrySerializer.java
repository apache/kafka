/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.telemetry;

import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class OtlpTelemetrySerializer implements TelemetrySerializer {

    @Override
    public void serialize(Collection<TelemetryMetric> telemetryMetrics, OutputStream out)
        throws IOException {
        InstrumentationLibraryMetrics.Builder instrumentationLibraryMetrics = InstrumentationLibraryMetrics.newBuilder();

        for (TelemetryMetric telemetryMetric : telemetryMetrics) {
            NumberDataPoint numberDataPoint = NumberDataPoint.newBuilder()
                .setAsInt(telemetryMetric.value())
                .build();

            Metric.Builder builder = Metric.newBuilder()
                .setName(telemetryMetric.metricName().name());

            switch (telemetryMetric.metricType()) {
                case gauge:
                    Gauge gauge = Gauge.newBuilder()
                        .addDataPoints(numberDataPoint)
                        .build();
                    builder.setGauge(gauge);

                    break;

                case histogram:
                    // TODO: TELEMETRY_TODO: we should figure out histograms at some point.
                    Histogram histogram = Histogram.newBuilder()
                        .build();
                    builder.setHistogram(histogram);

                    break;

                case sum:
                    Sum sum = Sum.newBuilder()
                        .addDataPoints(numberDataPoint)
                        .build();
                    builder.setSum(sum);

                    break;

                default:
                    throw new InvalidMetricTypeException("");
            }

            Metric metric = builder.build();
            instrumentationLibraryMetrics.addMetrics(metric);
        }

        ResourceMetrics.Builder resourceMetrics = ResourceMetrics.newBuilder();
        resourceMetrics.addInstrumentationLibraryMetrics(instrumentationLibraryMetrics.build());
        resourceMetrics.build().writeTo(out);
    }

}
