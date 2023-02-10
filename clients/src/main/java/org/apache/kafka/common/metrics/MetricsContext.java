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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * MetricsContext encapsulates additional contextLabels about metrics exposed via a
 * {@link org.apache.kafka.common.metrics.MetricsReporter}
 *
 * <p>The {@link #contextLabels()} map provides following information:
 * <dl>
 *   <dt>in all components</dt>
 *   <dd>a {@code _namespace} field indicating the component exposing metrics
 *   e.g. kafka.server, kafka.consumer.
 *   The {@link JmxReporter} uses this as prefix for MBean names</dd>
 *
 *   <dt>for clients and streams libraries</dt>
 *   <dd>any freeform fields passed in via
 *   client properties in the form of {@code metrics.context.<key>=<value>}</dd>
 *
 *   <dt>for kafka brokers</dt>
 *   <dd>kafka.broker.id, kafka.cluster.id</dd>
 *
 *   <dt>for connect workers</dt>
 *   <dd>connect.kafka.cluster.id, connect.group.id</dd>
 * </dl>
 */
@InterfaceStability.Evolving
public interface MetricsContext {
    /* predefined fields */
    String NAMESPACE = "_namespace"; // metrics namespace, formerly jmx prefix

    /**
     * Returns the labels for this metrics context.
     *
     * @return the map of label keys and values; never null but possibly empty
     */
    Map<String, String> contextLabels();
}
