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
package org.apache.kafka.connect.transforms.predicates;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * <p>A predicate on records.
 * Predicates can be used to conditionally apply a {@link org.apache.kafka.connect.transforms.Transformation}
 * by configuring the transformation's {@code predicate} (and {@code negate}) configuration parameters.
 * In particular, the {@code Filter} transformation can be conditionally applied in order to filter
 * certain records from further processing.
 *
 * <p>Kafka Connect may discover implementations of this interface using the Java {@link java.util.ServiceLoader} mechanism.
 * To support this, implementations of this interface should also contain a service provider configuration file in
 * {@code META-INF/services/org.apache.kafka.connect.transforms.predicates.Predicate}.
 *
 * @param <R> The type of record.
 */
public interface Predicate<R extends ConnectRecord<R>> extends Configurable, AutoCloseable {

    /**
     * Configuration specification for this predicate.
     *
     * @return the configuration definition for this predicate; never null
     */
    ConfigDef config();

    /**
     * Returns whether the given record satisfies this predicate.
     *
     * @param record the record to evaluate; may not be null
     * @return true if the predicate matches, or false otherwise
     */
    boolean test(R record);

    @Override
    void close();
}
