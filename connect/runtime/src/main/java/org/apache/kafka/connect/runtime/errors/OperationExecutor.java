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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.common.Configurable;

import java.util.Map;

/**
 * Execute a connect worker operation with configurable retries and tolerances. An operation here means applying tranformations
 * on a ConnectRecord, converting it to a format used by the Connect framework, writing it to a sink or reading from a source.
 *
 * <p/>The following failures will be retried:
 *
 * <ol>
 *     <li> Connector tasks fail with RetriableException when reading/writing records from/to external system.
 *     <li> Transformations fail and throw exceptions while processing records.
 *     <li> Converters fail to correctly serialize/deserialize records.
 *     <li> Exceptions while producing/consuming to/from Kafka topics in the Connect framework.
 * </ol>
 *
 * The executor will report errors on failure (after reattempting). It will throw a {@link ToleranceExceededException} if
 * any of the tolerance limits are crossed.
 */
public abstract class OperationExecutor implements Configurable {

    public <V> V execute(Operation<V> operation, ProcessingContext context) {
        return execute(operation, null, context);
    }

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    public void configure(Map<String, ?> configs) {
    }

    public abstract <V> V execute(Operation<V> operation, V value, ProcessingContext context);

    public interface Operation<V> {
        V apply() throws Exception;
    }

}
