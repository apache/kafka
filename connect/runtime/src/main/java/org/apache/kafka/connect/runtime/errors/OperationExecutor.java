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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Execute a recoverable operation in the Connector pipeline.
 */
public interface OperationExecutor extends Configurable {

    /**
     * Execute the recoverable operation
     *
     * @param operation the recoverable operation
     * @param stage stage of operation
     * @param <V> return type of the result of the operation.
     * @return result of the operation
     */
    <V> V execute(Operation<V> operation, Stage stage, Class<?> executingClass);

    void sourceRecord(SourceRecord preTransformRecord);

    void consumerRecord(ConsumerRecord<byte[], byte[]> consumedMessage);

    boolean failed();
}
