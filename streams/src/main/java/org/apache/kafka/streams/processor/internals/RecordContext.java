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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.Processor;

/**
 * The context associated with the current record being processed by
 * an {@link Processor}
 */
public interface RecordContext {
    /**
     * @return The offset of the original record received from Kafka
     */
    long offset();

    /**
     * @return The timestamp extracted from the record received from Kafka
     */
    long timestamp();

    /**
     * @return The topic the record was received on
     */
    String topic();

    /**
     * @return The partition the record was received on
     */
    int partition();
}
