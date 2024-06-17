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
package org.apache.kafka.connect.sink;

import java.util.concurrent.Future;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Component that a {@link SinkTask} can use to report problematic records (and their corresponding problems) as it
 * writes them through {@link SinkTask#put(java.util.Collection)}.
 *
 * @since 2.6
 */
public interface ErrantRecordReporter {

    /**
     * Report a problematic record and the corresponding error to be written to the sink
     * connector's dead letter queue (DLQ).
     * <p>
     * This call is asynchronous and returns a {@link java.util.concurrent.Future Future}.
     * Invoking {@link java.util.concurrent.Future#get() get()} on this future will block until the
     * record has been written or throw any exception that occurred while sending the record.
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method
     * immediately.
     * <p>
     * Connect guarantees that sink records reported through this reporter will be written to the error topic
     * before the framework calls the {@link SinkTask#preCommit(java.util.Map)} method and therefore before
     * committing the consumer offsets. SinkTask implementations can use the Future when stronger guarantees
     * are required.
     *
     * @param record the problematic record; may not be null
     * @param error  the error capturing the problem with the record; may not be null
     * @return a future that can be used to block until the record and error are reported
     *         to the DLQ
     * @throws ConnectException if the error reporter and DLQ fails to write a reported record
     * @since 2.6
     */
    Future<Void> report(SinkRecord record, Throwable error);
}
