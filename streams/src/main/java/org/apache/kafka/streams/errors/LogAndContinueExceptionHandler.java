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
package org.apache.kafka.streams.errors;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogAndContinueExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {

        log.warn("Deserialization exception {}. Processor context is {} and record is {}",
            exception.toString(), context, record);

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // ignore
    }
}
