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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValueJsonConverter;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public class TransactionLogMessageFormatter implements MessageFormatter {

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        Optional.ofNullable(consumerRecord.key())
                .ifPresent(key -> {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(key);
                    short version = byteBuffer.getShort();
                    if (version >= TransactionLogKey.LOWEST_SUPPORTED_VERSION
                            && version <= TransactionLogKey.HIGHEST_SUPPORTED_VERSION) {
                        byte[] value = consumerRecord.value();
                        TransactionLogValue transactionLogValue = 
                                new TransactionLogValue(new ByteBufferAccessor(ByteBuffer.wrap(value)), version);
                        try {
                            output.write(TransactionLogValueJsonConverter.write(transactionLogValue, version).toString().getBytes());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        try {
                            output.write(("unknown::version=" + version + "\n").getBytes());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }
}
