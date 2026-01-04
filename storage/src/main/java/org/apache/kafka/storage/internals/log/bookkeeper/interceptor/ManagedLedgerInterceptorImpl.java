/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kafka.storage.internals.log.bookkeeper.interceptor;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String INDEX = "index";
    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    private final AppendIndexMetadataInterceptor appendIndexMetadataInterceptor;
    private final Set<ManagedLedgerPayloadProcessor.Processor> inputProcessors;
    private final Set<ManagedLedgerPayloadProcessor.Processor> outputProcessors;

    public ManagedLedgerInterceptorImpl(Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
                                        Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;

        // save appendIndexMetadataInterceptor to field
        AppendIndexMetadataInterceptor appendIndexMetadataInterceptor = null;

        for (BrokerEntryMetadataInterceptor interceptor : this.brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendIndexMetadataInterceptor) {
                appendIndexMetadataInterceptor = (AppendIndexMetadataInterceptor) interceptor;
                break;
            }
        }

        this.appendIndexMetadataInterceptor = appendIndexMetadataInterceptor;

        if (brokerEntryPayloadProcessors != null) {
            this.inputProcessors = new LinkedHashSet<>();
            this.outputProcessors = new LinkedHashSet<>();
            for (ManagedLedgerPayloadProcessor processor : brokerEntryPayloadProcessors) {
                this.inputProcessors.add(processor.inputProcessor());
                this.outputProcessors.add(processor.outputProcessor());
            }
        } else {
            this.inputProcessors = null;
            this.outputProcessors = null;
        }
    }

    public long getIndex() {
        long index = -1;

        if (appendIndexMetadataInterceptor != null) {
            return appendIndexMetadataInterceptor.getIndex();
        }

        return index;
    }

    @Override
    public void beforeAddEntry(AddEntryOperation op, int numberOfMessages) {
       if (op == null || numberOfMessages <= 0) {
           return;
       }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors, numberOfMessages));
    }

    @Override
    public void afterFailedAddEntry(int numberOfMessages) {
        if (appendIndexMetadataInterceptor != null) {
            appendIndexMetadataInterceptor.decreaseWithNumberOfMessages(numberOfMessages);
        }
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.size() == 0) {
            return;
        }

        if (propertiesMap.containsKey(INDEX)) {
            if (appendIndexMetadataInterceptor != null) {
                appendIndexMetadataInterceptor.recoveryIndexGenerator(
                        Long.parseLong(propertiesMap.get(INDEX)));
            }
        }
    }

    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LastEntryHandle lh) {
        return lh.readLastEntryAsync().thenAccept(lastEntryOptional -> {
            if (lastEntryOptional.isPresent()) {
                Entry lastEntry = lastEntryOptional.get();
                try {
                    Commands.peekBrokerEntryMetadataAndConsume(lastEntry.getDataBuffer(), brokerEntryMetadata -> {
                        if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
                            appendIndexMetadataInterceptor.recoveryIndexGenerator(brokerEntryMetadata.getIndex());
                        }
                    });
                } finally {
                    lastEntry.release();
                }
            }
        });
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        if (appendIndexMetadataInterceptor != null) {
            propertiesMap.put(INDEX, String.valueOf(appendIndexMetadataInterceptor.getIndex()));
        }
    }

    private PayloadProcessorHandle processPayload(Set<ManagedLedgerPayloadProcessor.Processor> processors,
                                                  Object context, ByteBuf payload) {

        ByteBuf tmpData = payload;
        final Set<ImmutablePair<ManagedLedgerPayloadProcessor.Processor, ByteBuf>> processedSet = new LinkedHashSet<>();
        for (ManagedLedgerPayloadProcessor.Processor payloadProcessor : processors) {
            if (payloadProcessor != null) {
                tmpData = payloadProcessor.process(context, tmpData);
                processedSet.add(new ImmutablePair<>(payloadProcessor, tmpData));
            }
        }
        final ByteBuf dataToReturn = tmpData;
        return new PayloadProcessorHandle() {
            @Override
            public ByteBuf getProcessedPayload() {
                return dataToReturn;
            }

            @Override
            public void release() {
                for (ImmutablePair<ManagedLedgerPayloadProcessor.Processor, ByteBuf> p : processedSet) {
                    p.left.release(p.right);
                }
                processedSet.clear();
            }
        };
    }
    @Override
    public PayloadProcessorHandle processPayloadBeforeLedgerWrite(Object ctx, ByteBuf ledgerData) {
        if (this.inputProcessors == null || this.inputProcessors.size() == 0) {
            return null;
        }
        return processPayload(this.inputProcessors, ctx, ledgerData);
    }

    @Override
    public PayloadProcessorHandle processPayloadBeforeEntryCache(ByteBuf ledgerData){
        if (this.outputProcessors == null || this.outputProcessors.size() == 0) {
            return null;
        }
        return processPayload(this.outputProcessors, null, ledgerData);
    }
}
