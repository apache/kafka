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
package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MockApiProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private final ArrayList<Record<KIn, VIn>> processed = new ArrayList<>();
    private final Map<KIn, ValueAndTimestamp<VIn>> lastValueAndTimestampPerKey = new HashMap<>();

    private final ArrayList<Long> punctuatedStreamTime = new ArrayList<>();
    private final ArrayList<Long> punctuatedSystemTime = new ArrayList<>();

    private Cancellable scheduleCancellable;

    private final PunctuationType punctuationType;
    private final long scheduleInterval;

    private boolean commitRequested = false;
    private ProcessorContext<KOut, VOut> context;

    public MockApiProcessor(final PunctuationType punctuationType,
                            final long scheduleInterval) {
        this.punctuationType = punctuationType;
        this.scheduleInterval = scheduleInterval;
    }

    public MockApiProcessor() {
        this(PunctuationType.STREAM_TIME, -1);
    }

    @Override
    public void init(final ProcessorContext<KOut, VOut> context) {
        this.context = context;
        if (scheduleInterval > 0L) {
            scheduleCancellable = context.schedule(
                Duration.ofMillis(scheduleInterval),
                punctuationType,
                (punctuationType == PunctuationType.STREAM_TIME ? punctuatedStreamTime : punctuatedSystemTime)::add
            );
        }
    }

    @Override
    public void process(final Record<KIn, VIn> record) {
        final KIn key = record.key();
        final VIn value = record.value();
        final KeyValueTimestamp<KIn, VIn> keyValueTimestamp = new KeyValueTimestamp<>(key, value, record.timestamp());

        if (value != null) {
            lastValueAndTimestampPerKey.put(key, ValueAndTimestamp.make(value, record.timestamp()));
        } else {
            lastValueAndTimestampPerKey.remove(key);
        }

        processed.add(record);

        if (commitRequested) {
            context.commit();
            commitRequested = false;
        }
    }

    public void checkAndClearProcessResult(final KeyValueTimestamp<?, ?>... expected) {
        assertThat("the number of outputs:" + processed, processed.size(), is(expected.length));
        for (int i = 0; i < expected.length; i++) {
            final Record<KIn, VIn> record = processed.get(i);
            assertThat(
                "output[" + i + "]:",
                new KeyValueTimestamp<>(record.key(), record.value(), record.timestamp()),
                is(expected[i])
            );
        }

        processed.clear();
    }

    public void checkAndClearProcessedRecords(final Record<?, ?>... expected) {
        assertThat("the number of outputs:" + processed, processed.size(), is(expected.length));
        for (int i = 0; i < expected.length; i++) {
            assertThat("output[" + i + "]:", processed.get(i), is(expected[i]));
        }

        processed.clear();
    }

    public void requestCommit() {
        commitRequested = true;
    }

    public void checkEmptyAndClearProcessResult() {
        assertThat("the number of outputs:", processed.size(), is(0));
        processed.clear();
    }

    public void checkAndClearPunctuateResult(final PunctuationType type, final long... expected) {
        final ArrayList<Long> punctuated = type == PunctuationType.STREAM_TIME ? punctuatedStreamTime : punctuatedSystemTime;
        assertThat("the number of outputs:", punctuated.size(), is(expected.length));

        for (int i = 0; i < expected.length; i++) {
            assertThat("output[" + i + "]:", punctuated.get(i), is(expected[i]));
        }

        processed.clear();
    }

    public void addProcessorMetadata(final String key, final long value) {
        if (context instanceof InternalProcessorContext) {
            ((InternalProcessorContext<KOut, VOut>) context).addProcessorMetadataKeyValue(key, value);
        }
    }

    public ArrayList<KeyValueTimestamp<KIn, VIn>> processed() {
        return processed
            .stream()
            .map(r -> new KeyValueTimestamp<>(r.key(), r.value(), r.timestamp()))
            .collect(Collectors.toCollection(ArrayList::new));
    }

    public Map<KIn, ValueAndTimestamp<VIn>> lastValueAndTimestampPerKey() {
        return lastValueAndTimestampPerKey;
    }

    public List<Long> punctuatedStreamTime() {
        return punctuatedStreamTime;
    }

    public Cancellable scheduleCancellable() {
        return scheduleCancellable;
    }

    public ProcessorContext<KOut, VOut> context() {
        return context;
    }

    public void context(final ProcessorContext<KOut, VOut> context) {
        this.context = context;
    }
}
