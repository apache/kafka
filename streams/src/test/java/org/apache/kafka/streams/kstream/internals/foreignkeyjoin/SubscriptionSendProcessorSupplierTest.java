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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.apache.kafka.test.MockInternalNewProcessorContext;
import org.junit.Test;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ResponseJoinProcessorSupplierTest.getDroppedRecordsRateMetric;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ResponseJoinProcessorSupplierTest.getDroppedRecordsTotalMetric;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class SubscriptionSendProcessorSupplierTest {

    // Left join tests
    final Processor<String, Change<LeftValue>, String, SubscriptionWrapper<String>> leftJoinProcessor =
        new SubscriptionSendProcessorSupplier<String, String, LeftValue>(
            LeftValue::getForeignKey,
            () -> "subscription-topic-fk",
            () -> "value-serde-topic",
            Serdes.String(),
            new LeftValueSerializer(),
            true
        ).get();

    final Processor<String, Change<LeftValue>, String, SubscriptionWrapper<String>> innerJoinProcessor =
        new SubscriptionSendProcessorSupplier<String, String, LeftValue>(
            LeftValue::getForeignKey,
            () -> "subscription-topic-fk",
            () -> "value-serde-topic",
            Serdes.String(),
            new LeftValueSerializer(),
            false
        ).get();


    @Test
    public void leftJoinShouldPropagateNewPrimaryKeyWithNonNullFK() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue("merchant1");

        leftJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, null), 0));

        assertThat(context.forwarded().size(), is(1));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>("merchant1", new SubscriptionWrapper<>(hash(leftRecordValue), PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, "product1", 0), 0))
        );
    }

    @Test
    public void leftJoinShouldPropagateNewPrimaryKeyWithNullFK() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue(null);

        leftJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, null), 0));

        assertThat(context.forwarded().size(), is(1));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>(null, new SubscriptionWrapper<>(hash(leftRecordValue), PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, "product1", 0), 0))
        );
    }

    @Test
    public void leftJoinShouldPropagateChangeOfFKFromNonNullToNonNullValue() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue("merchant2");

        leftJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, new LeftValue("merchant1")), 0));

        assertThat(context.forwarded().size(), is(2));
        assertThat(
            context.forwarded().get(1).record(),
            is(new Record<>("merchant2", new SubscriptionWrapper<>(hash(leftRecordValue), PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, "product1", 0), 0))
        );
    }

    @Test
    public void leftJoinShouldPropagateChangeOfFKFromNonNullToNullValue() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue(null);

        leftJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, new LeftValue("merchant1")), 0));

        assertThat(context.forwarded().size(), greaterThan(0));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>("merchant1", new SubscriptionWrapper<>(hash(leftRecordValue), DELETE_KEY_AND_PROPAGATE, "product1", 0), 0))
        );
    }

    @Test
    public void leftJoinShouldPropagateChangeFromNullFKToNullFKValue() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue(null);

        leftJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, leftRecordValue), 0));

        assertThat(context.forwarded().size(), is(1));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>(null, new SubscriptionWrapper<>(hash(leftRecordValue), PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, "product1", 0), 0))
        );
    }

    @Test
    public void leftJoinShouldPropagateDeletionOfAPrimaryKey() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        leftJoinProcessor.process(new Record<>("product1", new Change<>(null, new LeftValue("merchant1")), 0));

        assertThat(context.forwarded().size(), greaterThan(0));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>("merchant1", new SubscriptionWrapper<>(null, DELETE_KEY_AND_PROPAGATE, "product1", 0), 0))
        );
    }

    @Test
    public void leftJoinShouldPropagateNothingWhenOldAndNewLeftValueIsNull() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        leftJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        leftJoinProcessor.process(new Record<>("product1", new Change<>(null, null), 0));

        assertThat(context.forwarded(), empty());
    }

    // Inner join tests
    @Test
    public void innerJoinShouldPropagateNewPrimaryKey() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        innerJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue("merchant1");

        innerJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, null), 0));

        assertThat(context.forwarded().size(), is(1));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>("merchant1", new SubscriptionWrapper<>(hash(leftRecordValue), PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, "product1", 0), 0))
        );
    }

    @Test
    public void innerJoinShouldNotPropagateNewPrimaryKeyWithNullFK() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        innerJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue(null);

        innerJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, null), 0));

        assertThat(context.forwarded(), empty());

        // test dropped-records sensors
        assertEquals(1.0, getDroppedRecordsTotalMetric(context));
        assertNotEquals(0.0, getDroppedRecordsRateMetric(context));
    }

    @Test
    public void innerJoinShouldDeleteOldAndPropagateNewFK() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        innerJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue("merchant2");

        innerJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, new LeftValue("merchant1")), 0));

        assertThat(context.forwarded().size(), is(2));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>("merchant1", new SubscriptionWrapper<>(hash(leftRecordValue), DELETE_KEY_NO_PROPAGATE, "product1", 0), 0))
        );
        assertThat(
            context.forwarded().get(1).record(),
            is(new Record<>("merchant2", new SubscriptionWrapper<>(hash(leftRecordValue), PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, "product1", 0), 0))
        );
    }

    @Test
    public void innerJoinShouldPropagateNothingWhenOldAndNewFKIsNull() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        innerJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        final LeftValue leftRecordValue = new LeftValue(null);

        innerJoinProcessor.process(new Record<>("product1", new Change<>(leftRecordValue, leftRecordValue), 0));

        assertThat(context.forwarded(), empty());

        // test dropped-records sensors
        assertEquals(1.0, getDroppedRecordsTotalMetric(context));
        assertNotEquals(0.0, getDroppedRecordsRateMetric(context));
    }

    @Test
    public void innerJoinShouldPropagateDeletionOfPrimaryKey() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        innerJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        innerJoinProcessor.process(new Record<>("product1", new Change<>(null, new LeftValue("merchant1")), 0));

        assertThat(context.forwarded().size(), is(1));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>("merchant1", new SubscriptionWrapper<>(null, DELETE_KEY_AND_PROPAGATE, "product1", 0), 0))
        );
    }

    @Test
    public void innerJoinShouldPropagateNothingWhenOldAndNewLeftValueIsNull() {
        final MockInternalNewProcessorContext<String, SubscriptionWrapper<String>> context = new MockInternalNewProcessorContext<>();
        innerJoinProcessor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        innerJoinProcessor.process(new Record<>("product1", new Change<>(null, null), 0));

        assertThat(context.forwarded(), empty());
    }

    private static class LeftValueSerializer implements Serializer<LeftValue> {
        @Override
        public byte[] serialize(final String topic, final LeftValue data) {
            if (data == null) return null;
            else if (data.foreignKey == null) return "null".getBytes();
            return new StringSerializer().serialize(topic, data.getForeignKey());
        }
    }

    private final static class LeftValue {
        private final String foreignKey;

        public LeftValue(final String value) {
            this.foreignKey = value;
        }

        public String getForeignKey() {
            return foreignKey;
        }
    }

    private static long[] hash(final LeftValue value) {
        return Murmur3.hash128(new LeftValueSerializer().serialize("value-serde-topic", value));
    }
}
