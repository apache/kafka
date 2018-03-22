package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class MockProcessorContextTest {

    @Test
    public void shouldEnableVerifyingProcess() {
        final Properties config = new Properties();
        final MockProcessorContext context = new MockProcessorContext("test-app", new TaskId(0, 0), config);
        final Processor<String, Long> processor = new Processor<String, Long>() {
            private ProcessorContext context;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public void process(final String key, final Long value) {
                context.forward(key, value);
            }

            @Override
            public void punctuate(final long timestamp) {}

            @Override
            public void close() {}
        };
        processor.init(context);

        processor.process("a", 1L);
        processor.process("a", 2L);
        final KeyValue<Object, Object> capture1 = context.forwarded().get(0);
        Assert.assertEquals(new KeyValue<>("a", 1L), capture1);
        final KeyValue<Object, Object> capture2 = context.forwarded().get(1);
        Assert.assertEquals(new KeyValue<>("a", 2L), capture2);

        Assert.assertTrue(context.committed());

        context.resetForwards();
        context.resetCommits();

        processor.process("a", 3L);
        final KeyValue<Object, Object> capture3 = context.forwarded().get(0);
        Assert.assertEquals(new KeyValue<>("a", 3L), capture3);
        Assert.assertFalse(context.committed());
    }

    private static class PunctuateProcessor implements Processor<String,Long> {

        private KeyValueStore<String, Long> store;

        @Override
        public void init(final ProcessorContext context) {
            //noinspection unchecked
            this.store = (KeyValueStore<String, Long>) context.getStateStore("my-store");

            // simple simulation: a punctuator that clears the state store when it's invoked
            context.schedule(60_000L, PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(final long timestamp) {
                    final KeyValueStore<String, Long> stateStore = store;
                    final KeyValueIterator<String, Long> all = stateStore.all();
                    while (all.hasNext()) {
                        final KeyValue<String, Long> next = all.next();
                        stateStore.delete(next.key);
                    }
                }
            });
        }

        @Override
        public void process(final String key, final Long value) {
            store.put(key, value);
        }

        @Override
        public void punctuate(final long timestamp) {}

        @Override
        public void close() {}

    }

    @Test
    public void shouldEnableVerifyingPunctuate() {
        // create the mocked context
        final Properties config = new Properties();
        final MockProcessorContext context = new MockProcessorContext("test-app", new TaskId(0, 0), config);

        // create an in-memory store to use in testing
        final KeyValueStore<String, Long> myStore = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Serdes.String(), Serdes.Long()).withLoggingDisabled().build();
        context.register(myStore, false, null);

        // create the processor with the mocked context
        final Processor<String, Long> processor = new PunctuateProcessor();
        processor.init(context);

        // add a record
        processor.process("a", 1L);
        Assert.assertEquals((Long) 1L, myStore.get("a"));

        // get the punctuator and invoke it
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

        // verify the punctuator did its work
        Assert.assertNull(myStore.get("a"));
    }
}
