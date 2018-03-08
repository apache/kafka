package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class MockProcessorContextTest {

    @Test
    public void shouldMockProcess() {
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
}
