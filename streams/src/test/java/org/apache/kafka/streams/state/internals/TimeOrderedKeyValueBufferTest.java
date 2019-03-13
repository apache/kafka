package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
public class TimeOrderedKeyValueBufferTest<B extends TimeOrderedKeyValueBuffer> {
    private final Function<String, B> bufferSupplier;

    @Parameterized.Parameters(name = "{index}: buffer={0}")
    public static Collection<Object[]> parameters() {
        return asList(
            new Object[] {
                (Function<String, InMemoryTimeOrderedKeyValueBuffer>) (name) ->
                    (InMemoryTimeOrderedKeyValueBuffer) new InMemoryTimeOrderedKeyValueBuffer
                        .Builder(name)
                        .build()
            },
            new Object[] {
                (Function<String, InMemoryTimeOrderedKeyValueBuffer>) (name) ->
                    (InMemoryTimeOrderedKeyValueBuffer) new InMemoryTimeOrderedKeyValueBuffer
                        .Builder(name)
                        .withLoggingDisabled().build()
            },
            new Object[] {
                (Function<String, RocksDBTimeOrderedKeyValueBuffer>) (name) ->
                    (RocksDBTimeOrderedKeyValueBuffer) new RocksDBTimeOrderedKeyValueBuffer
                        .Builder(name, new StrictBufferConfigImpl(-1L, 32_000L, BufferFullStrategy.SPILL_TO_DISK))
                        .build()
            },
            new Object[] {
                (Function<String, RocksDBTimeOrderedKeyValueBuffer>) (name) ->
                    (RocksDBTimeOrderedKeyValueBuffer) new RocksDBTimeOrderedKeyValueBuffer
                        .Builder(name, new StrictBufferConfigImpl(-1L, 32_000L, BufferFullStrategy.SPILL_TO_DISK))
                        .withLoggingDisabled().build()
            }
        );
    }

    public TimeOrderedKeyValueBufferTest(final Function<String, B> bufferSupplier) {
        this.bufferSupplier = bufferSupplier;
    }

    @Test
    public void shouldInit() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply("testStore" + new Random().nextInt(Integer.MAX_VALUE));
        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        context.setRecordCollector(new MockRecordCollector());
        buffer.init(context,buffer);
    }

    @Test
    public void shouldAcceptData() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply("testStore" + new Random().nextInt(Integer.MAX_VALUE));
        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        context.setRecordCollector(new MockRecordCollector());
        buffer.init(context,buffer);
        buffer.put(0, Bytes.wrap("asdf".getBytes(StandardCharsets.UTF_8)), new ContextualRecord("asdf".getBytes(StandardCharsets.UTF_8), new ProcessorRecordContext(0, 0, 0, "topic")));
    }

    @Test
    public void shoulsdf() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply("testStore" + new Random().nextInt(Integer.MAX_VALUE));
        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        context.setRecordCollector(new MockRecordCollector());
        buffer.init(context,buffer);
        buffer.put(0, Bytes.wrap("asdf".getBytes(StandardCharsets.UTF_8)), new ContextualRecord("asdf".getBytes(StandardCharsets.UTF_8), new ProcessorRecordContext(0, 0, 0, "topic")));
        final long size = buffer.bufferSize();
        System.out.println(size);
    }
}
