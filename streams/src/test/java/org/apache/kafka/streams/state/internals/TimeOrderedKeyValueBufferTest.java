package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TimeOrderedKeyValueBufferTest<B extends TimeOrderedKeyValueBuffer> {
    private final Function<String, B> bufferSupplier;
    private final String testName;

    @Parameterized.Parameters(name = "{index}: test={0}")
    public static Collection<Object[]> parameters() {
        return asList(
            new Object[] {
                InMemoryTimeOrderedKeyValueBuffer.class.getSimpleName() + "_" + new Random().nextInt(Integer.MAX_VALUE),
                (Function<String, InMemoryTimeOrderedKeyValueBuffer>) (name) ->
                    (InMemoryTimeOrderedKeyValueBuffer) new InMemoryTimeOrderedKeyValueBuffer
                        .Builder(name)
                        .withLoggingDisabled().build()
            },
            new Object[] {
                RocksDBTimeOrderedKeyValueBuffer.class.getSimpleName() + "_" + new Random().nextInt(Integer.MAX_VALUE),
                (Function<String, RocksDBTimeOrderedKeyValueBuffer>) (name) ->
                    (RocksDBTimeOrderedKeyValueBuffer) new RocksDBTimeOrderedKeyValueBuffer
                        .Builder(name, new StrictBufferConfigImpl(-1L,
                                                                  32_000L,
                                                                  BufferFullStrategy.SPILL_TO_DISK))
                        .withLoggingDisabled().build()
            }
        );
    }

    public TimeOrderedKeyValueBufferTest(final String testName, final Function<String, B> bufferSupplier) {
        this.testName = testName;
        this.bufferSupplier = bufferSupplier;
    }

    private static MockInternalProcessorContext makeContext() {
        return new MockInternalProcessorContext(TestUtils.tempDirectory());
    }


    private static void cleanup(final MockInternalProcessorContext context, final TimeOrderedKeyValueBuffer buffer) {
        try {
            buffer.close();
            Files.walk(context.stateDir().toPath())
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(File::delete);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldInit() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        cleanup(context, buffer);
    }

    @Test
    public void shouldAcceptData() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        buffer.put(0, getBytes("asdf"), getRecord("2p93nf"));
        cleanup(context, buffer);
    }

    private static ContextualRecord getRecord(final String value) {
        return new ContextualRecord(
            value.getBytes(UTF_8),
            new ProcessorRecordContext(0, 0, 0, "topic")
        );
    }

    private static Bytes getBytes(final String key) {
        return Bytes.wrap(key.getBytes(UTF_8));
    }

    @Test
    public void shouldRemoveData() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        buffer.put(0, getBytes("asdf"), getRecord("qwer"));
        assertThat(buffer.numRecords(), is(1));
        buffer.evictWhile(() -> true, kv -> {});
        assertThat(buffer.numRecords(), is(0));
        cleanup(context, buffer);
    }

    @Test
    public void shouldRespectEvictionPredicate() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        final Bytes firstKey = getBytes("asdf");
        final ContextualRecord firstRecord = getRecord("eyt");
        buffer.put(0, firstKey, firstRecord);
        buffer.put(1, getBytes("zxcv"), getRecord("rtg"));
        assertThat(buffer.numRecords(), is(2));
        final List<KeyValue<Bytes, ContextualRecord>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> buffer.numRecords() > 1, evicted::add);
        assertThat(buffer.numRecords(), is(1));
        assertThat(evicted, is(singletonList(new KeyValue<>(firstKey, firstRecord))));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackCount() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        buffer.put(0, getBytes("asdf"), getRecord("oin"));
        assertThat(buffer.numRecords(), is(1));
        buffer.put(1, getBytes("asdf"), getRecord("wekjn"));
        assertThat(buffer.numRecords(), is(1));
        buffer.put(0, getBytes("zxcv"), getRecord("24inf"));
        assertThat(buffer.numRecords(), is(2));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackSize() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        buffer.put(0, getBytes("asdf"), getRecord("23roni"));
        assertThat(buffer.bufferSize(), is(43L));
        buffer.put(1, getBytes("asdf"), getRecord("3l"));
        assertThat(buffer.bufferSize(), is(39L));
        buffer.put(0, getBytes("zxcv"), getRecord("qfowin"));
        assertThat(buffer.bufferSize(), is(82L));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackMinTimestamp() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        buffer.put(1, getBytes("asdf"), getRecord("2093j"));
        assertThat(buffer.minTimestamp(), is(1L));
        buffer.put(0, getBytes("zxcv"), getRecord("3gon4i"));
        assertThat(buffer.minTimestamp(), is(0L));
        cleanup(context, buffer);
    }

    @Test
    public void shouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        buffer.put(1, getBytes("zxcv"), getRecord("o23i4"));
        assertThat(buffer.numRecords(), is(1));
        assertThat(buffer.bufferSize(), is(42L));
        assertThat(buffer.minTimestamp(), is(1L));

        buffer.put(0, getBytes("asdf"), getRecord("3ng"));
        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.bufferSize(), is(82L));
        assertThat(buffer.minTimestamp(), is(0L));

        final AtomicInteger callbackCount = new AtomicInteger(0);
        buffer.evictWhile(() -> true, (kv) -> {
            switch (callbackCount.incrementAndGet()) {
                case 1: {
                    assertThat(new String(kv.key.get(), UTF_8), is("asdf"));
                    assertThat(buffer.numRecords(), is(2));
                    assertThat(buffer.bufferSize(), is(82L));
                    assertThat(buffer.minTimestamp(), is(0L));
                    break;
                }
                case 2: {
                    assertThat(new String(kv.key.get(), UTF_8), is("zxcv"));
                    assertThat(buffer.numRecords(), is(1));
                    assertThat(buffer.bufferSize(), is(42L));
                    assertThat(buffer.minTimestamp(), is(1L));
                    break;
                }
                default: {
                    fail("too many invocations");
                    break;
                }
            }
        });
        assertThat(callbackCount.get(), is(2));
        assertThat(buffer.numRecords(), is(0));
        assertThat(buffer.bufferSize(), is(0L));
        assertThat(buffer.minTimestamp(), is(Long.MAX_VALUE));
        cleanup(context, buffer);
    }


    @Test
    public void shouldRestore() {
        final TimeOrderedKeyValueBuffer buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("topic",
                                 0,
                                 0,
                                 -9,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + 4).putLong(1L).put("qwer".getBytes(UTF_8)).array()),
            new ConsumerRecord<>("topic",
                                 0,
                                 1,
                                 0,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + 5).putLong(0L).put("3o4im".getBytes(UTF_8)).array())
        ));

        // the buffer metadata is correct after adding both records
        
        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.bufferSize(), is(83L));
        assertThat(buffer.minTimestamp(), is(0L));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<KeyValue<Bytes, ContextualRecord>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored

        assertThat(evicted, is(asList(
            new KeyValue<>(
                getBytes("zxcv"),
                new ContextualRecord("3o4im".getBytes(UTF_8),
                                     new ProcessorRecordContext(0,
                                                                1,
                                                                0,
                                                                "topic",
                                                                new RecordHeaders()))),
            new KeyValue<>(
                getBytes("asdf"),
                new ContextualRecord("qwer".getBytes(UTF_8),
                                     new ProcessorRecordContext(-9,
                                                                0,
                                                                0,
                                                                "topic",
                                                                new RecordHeaders())))
        )));

        cleanup(context, buffer);
    }
}
