package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

@RunWith(EasyMockRunner.class)
public class KStreamRepartitionTest {
    private final String inputTopic = "input-topic";

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
    private final ConsumerRecordFactory<Integer, String> recordFactory = new ConsumerRecordFactory<>(
        Serdes.Integer().serializer(),
        Serdes.String().serializer(),
        0L
    );

    private StreamsBuilder builder;

    @Before
    public void setUp() {
        builder = new StreamsBuilder();
    }

    @Test
    public void shouldInvokePartitionerWhenSet() {
        final int[] expectedKeys = new int[]{0, 1};
        final StreamPartitioner<Integer, String> streamPartitionerMock = EasyMock.mock(StreamPartitioner.class);

        expect(streamPartitionerMock.partition(anyString(), eq(0), eq("X0"), anyInt())).andReturn(1).times(1);
        expect(streamPartitionerMock.partition(anyString(), eq(1), eq("X1"), anyInt())).andReturn(1).times(1);
        replay(streamPartitionerMock);

        final Repartitioned<Integer, String> repartitioned = Repartitioned
            .streamPartitioner(streamPartitionerMock)
            .withName("repartition");

        final KStream<Integer, String> repartitionedStream = builder.<Integer, String>stream(inputTopic)
            .repartition(repartitioned);

        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        repartitionedStream.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(inputTopic, expectedKeys[i], "X" + expectedKeys[i], i + 10));
            }
            final MockProcessor<Integer, String> proc = supplier.theCapturedProcessor();
            proc.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "X0", 10),
                new KeyValueTimestamp<>(1, "X1", 11)
            );
        }

        verify(streamPartitionerMock);
    }
}
