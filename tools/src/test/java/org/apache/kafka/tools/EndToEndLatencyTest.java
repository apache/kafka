package org.apache.kafka.tools;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EndToEndLatencyTest {

    @Mock
    KafkaConsumer<byte[], byte[]> consumer;

    @Mock
    ConsumerRecords<byte[], byte[]> records;

    @Test
    public void testUnexpectedArgs() {
        String[] args = new String[] {
                "-b", "localhost:9092",
                "-t", "test",
                "-n", "10000",
                "-a", "1",
                "-s", "200",
                "-e", "123"};
        ArgumentParser parser = EndToEndLatency.addArguments();
        ArgumentParserException thrown = assertThrows(ArgumentParserException.class, () -> parser.parseArgs(args));
        assertEquals("unrecognized arguments: '-e'", thrown.getMessage());
    }

    @Test
    public void shouldFailWhenProducerAcksAreNotSynchronised() throws Exception {
        String[] args = new String[] {
                "-b", "localhost:9092",
                "-t", "test",
                "-n", "10000",
                "-a", "0",
                "-s", "200"};

        assertThrows(IllegalArgumentException.class, () -> EndToEndLatency.execute(args));
    }

    @Test
    public void shouldFailWhenConsumerRecordsIsEmpty() {
        when(records.isEmpty()).thenReturn(true);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, new byte[0], records));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenSentIsNotEqualToReceived() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn("kafkab".getBytes(StandardCharsets.UTF_8));
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, "kafkaa".getBytes(StandardCharsets.UTF_8), records));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenReceivedMoreThanOneRecord() {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = mock(Iterator.class);
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(records.isEmpty()).thenReturn(false);
        when(records.iterator()).thenReturn(iterator);
        when(iterator.next()).thenReturn(record);
        when(record.value()).thenReturn("kafkaa".getBytes(StandardCharsets.UTF_8));
        when(records.count()).thenReturn(2);
        assertThrows(RuntimeException.class, () -> EndToEndLatency.validate(consumer, "kafkaa".getBytes(StandardCharsets.UTF_8), records));
    }

}
