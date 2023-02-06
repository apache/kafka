package org.apache.kafka.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogDirsCommandTest {

    @Test
    public void checkLogDirsCommandOutput() throws UnsupportedEncodingException, TerseException, ExecutionException, JsonProcessingException, InterruptedException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayOutputStream, false, StandardCharsets.UTF_8.name());

        PrintStream originalStandardOut = System.out;
        System.setOut(printStream);

        Node broker = new Node(1, "hostname", 9092);

        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            //input exist brokerList
            LogDirsCommand.LogDirsCommandOptions existentBrokerListOptions = new LogDirsCommand.LogDirsCommandOptions("--bootstrap-server", "EMPTY", "--broker-list", "1", "--describe");

            LogDirsCommand.execute(existentBrokerListOptions, adminClient);

            String existingBrokersContent = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
            Iterator<String> existingBrokersLineIterator = Arrays.stream(existingBrokersContent.split("\n")).iterator();

            assertTrue(existingBrokersLineIterator.hasNext());
            assertTrue(existingBrokersLineIterator.next().contains("Querying brokers for log directories information"));

            //input nonexistent brokerList
            byteArrayOutputStream.reset();
            final LogDirsCommand.LogDirsCommandOptions nonExistentBrokerListOptions = new LogDirsCommand.LogDirsCommandOptions("--bootstrap-server", "EMPTY", "--broker-list", "0,1,2", "--describe");
            assertThrows(TerseException.class, () -> LogDirsCommand.execute(nonExistentBrokerListOptions, adminClient));

            //input duplicate ids
            byteArrayOutputStream.reset();
            final LogDirsCommand.LogDirsCommandOptions duplicateIdsOptions = new LogDirsCommand.LogDirsCommandOptions("--bootstrap-server", "EMPTY", "--broker-list", "0,0,1,2,2", "--describe");
            assertThrows(TerseException.class, () -> LogDirsCommand.execute(duplicateIdsOptions, adminClient));

            //use all brokerList for current cluster
            byteArrayOutputStream.reset();
            final LogDirsCommand.LogDirsCommandOptions allBrokerListOptions = new LogDirsCommand.LogDirsCommandOptions("--bootstrap-server", "EMPTY", "--describe");
            LogDirsCommand.execute(allBrokerListOptions, adminClient);
            String allBrokersContent = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
            Iterator<String> allBrokersLineIterator = Arrays.stream(allBrokersContent.split("\n")).iterator();

            assertTrue(allBrokersLineIterator.hasNext());
            String sample = allBrokersLineIterator.next();
            assertTrue(sample.contains("Querying brokers for log directories information"));
        } finally {
            System.setOut(originalStandardOut);
        }
    }
}
