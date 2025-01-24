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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.internals.PrintedInternal;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PrintedTest {

    private final PrintStream originalSysOut = System.out;
    private final ByteArrayOutputStream sysOut = new ByteArrayOutputStream();
    private Printed<String, Integer> sysOutPrinter;

    @BeforeEach
    public void before() {
        System.setOut(new PrintStream(sysOut));
        sysOutPrinter = Printed.toSysOut();
    }

    @AfterEach
    public void after() {
        System.setOut(originalSysOut);
    }

    @Test
    public void shouldCreateProcessorThatPrintsToFile() throws IOException {
        final File file = TestUtils.tempFile();
        final ProcessorSupplier<String, Integer, Void, Void> processorSupplier = new PrintedInternal<>(
                Printed.<String, Integer>toFile(file.getPath()))
                .build("processor");
        final Processor<String, Integer, Void, Void> processor = processorSupplier.get();
        processor.process(new Record<>("hi", 1, 0L));
        processor.close();
        try (final InputStream stream = Files.newInputStream(file.toPath())) {
            final byte[] data = new byte[stream.available()];
            stream.read(data);
            assertThat(new String(data, StandardCharsets.UTF_8), equalTo("[processor]: hi, 1\n"));
        }
    }

    @Test
    public void shouldCreateProcessorThatPrintsToStdOut() throws UnsupportedEncodingException {
        final ProcessorSupplier<String, Integer, Void, Void> supplier = new PrintedInternal<>(sysOutPrinter).build("processor");
        final Processor<String, Integer, Void, Void> processor = supplier.get();

        processor.process(new Record<>("good", 2, 0L));
        processor.close();
        assertThat(sysOut.toString(StandardCharsets.UTF_8.name()), equalTo("[processor]: good, 2\n"));
    }

    @Test
    public void shouldPrintWithLabel() throws UnsupportedEncodingException {
        final Processor<String, Integer, Void, Void> processor = new PrintedInternal<>(sysOutPrinter.withLabel("label"))
                .build("processor")
                .get();

        processor.process(new Record<>("hello", 3, 0L));
        processor.close();
        assertThat(sysOut.toString(StandardCharsets.UTF_8.name()), equalTo("[label]: hello, 3\n"));
    }

    @Test
    public void shouldPrintWithKeyValueMapper() throws UnsupportedEncodingException {
        final Processor<String, Integer, Void, Void> processor = new PrintedInternal<>(
            sysOutPrinter.withKeyValueMapper((key, value) -> String.format("%s -> %d", key, value))
        ).build("processor").get();
        processor.process(new Record<>("hello", 1, 0L));
        processor.close();
        assertThat(sysOut.toString(StandardCharsets.UTF_8.name()), equalTo("[processor]: hello -> 1\n"));
    }

    @Test
    public void shouldThrowNullPointerExceptionIfFilePathIsNull() {
        assertThrows(NullPointerException.class, () -> Printed.toFile(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionIfMapperIsNull() {
        assertThrows(NullPointerException.class, () -> sysOutPrinter.withKeyValueMapper(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionIfLabelIsNull() {
        assertThrows(NullPointerException.class, () -> sysOutPrinter.withLabel(null));
    }

    @Test
    public void shouldThrowTopologyExceptionIfFilePathIsEmpty() {
        assertThrows(TopologyException.class, () -> Printed.toFile(""));
    }

    @Test
    public void shouldThrowTopologyExceptionIfFilePathDoesntExist() {
        assertThrows(TopologyException.class, () -> Printed.toFile("/this/should/not/exist"));
    }
}
