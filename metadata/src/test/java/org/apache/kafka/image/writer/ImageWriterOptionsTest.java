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

package org.apache.kafka.image.writer;

import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class ImageWriterOptionsTest {
    @Test
    public void testDefaultLossHandler() {
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        assertEquals("stuff", assertThrows(UnwritableMetadataException.class,
                () -> options.handleLoss("stuff")).loss());
    }

    @Test
    public void testSetMetadataVersion() {
        for (int i = MetadataVersion.MINIMUM_KRAFT_VERSION.ordinal();
                 i < MetadataVersion.VERSIONS.length;
                 i++) {
            MetadataVersion version = MetadataVersion.VERSIONS[i];
            ImageWriterOptions.Builder options = new ImageWriterOptions.Builder().
                    setMetadataVersion(version);
            if (i < MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.ordinal()) {
                assertEquals(MetadataVersion.MINIMUM_KRAFT_VERSION, options.metadataVersion());
                assertEquals(version, options.orgmetadataVersion());
            } else {
                assertEquals(version, options.metadataVersion());
            }
        }
    }

    @Test
    public void testHandleLoss() {
        PrintStream originalOut = System.out;
        String expectedMessage = "stuff";
        Consumer<UnwritableMetadataException> customLossHandler = e -> System.out.println(e.getMessage());

        for (int i = MetadataVersion.MINIMUM_KRAFT_VERSION.ordinal();
             i < MetadataVersion.VERSIONS.length;
             i++) {
            ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            MetadataVersion version = MetadataVersion.VERSIONS[i];
            ImageWriterOptions options = new ImageWriterOptions.Builder()
                    .setMetadataVersion(version)
                    .setLossHandler(customLossHandler)
                    .build();
            System.setOut(new PrintStream(outContent));
            options.handleLoss(expectedMessage);
            System.setOut(originalOut);
            String formattedMessage = String.format("Metadata has been lost because the following could not be represented in metadata version %s: %s", version, expectedMessage);
            assertEquals(formattedMessage, outContent.toString().trim());
        }
    }
}
