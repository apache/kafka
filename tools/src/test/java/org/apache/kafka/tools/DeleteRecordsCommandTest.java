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
package org.apache.kafka.tools;

import org.apache.kafka.server.common.AdminOperationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of {@link DeleteRecordsCommand} tool.
 */
public class DeleteRecordsCommandTest {
    @Test
    public void testOffsetFileNotExists() {
        assertThrows(IOException.class, () -> DeleteRecordsCommand.main(new String[]{
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file"
        }));
    }

    @Test
    public void testCommandConfigNotExists() {
        assertThrows(NoSuchFileException.class, () -> DeleteRecordsCommand.main(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file",
            "--command-config", "/another/not/existing/file"
        }));
    }

    @Test
    public void testWrongVersion() {
        assertThrows(
            AdminOperationException.class,
            () -> DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup("{version:\"string\"}")
        );

        assertThrows(
            AdminOperationException.class,
            () -> DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup("{version:2}")
        );
    }

    @Test
    public void testWrongPartitions() {
        assertThrows(
            AdminOperationException.class,
            () -> DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup("{version:1}")
        );

        assertThrows(
            AdminOperationException.class,
            () -> DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup("{version:1, partitions:2}")
        );
    }
}