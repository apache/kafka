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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class CommittedOffsetsFile {
    private final File offsetsFile;

    private static final Pattern MINIMUM_ONE_WHITESPACE = Pattern.compile("\\s+");

    CommittedOffsetsFile(File offsetsFile) {
        this.offsetsFile = offsetsFile;
    }

    public synchronized void write(Map<Integer, Long> committedOffsets) throws IOException {
        File newOffsetsFile = new File(offsetsFile.getAbsolutePath() + ".new");

        FileOutputStream fos = new FileOutputStream(newOffsetsFile);
        try (final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {
            for (Map.Entry<Integer, Long> entry : committedOffsets.entrySet()) {
                writer.write(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }

            writer.flush();
            fos.getFD().sync();
        }

        Utils.atomicMoveWithFallback(newOffsetsFile.toPath(), offsetsFile.toPath());
    }

    public synchronized Map<Integer, Long> read() throws IOException {
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(offsetsFile.toPath(),
                StandardCharsets.UTF_8)) {
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = MINIMUM_ONE_WHITESPACE.split(line);
                if (strings.length != 2) {
                    throw new IOException("Invalid format in line: []" + line);
                }
                int partition = Integer.parseInt(strings[0]);
                long offset = Long.parseLong(strings[1]);
                partitionOffsets.put(partition, offset);
            }
        }
        return partitionOffsets;
    }
}
