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

package org.apache.kafka.storage.internals.checkpoint;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Json;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.OptionalLong;

/**
 * Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
 * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
 * avoided by passing in the recovery point, however finding the correct position to do this
 * requires accessing the offset index which may not be safe in an unclean shutdown.
 * For more information see the discussion in PR#2104
 *
 * Also, the clean shutdown file can also store the broker epoch, this can be used in the broker registration to
 * demonstrate the last reboot is a clean shutdown. (KIP-966)
 */

public class CleanShutdownFileHandler {
    public static final String CLEAN_SHUTDOWN_FILE_NAME = ".kafka_cleanshutdown";
    // Visible for testing
    final File cleanShutdownFile;
    private static final int CURRENT_VERSION = 0;
    private final Logger logger;

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Content {
        public int version;
        public Long brokerEpoch;

        public Content() {};

        public Content(int version, Long brokerEpoch) {
            this.version = version;
            this.brokerEpoch = brokerEpoch;
        }
    }

    public CleanShutdownFileHandler(String dirPath) {
        logger = new LogContext().logger(CleanShutdownFileHandler.class);
        this.cleanShutdownFile = new File(dirPath, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME);
    }

    public void write(long brokerEpoch) throws Exception {
        write(brokerEpoch, CURRENT_VERSION);
    }

    // visible to test.
    void write(long brokerEpoch, int version) throws Exception {
        FileOutputStream os = new FileOutputStream(cleanShutdownFile);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
        try {
            Content content = new Content(version, brokerEpoch);
            bw.write(Json.encodeAsString(content));
            bw.flush();
            os.getFD().sync();
        } finally {
            bw.close();
            os.close();
        }
    }

    @SuppressWarnings("unchecked")
    public OptionalLong read() {
        try {
            String text = Utils.readFileAsString(cleanShutdownFile.toPath().toString());
            Content content = Json.parseStringAs(text, Content.class);
            return OptionalLong.of(content.brokerEpoch);
        } catch (Exception e) {
            logger.debug("Fail to read the clean shutdown file in " + cleanShutdownFile.toPath() + ":" + e);
            return OptionalLong.empty();
        }
    }

    public void delete() throws Exception {
        Files.deleteIfExists(cleanShutdownFile.toPath());
    }

    public boolean exists() {
        return  cleanShutdownFile.exists();
    }

    @Override
    public String toString() {
        return "CleanShutdownFile=(" + "file=" + cleanShutdownFile.toString() + ')';
    }
}