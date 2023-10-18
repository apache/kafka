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

package kafka.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
    private final File cleanShutdownFile;
    private static final int CURRENT_VERSION = 0;
    private final Logger logger;

    private enum Fields {
        VERSION,
        BROKER_EPOCH;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
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
            Map<String, String> payload = new HashMap<>();
            payload.put(Fields.VERSION.toString(), Integer.toString(version));
            payload.put(Fields.BROKER_EPOCH.toString(), Long.toString(brokerEpoch));
            bw.write(new ObjectMapper().writeValueAsString(payload));
            bw.flush();
            os.getFD().sync();
        } finally {
            bw.close();
            os.close();
        }
    }

    public long read() {
        long brokerEpoch = -1L;
        try {
            String text = Utils.readFileAsString(cleanShutdownFile.toPath().toString());
            Map<String, String> content = new ObjectMapper().readValue(text, HashMap.class);

            brokerEpoch = Long.parseLong(content.getOrDefault(Fields.BROKER_EPOCH.toString(), "-1L"));
        } catch (Exception e) {
            logger.warn("Fail to read the clean shutdown file in " + cleanShutdownFile.toPath() + ":" + e);
        }
        return brokerEpoch;
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