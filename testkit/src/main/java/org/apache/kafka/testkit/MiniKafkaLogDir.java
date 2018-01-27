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

package org.apache.kafka.testkit;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Implements a Kafka log directory.
 */
public class MiniKafkaLogDir implements AutoCloseable {
    private final Logger log;
    File dir;

    MiniKafkaLogDir(Logger log, File dir) {
        this.log = log;
        this.dir = dir;
    }

    /**
     * Stops Zookeeper.
     * Waits for all threads to be stopped.
     * Does not throw exceptions.
     */
    @Override
    public void close() {
        if (dir != null) {
            try {
                Utils.delete(dir);
            } catch (IOException e) {
                log.error("Error deleting logDir", e);
            } finally {
                dir = null;
            }
        }
    }
}
