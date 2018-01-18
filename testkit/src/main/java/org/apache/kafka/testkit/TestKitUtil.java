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
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class TestKitUtil {
    private final static Logger log = LoggerFactory.getLogger(TestKitUtil.class);

    //static void closeAll(final T... vals) {
        //
    //}

    static <T> T firstNonNull(final T... vals) {
        for (T val : vals) {
            if (val != null) {
                return val;
            }
        }
        throw new RuntimeException("All values were null.");
    }

    public static boolean awaitTerminationUninterruptibly(ExecutorService executorService) {
        boolean interrupted = false;
        while (true) {
            try {
                executorService.awaitTermination(100, TimeUnit.DAYS);
                return interrupted;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
    }

    static File createTempDir(final String prefix) {
        final File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Utils.delete(file);
                } catch (IOException e) {
                    log.error("Error deleting {}", file.getAbsolutePath(), e);
                }
            }
        });
        return file;
    }

    /**
     * Merge many configuration maps into one.
     * The later configuration maps override the earlier ones.
     */
    public static Map<String, String> mergeConfigs(Map<String, String>... configsArr) {
        HashMap<String, String> map = new HashMap<>();
        for (Map<String, String> configs : configsArr) {
            map.putAll(configs);
        }
        return map;
    }
}
