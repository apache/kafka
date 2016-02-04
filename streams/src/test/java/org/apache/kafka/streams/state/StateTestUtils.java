/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A utility for tests to create and manage unique and isolated directories on the file system for local state.
 */
public class StateTestUtils {

    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong();

    /**
     * Create a new temporary directory that will be cleaned up automatically upon shutdown.
     * @return the new directory that will exist; never null
     */
    public static File tempDir() {
        try {
            final File dir = Files.createTempDirectory("test").toFile();
            dir.mkdirs();
            dir.deleteOnExit();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    deleteDirectory(dir);
                }
            });
            return dir;
        } catch (IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
    }

    private static void deleteDirectory(File dir) {
        if (dir != null && dir.exists()) {
            try {
                Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }

                });
            } catch (IOException e) {
                // do nothing
            }
        }
    }
}
