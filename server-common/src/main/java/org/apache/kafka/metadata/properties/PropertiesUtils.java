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

package org.apache.kafka.metadata.properties;

import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public final class PropertiesUtils {
    /**
     * Writes a Java Properties object to a file.
     *
     * @param props         The Properties object.
     * @param path          The file to write to.
     * @throws IOException
     */
    public static void writePropertiesFile(
        Properties props,
        String path,
        boolean fsync
    ) throws IOException {
        File tempFile = new File(path + ".tmp");
        try (
            FileOutputStream fos = new FileOutputStream(tempFile, false);
            OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            PrintWriter pw = new PrintWriter(osw)
        ) {
            props.store(pw, "");
            fos.flush();
            if (fsync) {
                fos.getFD().sync();
            }
        }
        File targetFile = new File(path);
        try {
            Utils.atomicMoveWithFallback(tempFile.toPath(), targetFile.toPath(), fsync);
        } catch (Throwable e) {
            Utils.delete(tempFile);
            throw e;
        }
    }

    /**
     * Reads a Java Properties object from a file.
     *
     * @param path          The file to read from.
     *
     * @throws java.nio.file.NoSuchFileException    If the file is not found.
     * @throws IOException                          If there is another exception while reading.
     */
    public static Properties readPropertiesFile(String path) throws IOException {
        Properties props = new Properties();
        try (InputStream propStream = Files.newInputStream(Paths.get(path))) {
            props.load(propStream);
        }
        return props;
    }

    /**
     * Pull an integer from a Properties object.
     *
     * @param props     The Properties object.
     * @param keyName   The key to look for.
     *
     * @return          The integer.
     */
    static int loadRequiredIntProp(
        Properties props,
        String keyName
    ) {
        String value = props.getProperty(keyName);
        if (value == null) {
            throw new RuntimeException("Failed to find " + keyName);
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Unable to read " + keyName + " as a base-10 number.", e);
        }
    }
}
