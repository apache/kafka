/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.standalone;

import org.apache.kafka.copycat.errors.CopycatRuntimeException;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * Implementation of ConfigStorage that saves state to a local file. This allows a standalone
 * node to tolerate faults/restarts.
 * </p>
 * <p>
 * Currently the implementation is naive, inefficient, and only meant for testing or a small
 * number of jobs.
 * </p>
 */
public class FileConfigStorage implements ConfigStorage {
    public static final String FILE_CONFIG = "config.storage.file";
    public static final String FILE_DEFAULT = "configs.db";

    private String filename;
    private Map<String, Properties> connectorConfig = new HashMap<>();

    @Override
    public void configure(Properties props) {
        filename = props.getProperty(FILE_CONFIG);
        if (filename == null)
            filename = FILE_DEFAULT;
        load();
    }

    @Override
    public void close() {

    }

    @Override
    public void putConnectorConfig(String connector, Properties properties) {
        if (properties == null) {
            connectorConfig.remove(connector);
        } else {
            connectorConfig.put(connector, properties);
        }
        save();
    }

    @Override
    public Properties getConnectorConfig(String connector) {
        return connectorConfig.get(connector);
    }

    @Override
    public Collection<String> getConnectors() {
        return connectorConfig.keySet();
    }

    /**
     * Saves the current state to disk, overwriting previous data. This action is performed
     * atomically.
     */
    private void save() {
        try {
            String tempFilename = filename + ".temp";
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(tempFilename));
            os.writeObject(connectorConfig);
            os.close();

            // Overwrite the original. Since the nio file package is JDK7+ only, this is the best we
            // can do.
            File tempFile = new File(tempFilename);
            tempFile.renameTo(new File(filename));
        } catch (IOException e) {
            throw new CopycatRuntimeException("Failed to save config data to file", e);
        }
    }

    private void load() {
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(filename));
            connectorConfig = (Map<String, Properties>) is.readObject();
        } catch (FileNotFoundException e) {
            // Expected on first run
        } catch (IOException | ClassNotFoundException e) {
            throw new CopycatRuntimeException("Failed to load config data", e);
        }
    }
}
