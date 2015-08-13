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

package org.apache.kafka.copycat.cli;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Properties;

@InterfaceStability.Unstable
public class CopycatConfig extends AbstractConfig {

    public static final String WORKER_PROPERTIES_CONFIG = "worker-config";
    public static final String WORKER_PROPERTIES_CONFIG_DEFAULT = "";
    private static final String WORKER_PROPERTIES_CONFIG_DOC =
            "Path to a properties file with worker configuration.";

    public static final String CREATE_CONNECTORS_CONFIG = "create-connectors";
    public static final String CREATE_CONNECTORS_CONFIG_DEFAULT = "";
    private static final String CREATE_CONNECTORS_CONFIG_DOC =
            "List of paths to properties files with connector properties to use to create new connectors";

    public static final String DELETE_CONNECTORS_CONFIG = "delete-connectors";
    public static final String DELETE_CONNECTORS_CONFIG_DEFAULT = "";
    private static final String DELETE_CONNECTORS_CONFIG_DOC = "List of names of a connectors to "
            + "stop and delete.";

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(WORKER_PROPERTIES_CONFIG, Type.STRING, WORKER_PROPERTIES_CONFIG_DEFAULT,
                        Importance.HIGH, WORKER_PROPERTIES_CONFIG_DOC)
                .define(CREATE_CONNECTORS_CONFIG, Type.LIST, CREATE_CONNECTORS_CONFIG_DEFAULT,
                        Importance.HIGH, CREATE_CONNECTORS_CONFIG_DOC)
                .define(DELETE_CONNECTORS_CONFIG, Type.LIST, DELETE_CONNECTORS_CONFIG_DEFAULT,
                        Importance.HIGH, DELETE_CONNECTORS_CONFIG_DOC);
    }

    public CopycatConfig(Properties props) {
        super(config, props);
    }

    /**
     * Parses command line arguments into a Properties object and instantiate a
     * CopycatConfig with it.
     * @param args
     * @return
     */
    public static CopycatConfig parseCommandLineArgs(String[] args) {
        Properties props = new Properties();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String key, value;

            // Check for foo=bar or --foo=bar syntax
            if (arg.contains("=")) {
                String[] parts = arg.split("=", 1);
                key = parts[0];
                value = parts[1];
            } else {
                key = args[i];
                i += 1;
                value = args[i];
            }

            // Check for -- prefix on key
            if (key.startsWith("--"))
                key = key.substring(2);

            props.setProperty(key, value);
        }

        return new CopycatConfig(props);
    }
}
