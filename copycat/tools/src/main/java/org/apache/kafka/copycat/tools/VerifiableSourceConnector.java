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
 **/

package org.apache.kafka.copycat.tools;

import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @see VerifiableSourceTask
 */
public class VerifiableSourceConnector extends SourceConnector {
    private Properties config;

    @Override
    public void start(Properties props) {
        this.config = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return VerifiableSourceTask.class;
    }

    @Override
    public List<Properties> taskConfigs(int maxTasks) {
        ArrayList<Properties> configs = new ArrayList<>();
        for (Integer i = 0; i < maxTasks; i++) {
            Properties props = new Properties();
            for (String propName : config.stringPropertyNames())
                props.setProperty(propName, config.getProperty(propName));
            props.setProperty(VerifiableSourceTask.ID_CONFIG, i.toString());
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
    }
}
