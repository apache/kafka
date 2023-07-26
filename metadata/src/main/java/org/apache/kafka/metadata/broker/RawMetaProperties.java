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

package org.apache.kafka.metadata.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class RawMetaProperties {
    public static final String CLUSTER_ID_KEY = "cluster.id";
    public static final String BROKER_ID_KEY = "broker.id";
    public static final String NODE_ID_KEY = "node.id";
    public static final String VERSION_KEY = "version";

    private Properties props;

    public RawMetaProperties(Properties props) {
        this.props = props != null ? props : new Properties();
    }

    public Optional<String> getClusterId() {
        return Optional.ofNullable(props.getProperty(CLUSTER_ID_KEY));
    }

    public void setClusterId(String id) {
        props.setProperty(CLUSTER_ID_KEY, id);
    }

    public Optional<Integer> getBrokerId() {
        return intValue(BROKER_ID_KEY);
    }

    public void setBrokerId(int id) {
        props.setProperty(BROKER_ID_KEY, Integer.toString(id));
    }

    public Optional<Integer> getNodeId() {
        return intValue(NODE_ID_KEY);
    }

    public void setNodeId(int id) {
        props.setProperty(NODE_ID_KEY, Integer.toString(id));
    }

    public int getVersion() {
        return intValue(VERSION_KEY).orElse(0);
    }

    public void setVersion(int ver) {
        props.setProperty(VERSION_KEY, Integer.toString(ver));
    }

    public Properties getProps() {
        return props;
    }

    public void requireVersion(int expectedVersion) {
        if (getVersion() != expectedVersion) {
            throw new RuntimeException("Expected version " + expectedVersion + ", but got version " + getVersion());
        }
    }

    private Optional<Integer> intValue(String key) {
        try {
            return Optional.ofNullable(props.getProperty(key)).map(Integer::parseInt);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to parse " + key + " property as an int: " + e.getMessage());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof RawMetaProperties) {
            RawMetaProperties other = (RawMetaProperties) obj;
            return props.equals(other.props);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return props.hashCode();
    }

    @Override
    public String toString() {
        List<String> keys = new ArrayList<>(props.stringPropertyNames());
        Collections.sort(keys);

        List<String> keyValuePairs = new ArrayList<>();
        for (String key : keys) {
            String value = props.getProperty(key);
            keyValuePairs.add(key + "=" + value);
        }

        return "{" + String.join(", ", keyValuePairs) + "}";
    }
}


