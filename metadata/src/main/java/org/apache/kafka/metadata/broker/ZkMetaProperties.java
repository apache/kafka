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

import java.util.Properties;

public class ZkMetaProperties {
    private String clusterId;
    private int brokerId;

    public ZkMetaProperties(String clusterId, int brokerId) {
        this.clusterId = clusterId;
        this.brokerId = brokerId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public Properties toProperties() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setVersion(0);
        properties.setClusterId(clusterId);
        properties.setBrokerId(brokerId);
        return properties.getProps();
    }

    @Override
    public String toString() {
        return "ZkMetaProperties(brokerId=" + brokerId + ", clusterId=" + clusterId + ")";
    }
}

