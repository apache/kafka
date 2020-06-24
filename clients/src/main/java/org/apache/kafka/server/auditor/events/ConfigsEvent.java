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

package org.apache.kafka.server.auditor.events;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.AuditInfo;

import java.util.Collections;
import java.util.Map;

@InterfaceStability.Evolving
public class ConfigsEvent extends AuditEvent {

    public enum EventType {
        ALTER, INCREMENTAL_ALTER, DESCRIBE;
    }

    private final Map<ConfigResource, AuditInfo> auditInfo;
    private final EventType eventType;

    public static ConfigsEvent configsDescribeEvent(Map<ConfigResource, AuditInfo> auditInfo) {
        return new ConfigsEvent(auditInfo, EventType.DESCRIBE);
    }

    public static ConfigsEvent configsAlterEvent(Map<ConfigResource, AuditInfo> auditInfo) {
        return new ConfigsEvent(auditInfo, EventType.ALTER);
    }

    public static ConfigsEvent configsIncrementalAlterEvent(Map<ConfigResource, AuditInfo> auditInfo) {
        return new ConfigsEvent(auditInfo, EventType.INCREMENTAL_ALTER);
    }

    public ConfigsEvent(Map<ConfigResource, AuditInfo> auditInfo, EventType eventType) {
        this.auditInfo = Collections.unmodifiableMap(auditInfo);
        this.eventType = eventType;
    }

    public Map<ConfigResource, AuditInfo> auditedConfigs() {
        return auditInfo;
    }

    public EventType eventType() {
        return eventType;
    }
}
