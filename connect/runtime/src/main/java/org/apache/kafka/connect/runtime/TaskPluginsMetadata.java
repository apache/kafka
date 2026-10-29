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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TaskPluginsMetadata {

    private final String connectorClass;
    private final String connectorVersion;
    private final ConnectorType connectorType;
    private final String taskClass;
    private final String taskVersion;
    private final String keyConverterClass;
    private final String keyConverterVersion;
    private final String valueConverterClass;
    private final String valueConverterVersion;
    private final String headerConverterClass;
    private final String headerConverterVersion;
    private final Set<TransformationStage.AliasedPluginInfo> transformations;
    private final Set<TransformationStage.AliasedPluginInfo> predicates;

    public TaskPluginsMetadata(
            Class<? extends Connector> connectorClass,
            Task task,
            Converter keyConverter,
            Converter valueConverter,
            HeaderConverter headerConverter,
            List<TransformationStage.StageInfo> transformationStageInfo,
            Plugins plugins
    ) {

        assert connectorClass != null;
        assert task != null;
        assert keyConverter != null;
        assert valueConverter != null;
        assert headerConverter != null;
        assert transformationStageInfo != null;

        this.connectorClass = connectorClass.getName();
        this.connectorVersion = plugins.pluginVersion(connectorClass.getName(), connectorClass.getClassLoader(), PluginType.SINK, PluginType.SOURCE);
        this.connectorType = ConnectorType.from(connectorClass);
        this.taskClass = task.getClass().getName();
        this.taskVersion = task.version();
        this.keyConverterClass = keyConverter.getClass().getName();
        this.keyConverterVersion = plugins.pluginVersion(keyConverter.getClass().getName(), keyConverter.getClass().getClassLoader(), PluginType.CONVERTER);
        this.valueConverterClass = valueConverter.getClass().getName();
        this.valueConverterVersion = plugins.pluginVersion(valueConverter.getClass().getName(), valueConverter.getClass().getClassLoader(), PluginType.CONVERTER);
        this.headerConverterClass = headerConverter.getClass().getName();
        this.headerConverterVersion = plugins.pluginVersion(headerConverter.getClass().getName(), headerConverter.getClass().getClassLoader(), PluginType.HEADER_CONVERTER);
        this.transformations = transformationStageInfo.stream().map(TransformationStage.StageInfo::transform).collect(Collectors.toSet());
        this.predicates = transformationStageInfo.stream().map(TransformationStage.StageInfo::predicate).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    public String connectorClass() {
        return connectorClass;
    }

    public String connectorVersion() {
        return connectorVersion;
    }

    public ConnectorType connectorType() {
        return connectorType;
    }

    public String taskClass() {
        return taskClass;
    }

    public String taskVersion() {
        return taskVersion;
    }

    public String keyConverterClass() {
        return keyConverterClass;
    }

    public String keyConverterVersion() {
        return keyConverterVersion;
    }

    public String valueConverterClass() {
        return valueConverterClass;
    }

    public String valueConverterVersion() {
        return valueConverterVersion;
    }

    public String headerConverterClass() {
        return headerConverterClass;
    }

    public String headerConverterVersion() {
        return headerConverterVersion;
    }

    public Set<TransformationStage.AliasedPluginInfo> transformations() {
        return transformations;
    }

    public Set<TransformationStage.AliasedPluginInfo> predicates() {
        return predicates;
    }
}
