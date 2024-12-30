package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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
            Connector connector,
            Task task,
            Converter keyConverter,
            Converter valueConverter,
            HeaderConverter headerConverter,
            List<TransformationStage.StageInfo> transformationStageInfo,
            Function<ClassLoader, LoaderSwap> pluginLoaderSwapper
    ) {

        assert connector != null;
        assert task != null;
        assert keyConverter != null;
        assert valueConverter != null;
        assert headerConverter != null;
        assert transformationStageInfo != null;

        this.connectorClass = connector.getClass().getName();
        this.connectorVersion = PluginUtils.getVersionOrUndefined(connector, pluginLoaderSwapper);
        this.connectorType = getConnectorType(connector);
        this.taskClass = task.getClass().getName();
        this.taskVersion = task.version();
        this.keyConverterClass = keyConverter.getClass().getName();
        this.keyConverterVersion = PluginUtils.getVersionOrUndefined(keyConverter, pluginLoaderSwapper);
        this.valueConverterClass = valueConverter.getClass().getName();
        this.valueConverterVersion = PluginUtils.getVersionOrUndefined(valueConverter, pluginLoaderSwapper);
        this.headerConverterClass = headerConverter.getClass().getName();
        this.headerConverterVersion = PluginUtils.getVersionOrUndefined(headerConverter, pluginLoaderSwapper);
        this.transformations = transformationStageInfo.stream().map(TransformationStage.StageInfo::transform).collect(Collectors.toSet());
        this.predicates = transformationStageInfo.stream().map(TransformationStage.StageInfo::predicate).filter(Objects::nonNull).collect(Collectors.toSet());
    }


    public ConnectorType getConnectorType(Connector connector) {
        if (connector instanceof SourceConnector) {
            return ConnectorType.SOURCE;
        } else if (connector instanceof SinkConnector) {
            return ConnectorType.SINK;
        } else {
            return ConnectorType.UNKNOWN;
        }
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
