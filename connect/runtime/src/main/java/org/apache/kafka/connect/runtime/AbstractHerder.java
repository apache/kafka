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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.VersionedPluginLoadingException;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.Stage;
import org.apache.kafka.connect.util.TemporaryStage;

import org.apache.logging.log4j.Level;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_VERSION;
import static org.apache.kafka.connect.runtime.ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.HEADER_CONVERTER_VERSION_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_VERSION_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_VERSION_CONFIG;


/**
 * Abstract Herder implementation which handles connector/task lifecycle tracking. Extensions
 * must invoke the lifecycle hooks appropriately.
 * <p>
 * This class takes the following approach for sending status updates to the backing store:
 *
 * <ol>
 * <li>
 *    When the connector or task is starting, we overwrite the previous state blindly. This ensures that
 *    every rebalance will reset the state of tasks to the proper state. The intuition is that there should
 *    be less chance of write conflicts when the worker has just received its assignment and is starting tasks.
 *    In particular, this prevents us from depending on the generation absolutely. If the group disappears
 *    and the generation is reset, then we'll overwrite the status information with the older (and larger)
 *    generation with the updated one. The danger of this approach is that slow starting tasks may cause the
 *    status to be overwritten after a rebalance has completed.
 *
 * <li>
 *    If the connector or task fails or is shutdown, we use {@link StatusBackingStore#putSafe(ConnectorStatus)},
 *    which provides a little more protection if the worker is no longer in the group (in which case the
 *    task may have already been started on another worker). Obviously this is still racy. If the task has just
 *    started on another worker, we may not have the updated status cached yet. In this case, we'll overwrite
 *    the value which will cause the state to be inconsistent (most likely until the next rebalance). Until
 *    we have proper producer groups with fenced groups, there is not much else we can do.
 * </ol>
 */
public abstract class AbstractHerder implements Herder, TaskStatus.Listener, ConnectorStatus.Listener {

    private static final Logger log = LoggerFactory.getLogger(AbstractHerder.class);

    private final String workerId;
    protected final Worker worker;
    private final String kafkaClusterId;
    protected final StatusBackingStore statusBackingStore;
    protected final ConfigBackingStore configBackingStore;
    private volatile boolean ready = false;
    private final ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy;
    private final ExecutorService connectorExecutor;
    private final Time time;
    protected final Loggers loggers;

    private final CachedConnectors cachedConnectors;

    public AbstractHerder(Worker worker,
                          String workerId,
                          String kafkaClusterId,
                          StatusBackingStore statusBackingStore,
                          ConfigBackingStore configBackingStore,
                          ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                          Time time) {
        this.worker = worker;
        this.worker.herder = this;
        this.workerId = workerId;
        this.kafkaClusterId = kafkaClusterId;
        this.statusBackingStore = statusBackingStore;
        this.configBackingStore = configBackingStore;
        this.connectorClientConfigOverridePolicy = connectorClientConfigOverridePolicy;
        this.connectorExecutor = Executors.newCachedThreadPool();
        this.time = time;
        this.loggers = new Loggers(time);
        this.cachedConnectors = new CachedConnectors(worker.getPlugins());
    }

    @Override
    public String kafkaClusterId() {
        return kafkaClusterId;
    }

    protected abstract int generation();

    protected void startServices() {
        this.worker.start();
        this.statusBackingStore.start();
        this.configBackingStore.start();
    }

    protected void stopServices() {
        this.statusBackingStore.stop();
        this.configBackingStore.stop();
        this.worker.stop();
        this.connectorExecutor.shutdown();
        Utils.closeQuietly(this.connectorClientConfigOverridePolicy, "connector client config override policy");
    }

    protected void ready() {
        this.ready = true;
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public void onStartup(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.RUNNING,
                workerId, generation()));
    }

    @Override
    public void onStop(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, AbstractStatus.State.STOPPED,
                workerId, generation()));
    }

    @Override
    public void onPause(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.PAUSED,
                workerId, generation()));
    }

    @Override
    public void onResume(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, TaskStatus.State.RUNNING,
                workerId, generation()));
    }

    @Override
    public void onShutdown(String connector) {
        statusBackingStore.putSafe(new ConnectorStatus(connector, ConnectorStatus.State.UNASSIGNED,
                workerId, generation()));
    }

    @Override
    public void onFailure(String connector, Throwable cause) {
        statusBackingStore.putSafe(new ConnectorStatus(connector, ConnectorStatus.State.FAILED,
                trace(cause), workerId, generation()));
    }

    @Override
    public void onStartup(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, generation()));
    }

    @Override
    public void onFailure(ConnectorTaskId id, Throwable cause) {
        statusBackingStore.putSafe(new TaskStatus(id, TaskStatus.State.FAILED, workerId, generation(), trace(cause)));
    }

    @Override
    public void onShutdown(ConnectorTaskId id) {
        statusBackingStore.putSafe(new TaskStatus(id, TaskStatus.State.UNASSIGNED, workerId, generation()));
    }

    @Override
    public void onResume(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, generation()));
    }

    @Override
    public void onPause(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.PAUSED, workerId, generation()));
    }

    @Override
    public void onDeletion(String connector) {
        for (TaskStatus status : statusBackingStore.getAll(connector))
            onDeletion(status.id());
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.DESTROYED, workerId, generation()));
    }

    @Override
    public void onDeletion(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.DESTROYED, workerId, generation()));
    }

    public void onRestart(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.RESTARTING,
                workerId, generation()));
    }

    public void onRestart(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RESTARTING, workerId, generation()));
    }

    @Override
    public void pauseConnector(String connector) {
        if (!configBackingStore.contains(connector))
            throw new NotFoundException("Unknown connector " + connector);
        configBackingStore.putTargetState(connector, TargetState.PAUSED);
    }

    @Override
    public void resumeConnector(String connector) {
        if (!configBackingStore.contains(connector))
            throw new NotFoundException("Unknown connector " + connector);
        configBackingStore.putTargetState(connector, TargetState.STARTED);
    }

    @Override
    public Plugins plugins() {
        return worker.getPlugins();
    }

    /*
     * Retrieves raw config map by connector name.
     */
    protected abstract Map<String, String> rawConfig(String connName);

    @Override
    public void connectorConfig(String connName, Callback<Map<String, String>> callback) {
        // Subset of connectorInfo, so piggy back on that implementation
        connectorInfo(connName, (error, result) -> {
            if (error != null)
                callback.onCompletion(error, null);
            else
                callback.onCompletion(null, result.config());
        });
    }

    @Override
    public Collection<String> connectors() {
        return configBackingStore.snapshot().connectors();
    }

    @Override
    public ConnectorInfo connectorInfo(String connector) {
        final ClusterConfigState configState = configBackingStore.snapshot();

        if (!configState.contains(connector))
            return null;
        Map<String, String> config = configState.rawConnectorConfig(connector);

        return new ConnectorInfo(
            connector,
            config,
            configState.tasks(connector),
            connectorType(config)
        );
    }

    @Override
    public ConnectorStateInfo connectorStatus(String connName) {
        ConnectorStatus connector = statusBackingStore.get(connName);
        if (connector == null)
            throw new NotFoundException("No status found for connector " + connName);

        Collection<TaskStatus> tasks = statusBackingStore.getAll(connName);

        ConnectorStateInfo.ConnectorState connectorState = new ConnectorStateInfo.ConnectorState(
                connector.state().toString(), connector.workerId(), connector.trace());
        List<ConnectorStateInfo.TaskState> taskStates = new ArrayList<>();

        for (TaskStatus status : tasks) {
            taskStates.add(new ConnectorStateInfo.TaskState(status.id().task(),
                    status.state().toString(), status.workerId(), status.trace()));
        }

        Collections.sort(taskStates);

        Map<String, String> conf = rawConfig(connName);
        return new ConnectorStateInfo(connName, connectorState, taskStates, connectorType(conf));
    }

    @Override
    public ActiveTopicsInfo connectorActiveTopics(String connName) {
        Collection<String> topics = statusBackingStore.getAllTopics(connName).stream()
                .map(TopicStatus::topic)
                .collect(Collectors.toList());
        return new ActiveTopicsInfo(connName, topics);
    }

    @Override
    public void resetConnectorActiveTopics(String connName) {
        statusBackingStore.getAllTopics(connName).stream()
                .forEach(status -> statusBackingStore.deleteTopic(status.connector(), status.topic()));
    }

    @Override
    public StatusBackingStore statusBackingStore() {
        return statusBackingStore;
    }

    @Override
    public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id) {
        TaskStatus status = statusBackingStore.get(id);

        if (status == null)
            throw new NotFoundException("No status found for task " + id);

        return new ConnectorStateInfo.TaskState(id.task(), status.state().toString(),
                status.workerId(), status.trace());
    }

    protected Map<String, ConfigValue> validateSinkConnectorConfig(SinkConnector connector, ConfigDef configDef, Map<String, String> config) {
        Map<String, ConfigValue> result = configDef.validateAll(config);
        SinkConnectorConfig.validate(config, result);
        return result;
    }

    protected Map<String, ConfigValue> validateSourceConnectorConfig(SourceConnector connector, ConfigDef configDef, Map<String, String> config) {
        return configDef.validateAll(config);
    }

    /**
     * General-purpose validation logic for converters that are configured directly
     * in a connector config (as opposed to inherited from the worker config).
     * @param connectorConfig the configuration for the connector; may not be null
     * @param pluginConfigValue the {@link ConfigValue} for the converter property in the connector config;
     *                          may be null, in which case no validation will be performed under the assumption that the
     *                          connector will use inherit the converter settings from the worker. Some errors encountered
     *                          during validation may be {@link ConfigValue#addErrorMessage(String) added} to this object
     * @param pluginVersionValue the {@link ConfigValue} for the converter version property in the connector config;
     *
     * @param pluginInterface the interface for the plugin type
     *                        (e.g., {@code org.apache.kafka.connect.storage.Converter.class});
     *                        may not be null
     * @param configDefAccessor an accessor that can be used to retrieve a {@link ConfigDef}
     *                          from an instance of the plugin type (e.g., {@code Converter::config});
     *                          may not be null
     * @param pluginName a lowercase, human-readable name for the type of plugin (e.g., {@code "key converter"});
     *                   may not be null
     * @param pluginProperty the property used to define a custom class for the plugin type
     *                       in a connector config (e.g., {@link ConnectorConfig#KEY_CONVERTER_CLASS_CONFIG});
     *                       may not be null
     * @param defaultProperties any default properties to include in the configuration that will be used for
     *                          the plugin; may be null

     * @return a {@link ConfigInfos} object containing validation results for the plugin in the connector config,
     * or null if either no custom validation was performed (possibly because no custom plugin was defined in the
     * connector config), or if custom validation failed

     * @param <T> the plugin class to perform validation for
     */
    @SuppressWarnings("unchecked")
    private <T> ConfigInfos validateConverterConfig(
            Map<String, String> connectorConfig,
            ConfigValue pluginConfigValue,
            ConfigValue pluginVersionValue,
            Class<T> pluginInterface,
            Function<T, ConfigDef> configDefAccessor,
            String pluginName,
            String pluginProperty,
            String pluginVersionProperty,
            Map<String, String> defaultProperties,
            ClassLoader connectorLoader,
            Function<String, TemporaryStage> reportStage
    ) {
        Objects.requireNonNull(connectorConfig);
        Objects.requireNonNull(pluginInterface);
        Objects.requireNonNull(configDefAccessor);
        Objects.requireNonNull(pluginName);
        Objects.requireNonNull(pluginProperty);
        Objects.requireNonNull(pluginVersionProperty);

        String pluginClass = connectorConfig.get(pluginProperty);
        String pluginVersion = connectorConfig.get(pluginVersionProperty);

        if (pluginClass == null
                || pluginConfigValue == null
                || !pluginConfigValue.errorMessages().isEmpty()
                || !pluginVersionValue.errorMessages().isEmpty()
        ) {
            // Either no custom converter was specified, or one was specified but there's a problem with it.
            // No need to proceed any further.
            return null;
        }

        T pluginInstance;
        String stageDescription = "instantiating the connector's " + pluginName + " for validation";
        try (TemporaryStage stage = reportStage.apply(stageDescription)) {
            VersionRange range = PluginUtils.connectorVersionRequirement(pluginVersion);
            pluginInstance = (T) plugins().newPlugin(pluginClass, range, connectorLoader);
        } catch (VersionedPluginLoadingException e) {
            log.error("Failed to load {} class {} with version {}: {}", pluginName, pluginClass, pluginVersion, e);
            pluginConfigValue.addErrorMessage(e.getMessage());
            pluginVersionValue.addErrorMessage(e.getMessage());
            return null;
        } catch (ClassNotFoundException | RuntimeException e) {
            log.error("Failed to instantiate {} class {}; this should have been caught by prior validation logic", pluginName, pluginClass, e);
            pluginConfigValue.addErrorMessage("Failed to load class " + pluginClass + (e.getMessage() != null ? ": " + e.getMessage() : ""));
            return null;
        } catch (InvalidVersionSpecificationException e) {
            // this should have been caught by prior validation logic
            log.error("Invalid version range for {} class {}: {}", pluginName, pluginClass, pluginVersion, e);
            pluginVersionValue.addErrorMessage(e.getMessage());
            return null;
        }

        try {
            ConfigDef configDef;
            stageDescription = "retrieving the configuration definition from the connector's " + pluginName;
            try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                configDef = configDefAccessor.apply(pluginInstance);
            } catch (RuntimeException e) {
                log.error("Failed to load ConfigDef from {} of type {}", pluginName, pluginClass, e);
                pluginConfigValue.addErrorMessage("Failed to load ConfigDef from " + pluginName + (e.getMessage() != null ? ": " + e.getMessage() : ""));
                return null;
            }
            if (configDef == null) {
                log.warn("{}.config() has returned a null ConfigDef; no further preflight config validation for this converter will be performed", pluginClass);
                // Older versions of Connect didn't do any converter validation.
                // Even though converters are technically required to return a non-null ConfigDef object from their config() method,
                // we permit this case in order to avoid breaking existing converters that, despite not adhering to this requirement,
                // can be used successfully with a connector.
                return null;
            }
            final String pluginPrefix = pluginProperty + ".";
            Map<String, String> pluginConfig = Utils.entriesWithPrefix(connectorConfig, pluginPrefix);
            if (defaultProperties != null)
                defaultProperties.forEach(pluginConfig::putIfAbsent);

            List<ConfigValue> configValues;
            stageDescription = "performing config validation for the connector's " + pluginName;
            try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                configValues = configDef.validate(pluginConfig);
            } catch (RuntimeException e) {
                log.error("Failed to perform config validation for {} of type {}", pluginName, pluginClass, e);
                pluginConfigValue.addErrorMessage("Failed to perform config validation for " + pluginName + (e.getMessage() != null ? ": " + e.getMessage() : ""));
                return null;
            }

            return prefixedConfigInfos(configDef.configKeys(), configValues, pluginPrefix);
        } finally {
            Utils.maybeCloseQuietly(pluginInstance, pluginName + " " + pluginClass);
        }
    }

    private ConfigInfos validateAllConverterConfigs(
            Map<String, String> connectorProps,
            Map<String, ConfigValue> validatedConnectorConfig,
            ClassLoader connectorLoader,
            Function<String, TemporaryStage> reportStage
    ) {
        String connType = connectorProps.get(CONNECTOR_CLASS_CONFIG);
        // do custom converter-specific validation
        ConfigInfos headerConverterConfigInfos = validateConverterConfig(
                connectorProps,
                validatedConnectorConfig.get(HEADER_CONVERTER_CLASS_CONFIG),
                validatedConnectorConfig.get(HEADER_CONVERTER_VERSION_CONFIG),
                HeaderConverter.class,
                HeaderConverter::config,
                "header converter",
                HEADER_CONVERTER_CLASS_CONFIG,
                HEADER_CONVERTER_VERSION_CONFIG,
                Collections.singletonMap(ConverterConfig.TYPE_CONFIG, ConverterType.HEADER.getName()),
                connectorLoader,
                reportStage
        );
        ConfigInfos keyConverterConfigInfos = validateConverterConfig(
                connectorProps,
                validatedConnectorConfig.get(KEY_CONVERTER_CLASS_CONFIG),
                validatedConnectorConfig.get(KEY_CONVERTER_VERSION_CONFIG),
                Converter.class,
                Converter::config,
                "key converter",
                KEY_CONVERTER_CLASS_CONFIG,
                KEY_CONVERTER_VERSION_CONFIG,
                Collections.singletonMap(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName()),
                connectorLoader,
                reportStage
        );

        ConfigInfos valueConverterConfigInfos = validateConverterConfig(
                connectorProps,
                validatedConnectorConfig.get(VALUE_CONVERTER_CLASS_CONFIG),
                validatedConnectorConfig.get(VALUE_CONVERTER_VERSION_CONFIG),
                Converter.class,
                Converter::config,
                "value converter",
                VALUE_CONVERTER_CLASS_CONFIG,
                VALUE_CONVERTER_VERSION_CONFIG,
                Collections.singletonMap(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName()),
                connectorLoader,
                reportStage
        );
        return mergeConfigInfos(connType, headerConverterConfigInfos, keyConverterConfigInfos, valueConverterConfigInfos);
    }

    @Override
    public void validateConnectorConfig(Map<String, String> connectorProps, Callback<ConfigInfos> callback) {
        validateConnectorConfig(connectorProps, callback, true);
    }

    @Override
    public void validateConnectorConfig(Map<String, String> connectorProps, Callback<ConfigInfos> callback, boolean doLog) {
        Stage waitingForThread = new Stage(
                "waiting for a new thread to become available for connector validation",
                time.milliseconds()
        );
        callback.recordStage(waitingForThread);
        connectorExecutor.submit(() -> {
            waitingForThread.complete(time.milliseconds());
            try {
                Function<String, TemporaryStage> reportStage = description ->
                        new TemporaryStage(description, callback, time);
                ConfigInfos result = validateConnectorConfig(connectorProps, reportStage, doLog);
                callback.onCompletion(null, result);
            } catch (Throwable t) {
                callback.onCompletion(t, null);
            }
        });
    }

    /**
     * Build the {@link RestartPlan} that describes what should and should not be restarted given the restart request
     * and the current status of the connector and task instances.
     *
     * @param request the restart request; may not be null
     * @return the restart plan, or empty if this worker has no status for the connector named in the request and therefore the
     *         connector cannot be restarted
     */
    public Optional<RestartPlan> buildRestartPlan(RestartRequest request) {
        String connectorName = request.connectorName();
        ConnectorStatus connectorStatus = statusBackingStore.get(connectorName);
        if (connectorStatus == null) {
            return Optional.empty();
        }

        // If requested, mark the connector as restarting
        AbstractStatus.State connectorState = request.shouldRestartConnector(connectorStatus) ? AbstractStatus.State.RESTARTING : connectorStatus.state();
        ConnectorStateInfo.ConnectorState connectorInfoState = new ConnectorStateInfo.ConnectorState(
                connectorState.toString(),
                connectorStatus.workerId(),
                connectorStatus.trace()
        );

        // Collect the task states, If requested, mark the task as restarting
        List<ConnectorStateInfo.TaskState> taskStates = statusBackingStore.getAll(connectorName)
                .stream()
                .map(taskStatus -> {
                    AbstractStatus.State taskState = request.shouldRestartTask(taskStatus) ? AbstractStatus.State.RESTARTING : taskStatus.state();
                    return new ConnectorStateInfo.TaskState(
                            taskStatus.id().task(),
                            taskState.toString(),
                            taskStatus.workerId(),
                            taskStatus.trace()
                    );
                })
                .collect(Collectors.toList());
        // Construct the response from the various states
        Map<String, String> conf = rawConfig(connectorName);
        ConnectorStateInfo stateInfo = new ConnectorStateInfo(
                connectorName,
                connectorInfoState,
                taskStates,
                connectorType(conf)
        );
        return Optional.of(new RestartPlan(request, stateInfo));
    }

    protected boolean connectorUsesConsumer(org.apache.kafka.connect.health.ConnectorType connectorType, Map<String, String> connProps) {
        return connectorType == org.apache.kafka.connect.health.ConnectorType.SINK;
    }

    protected boolean connectorUsesAdmin(org.apache.kafka.connect.health.ConnectorType connectorType, Map<String, String> connProps) {
        if (connectorType == org.apache.kafka.connect.health.ConnectorType.SOURCE) {
            return SourceConnectorConfig.usesTopicCreation(connProps);
        } else {
            return SinkConnectorConfig.hasDlqTopicConfig(connProps);
        }
    }

    protected boolean connectorUsesProducer(org.apache.kafka.connect.health.ConnectorType connectorType, Map<String, String> connProps) {
        return connectorType == org.apache.kafka.connect.health.ConnectorType.SOURCE
            || SinkConnectorConfig.hasDlqTopicConfig(connProps);
    }

    private ConfigInfos validateClientOverrides(
            Map<String, String> connectorProps,
            org.apache.kafka.connect.health.ConnectorType connectorType,
            Class<? extends Connector> connectorClass,
            Function<String, TemporaryStage> reportStage,
            boolean doLog
    ) {
        if (connectorClass == null || connectorType == null) {
            return null;
        }
        AbstractConfig connectorConfig = new AbstractConfig(new ConfigDef(), connectorProps, doLog);
        String connName = connectorProps.get(ConnectorConfig.NAME_CONFIG);
        String connType = connectorProps.get(CONNECTOR_CLASS_CONFIG);
        ConfigInfos producerConfigInfos = null;
        ConfigInfos consumerConfigInfos = null;
        ConfigInfos adminConfigInfos = null;
        String stageDescription = null;

        if (connectorUsesProducer(connectorType, connectorProps)) {
            stageDescription = "validating producer config overrides for the connector";
            try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                producerConfigInfos = validateClientOverrides(
                        connName,
                        ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX,
                        connectorConfig,
                        ProducerConfig.configDef(),
                        connectorClass,
                        connectorType,
                        ConnectorClientConfigRequest.ClientType.PRODUCER,
                        connectorClientConfigOverridePolicy);
            }
        }
        if (connectorUsesAdmin(connectorType, connectorProps)) {
            stageDescription = "validating admin config overrides for the connector";
            try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                adminConfigInfos = validateClientOverrides(
                        connName,
                        ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX,
                        connectorConfig,
                        AdminClientConfig.configDef(),
                        connectorClass,
                        connectorType,
                        ConnectorClientConfigRequest.ClientType.ADMIN,
                        connectorClientConfigOverridePolicy);
            }
        }
        if (connectorUsesConsumer(connectorType, connectorProps)) {
            stageDescription = "validating consumer config overrides for the connector";
            try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                consumerConfigInfos = validateClientOverrides(
                        connName,
                        ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX,
                        connectorConfig,
                        ConsumerConfig.configDef(),
                        connectorClass,
                        connectorType,
                        ConnectorClientConfigRequest.ClientType.CONSUMER,
                        connectorClientConfigOverridePolicy);
            }
        }
        return mergeConfigInfos(connType,
                producerConfigInfos,
                consumerConfigInfos,
                adminConfigInfos
        );
    }

    private ConfigInfos validateConnectorPluginSpecifiedConfigs(
            Map<String, String> connectorProps,
            Map<String, ConfigValue> validatedConnectorConfig,
            ConfigDef enrichedConfigDef,
            Connector connector,
            Function<String, TemporaryStage> reportStage
    ) {
        List<ConfigValue> configValues = new ArrayList<>(validatedConnectorConfig.values());
        Map<String, ConfigKey> configKeys = new LinkedHashMap<>(enrichedConfigDef.configKeys());
        Set<String> allGroups = new LinkedHashSet<>(enrichedConfigDef.groups());

        String connType = connectorProps.get(CONNECTOR_CLASS_CONFIG);
        // do custom connector-specific validation
        ConfigDef configDef;
        String stageDescription = "retrieving the configuration definition from the connector";
        try (TemporaryStage stage = reportStage.apply(stageDescription)) {
            configDef = connector.config();
        }
        if (null == configDef) {
            throw new BadRequestException(
                    String.format(
                            "%s.config() must return a ConfigDef that is not null.",
                            connector.getClass().getName()
                    )
            );
        }

        Config config;
        stageDescription = "performing multi-property validation for the connector";
        try (TemporaryStage stage = reportStage.apply(stageDescription)) {
            config = connector.validate(connectorProps);
        }
        if (null == config) {
            throw new BadRequestException(
                    String.format(
                            "%s.validate() must return a Config that is not null.",
                            connector.getClass().getName()
                    )
            );
        }
        configKeys.putAll(configDef.configKeys());
        allGroups.addAll(configDef.groups());
        configValues.addAll(config.configValues());
        return generateResult(connType, configKeys, configValues, new ArrayList<>(allGroups));
    }

    private void addNullValuedErrors(Map<String, String> connectorProps, Map<String, ConfigValue> validatedConfig) {
        connectorProps.entrySet().stream()
                .filter(e -> e.getValue() == null)
                .map(Map.Entry::getKey)
                .forEach(prop ->
                        validatedConfig.computeIfAbsent(prop, ConfigValue::new)
                                .addErrorMessage("Null value can not be supplied as the configuration value."));
    }

    private ConfigInfos invalidVersionedConnectorValidation(
            Map<String, String> connectorProps,
            VersionedPluginLoadingException e,
            Function<String, TemporaryStage> reportStage
    ) {
        String connType = connectorProps.get(CONNECTOR_CLASS_CONFIG);
        ConfigDef configDef = ConnectorConfig.enrichedConfigDef(worker.getPlugins(), connType);
        Map<String, ConfigValue> validatedConfig;
        try (TemporaryStage stage = reportStage.apply("validating connector configuration")) {
            validatedConfig = configDef.validateAll(connectorProps);
        }
        validatedConfig.get(CONNECTOR_CLASS_CONFIG).addErrorMessage(e.getMessage());
        validatedConfig.get(CONNECTOR_VERSION).addErrorMessage(e.getMessage());
        validatedConfig.get(CONNECTOR_VERSION).recommendedValues(e.availableVersions().stream().map(v -> (Object) v).collect(Collectors.toList()));
        addNullValuedErrors(connectorProps, validatedConfig);
        return generateResult(connType, configDef.configKeys(), new ArrayList<>(validatedConfig.values()), new ArrayList<>(configDef.groups()));
    }

    ConfigInfos validateConnectorConfig(
            Map<String, String> connectorProps,
            Function<String, TemporaryStage> reportStage,
            boolean doLog
    ) {
        String stageDescription;
        if (worker.configTransformer() != null) {
            stageDescription = "resolving transformed configuration properties for the connector";
            try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                connectorProps = worker.configTransformer().transform(connectorProps);
            }
        }
        String connType = connectorProps.get(CONNECTOR_CLASS_CONFIG);
        if (connType == null) {
            throw new BadRequestException("Connector config " + connectorProps + " contains no connector type");
        }

        VersionRange connVersion;
        Connector connector;
        ClassLoader connectorLoader;
        try {
            connVersion = PluginUtils.connectorVersionRequirement(connectorProps.get(CONNECTOR_VERSION));
            connector = cachedConnectors.getConnector(connType, connVersion);
            connectorLoader = plugins().pluginLoader(connType, connVersion);
            log.info("Validating connector {}, version {}", connType, connector.version());
        } catch (VersionedPluginLoadingException e) {
            log.warn("Failed to load connector {} with version {}, skipping additional validations (connector, converters, transformations, client overrides) ",
                    connType, connectorProps.get(CONNECTOR_VERSION), e);
            return invalidVersionedConnectorValidation(connectorProps, e, reportStage);
        } catch (Exception e) {
            throw new BadRequestException(e.getMessage(), e);
        }

        try (LoaderSwap loaderSwap = plugins().withClassLoader(connectorLoader)) {

            ConfigDef enrichedConfigDef;
            Map<String, ConfigValue> validatedConnectorConfig;
            org.apache.kafka.connect.health.ConnectorType connectorType;
            if (connector instanceof SourceConnector) {
                connectorType = org.apache.kafka.connect.health.ConnectorType.SOURCE;
                enrichedConfigDef = ConnectorConfig.enrich(plugins(), SourceConnectorConfig.enrichedConfigDef(plugins(), connectorProps, worker.config()), connectorProps, false);
                stageDescription = "validating source connector-specific properties for the connector";
                try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                    validatedConnectorConfig = validateSourceConnectorConfig((SourceConnector) connector, enrichedConfigDef, connectorProps);
                }
            } else {
                connectorType = org.apache.kafka.connect.health.ConnectorType.SINK;
                enrichedConfigDef = ConnectorConfig.enrich(plugins(), SinkConnectorConfig.enrichedConfigDef(plugins(), connectorProps, worker.config()), connectorProps, false);
                stageDescription = "validating sink connector-specific properties for the connector";
                try (TemporaryStage stage = reportStage.apply(stageDescription)) {
                    validatedConnectorConfig = validateSinkConnectorConfig((SinkConnector) connector, enrichedConfigDef, connectorProps);
                }
            }

            addNullValuedErrors(connectorProps, validatedConnectorConfig);

            ConfigInfos connectorConfigInfo = validateConnectorPluginSpecifiedConfigs(connectorProps, validatedConnectorConfig, enrichedConfigDef, connector, reportStage);
            ConfigInfos converterConfigInfo = validateAllConverterConfigs(connectorProps, validatedConnectorConfig, connectorLoader, reportStage);
            ConfigInfos clientOverrideInfo = validateClientOverrides(connectorProps, connectorType, connector.getClass(), reportStage, doLog);

            return mergeConfigInfos(connType,
                    connectorConfigInfo,
                    clientOverrideInfo,
                    converterConfigInfo
            );
        }
    }

    private static ConfigInfos mergeConfigInfos(String connType, ConfigInfos... configInfosList) {
        int errorCount = 0;
        List<ConfigInfo> configInfoList = new LinkedList<>();
        Set<String> groups = new LinkedHashSet<>();
        for (ConfigInfos configInfos : configInfosList) {
            if (configInfos != null) {
                errorCount += configInfos.errorCount();
                configInfoList.addAll(configInfos.values());
                groups.addAll(configInfos.groups());
            }
        }
        return new ConfigInfos(connType, errorCount, new ArrayList<>(groups), configInfoList);
    }

    private static ConfigInfos validateClientOverrides(String connName,
                                                      String prefix,
                                                      AbstractConfig connectorConfig,
                                                      ConfigDef configDef,
                                                      Class<? extends Connector> connectorClass,
                                                      org.apache.kafka.connect.health.ConnectorType connectorType,
                                                      ConnectorClientConfigRequest.ClientType clientType,
                                                      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        Map<String, Object> clientConfigs = new HashMap<>();
        for (Map.Entry<String, Object> rawClientConfig : connectorConfig.originalsWithPrefix(prefix).entrySet()) {
            String configName = rawClientConfig.getKey();
            Object rawConfigValue = rawClientConfig.getValue();
            ConfigKey configKey = configDef.configKeys().get(configName);
            Object parsedConfigValue = configKey != null
                ? ConfigDef.parseType(configName, rawConfigValue, configKey.type)
                : rawConfigValue;
            clientConfigs.put(configName, parsedConfigValue);
        }
        ConnectorClientConfigRequest connectorClientConfigRequest = new ConnectorClientConfigRequest(
            connName, connectorType, connectorClass, clientConfigs, clientType);
        List<ConfigValue> configValues = connectorClientConfigOverridePolicy.validate(connectorClientConfigRequest);

        return prefixedConfigInfos(configDef.configKeys(), configValues, prefix);
    }

    private static ConfigInfos prefixedConfigInfos(Map<String, ConfigKey> configKeys, List<ConfigValue> configValues, String prefix) {
        int errorCount = 0;
        Set<String> groups = new LinkedHashSet<>();
        List<ConfigInfo> configInfos = new ArrayList<>();

        if (configValues == null) {
            return new ConfigInfos("", 0, new ArrayList<>(groups), configInfos);
        }

        for (ConfigValue validatedConfigValue : configValues) {
            ConfigKey configKey = configKeys.get(validatedConfigValue.name());
            ConfigKeyInfo configKeyInfo = null;
            if (configKey != null) {
                if (configKey.group != null) {
                    groups.add(configKey.group);
                }
                configKeyInfo = convertConfigKey(configKey, prefix);
            }

            ConfigValue configValue = new ConfigValue(prefix + validatedConfigValue.name(), validatedConfigValue.value(),
                    validatedConfigValue.recommendedValues(), validatedConfigValue.errorMessages());
            if (configValue.errorMessages().size() > 0) {
                errorCount++;
            }
            ConfigValueInfo configValueInfo = convertConfigValue(configValue, configKey != null ? configKey.type : null);
            configInfos.add(new ConfigInfo(configKeyInfo, configValueInfo));
        }
        return new ConfigInfos("", errorCount, new ArrayList<>(groups), configInfos);
    }

    // public for testing
    public static ConfigInfos generateResult(String connType, Map<String, ConfigKey> configKeys, List<ConfigValue> configValues, List<String> groups) {
        int errorCount = 0;
        List<ConfigInfo> configInfoList = new LinkedList<>();

        Map<String, ConfigValue> configValueMap = new HashMap<>();
        for (ConfigValue configValue: configValues) {
            String configName = configValue.name();
            configValueMap.put(configName, configValue);
            if (!configKeys.containsKey(configName)) {
                configInfoList.add(new ConfigInfo(null, convertConfigValue(configValue, null)));
                errorCount += configValue.errorMessages().size();
            }
        }

        for (Map.Entry<String, ConfigKey> entry : configKeys.entrySet()) {
            String configName = entry.getKey();
            ConfigKeyInfo configKeyInfo = convertConfigKey(entry.getValue());
            Type type = entry.getValue().type;
            ConfigValueInfo configValueInfo = null;
            if (configValueMap.containsKey(configName)) {
                ConfigValue configValue = configValueMap.get(configName);
                configValueInfo = convertConfigValue(configValue, type);
                errorCount += configValue.errorMessages().size();
            }
            configInfoList.add(new ConfigInfo(configKeyInfo, configValueInfo));
        }
        return new ConfigInfos(connType, errorCount, groups, configInfoList);
    }

    public static ConfigKeyInfo convertConfigKey(ConfigKey configKey) {
        return convertConfigKey(configKey, "");
    }

    private static ConfigKeyInfo convertConfigKey(ConfigKey configKey, String prefix) {
        String name = prefix + configKey.name;
        Type type = configKey.type;
        String typeName = configKey.type.name();

        boolean required = false;
        String defaultValue;
        if (ConfigDef.NO_DEFAULT_VALUE.equals(configKey.defaultValue)) {
            defaultValue = null;
            required = true;
        } else {
            defaultValue = ConfigDef.convertToString(configKey.defaultValue, type);
        }
        String importance = configKey.importance.name();
        String documentation = configKey.documentation;
        String group = configKey.group;
        int orderInGroup = configKey.orderInGroup;
        String width = configKey.width.name();
        String displayName = configKey.displayName;
        List<String> dependents = configKey.dependents;
        return new ConfigKeyInfo(name, typeName, required, defaultValue, importance, documentation, group, orderInGroup, width, displayName, dependents);
    }

    private static ConfigValueInfo convertConfigValue(ConfigValue configValue, Type type) {
        String value = ConfigDef.convertToString(configValue.value(), type);
        List<String> recommendedValues = new LinkedList<>();

        if (type == Type.LIST) {
            for (Object object: configValue.recommendedValues()) {
                recommendedValues.add(ConfigDef.convertToString(object, Type.STRING));
            }
        } else {
            for (Object object : configValue.recommendedValues()) {
                recommendedValues.add(ConfigDef.convertToString(object, type));
            }
        }
        return new ConfigValueInfo(configValue.name(), value, recommendedValues, configValue.errorMessages(), configValue.visible());
    }

    /**
     * Retrieves ConnectorType for the class specified in the connector config
     * @param connConfig the connector config, may be null
     * @return the {@link ConnectorType} of the connector, or {@link ConnectorType#UNKNOWN} if an error occurs or the
     * type cannot be determined
     */
    public ConnectorType connectorType(Map<String, String> connConfig) {
        if (connConfig == null) {
            return ConnectorType.UNKNOWN;
        }
        String connClass = connConfig.get(CONNECTOR_CLASS_CONFIG);
        if (connClass == null) {
            return ConnectorType.UNKNOWN;
        }
        try {
            VersionRange range = PluginUtils.connectorVersionRequirement(connConfig.get(CONNECTOR_VERSION));
            return ConnectorType.from(cachedConnectors.getConnector(connClass, range).getClass());
        } catch (Exception e) {
            log.warn("Unable to retrieve connector type", e);
            return ConnectorType.UNKNOWN;
        }
    }

    /**
     * Checks a given {@link ConfigInfos} for validation error messages and adds an exception
     * to the given {@link Callback} if any were found.
     *
     * @param configInfos configInfos to read Errors from
     * @param callback callback to add config error exception to
     * @return true if errors were found in the config
     */
    protected final boolean maybeAddConfigErrors(
        ConfigInfos configInfos,
        Callback<Created<ConnectorInfo>> callback
    ) {
        int errors = configInfos.errorCount();
        boolean hasErrors = errors > 0;
        if (hasErrors) {
            StringBuilder messages = new StringBuilder();
            messages.append("Connector configuration is invalid and contains the following ")
                .append(errors).append(" error(s):");
            for (ConfigInfo configInfo : configInfos.values()) {
                for (String msg : configInfo.configValue().errors()) {
                    messages.append('\n').append(msg);
                }
            }
            callback.onCompletion(
                new BadRequestException(
                    messages.append(
                        "\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`"
                    ).toString()
                ), null
            );
        }
        return hasErrors;
    }

    private String trace(Throwable t) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            t.printStackTrace(new PrintStream(output, false, StandardCharsets.UTF_8.name()));
            return output.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    /*
     * Performs a reverse transformation on a set of task configs, by replacing values with variable references.
     */
    public static List<Map<String, String>> reverseTransform(String connName,
                                                             ClusterConfigState configState,
                                                             List<Map<String, String>> configs) {

        // Find the config keys in the raw connector config that have variable references
        Map<String, String> rawConnConfig = configState.rawConnectorConfig(connName);
        Set<String> connKeysWithVariableValues = keysWithVariableValues(rawConnConfig, ConfigTransformer.DEFAULT_PATTERN);

        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> config : configs) {
            Map<String, String> newConfig = new HashMap<>(config);
            for (String key : connKeysWithVariableValues) {
                if (newConfig.containsKey(key)) {
                    newConfig.put(key, rawConnConfig.get(key));
                }
            }
            result.add(newConfig);
        }
        return result;
    }

    public static boolean taskConfigsChanged(ClusterConfigState configState, String connName, List<Map<String, String>> rawTaskProps) {
        int currentNumTasks = configState.taskCount(connName);
        boolean result = false;
        if (rawTaskProps.size() != currentNumTasks) {
            log.debug("Connector {} task count changed from {} to {}", connName, currentNumTasks, rawTaskProps.size());
            result = true;
        }
        if (!result) {
            for (int index = 0; index < currentNumTasks; index++) {
                ConnectorTaskId taskId = new ConnectorTaskId(connName, index);
                if (!rawTaskProps.get(index).equals(configState.rawTaskConfig(taskId))) {
                    log.debug("Connector {} has change in configuration for task {}-{}", connName, connName, index);
                    result = true;
                }
            }
        }
        if (!result) {
            Map<String, String> appliedConnectorConfig = configState.appliedConnectorConfig(connName);
            Map<String, String> currentConnectorConfig = configState.connectorConfig(connName);
            if (!Objects.equals(appliedConnectorConfig, currentConnectorConfig)) {
                log.debug("Forcing task restart for connector {} as its configuration appears to be updated", connName);
                result = true;
            }
        }
        if (result) {
            log.debug("Reconfiguring connector {}: writing new updated configurations for tasks", connName);
        } else {
            log.debug("Skipping reconfiguration of connector {} as generated configs appear unchanged", connName);
        }
        return result;
    }

    // Visible for testing
    static Set<String> keysWithVariableValues(Map<String, String> rawConfig, Pattern pattern) {
        Set<String> keys = new HashSet<>();
        for (Map.Entry<String, String> config : rawConfig.entrySet()) {
            if (config.getValue() != null) {
                Matcher matcher = pattern.matcher(config.getValue());
                if (matcher.find()) {
                    keys.add(config.getKey());
                }
            }
        }
        return keys;
    }

    @Override
    public List<ConfigKeyInfo> connectorPluginConfig(String pluginName) {
        return connectorPluginConfig(pluginName, null);
    }

    @Override
    public List<ConfigKeyInfo> connectorPluginConfig(String pluginName, VersionRange range) {

        Plugins p = plugins();
        Class<?> pluginClass;
        try {
            pluginClass = p.pluginClass(pluginName, range);
        } catch (ClassNotFoundException cnfe) {
            throw new NotFoundException("Unknown plugin " + pluginName + ".");
        } catch (VersionedPluginLoadingException e) {
            throw new BadRequestException(e.getMessage(), e);
        }

        try (LoaderSwap loaderSwap = p.withClassLoader(pluginClass.getClassLoader())) {
            Object plugin = p.newPlugin(pluginName, range);
            // Contains definitions coming from Connect framework
            ConfigDef baseConfigDefs = null;
            // Contains definitions specifically declared on the plugin
            ConfigDef pluginConfigDefs;
            if (plugin instanceof SinkConnector) {
                baseConfigDefs = SinkConnectorConfig.enrichedConfigDef(p, pluginName);
                pluginConfigDefs = ((SinkConnector) plugin).config();
            } else if (plugin instanceof SourceConnector) {
                baseConfigDefs = SourceConnectorConfig.enrichedConfigDef(p, pluginName);
                pluginConfigDefs = ((SourceConnector) plugin).config();
            } else if (plugin instanceof Converter) {
                pluginConfigDefs = ((Converter) plugin).config();
            } else if (plugin instanceof HeaderConverter) {
                pluginConfigDefs = ((HeaderConverter) plugin).config();
            } else if (plugin instanceof Transformation) {
                pluginConfigDefs = ((Transformation<?>) plugin).config();
            } else if (plugin instanceof Predicate) {
                pluginConfigDefs = ((Predicate<?>) plugin).config();
            } else {
                throw new BadRequestException("Invalid plugin class " + pluginName + ". Valid types are sink, source, converter, header_converter, transformation, predicate.");
            }

            // Track config properties by name and, if the same property is defined in multiple places,
            // give precedence to the one defined by the plugin class
            // Preserve the ordering of properties as they're returned from each ConfigDef
            Map<String, ConfigKey> configsMap = new LinkedHashMap<>(pluginConfigDefs.configKeys());
            if (baseConfigDefs != null) {
                baseConfigDefs.configKeys().forEach(configsMap::putIfAbsent);
            }

            List<ConfigKeyInfo> results = new ArrayList<>();
            for (ConfigKey configKey : configsMap.values()) {
                results.add(AbstractHerder.convertConfigKey(configKey));
            }
            return results;
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Failed to load plugin class or one of its dependencies", e);
        }
    }

    @Override
    public void connectorOffsets(String connName, Callback<ConnectorOffsets> cb) {
        ClusterConfigState configSnapshot = configBackingStore.snapshot();
        try {
            if (!configSnapshot.contains(connName)) {
                cb.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                return;
            }
            // The worker asynchronously processes the request and completes the passed callback when done
            worker.connectorOffsets(connName, configSnapshot.connectorConfig(connName), cb);
        } catch (Throwable t) {
            cb.onCompletion(t, null);
        }
    }

    @Override
    public void alterConnectorOffsets(String connName, Map<Map<String, ?>, Map<String, ?>> offsets, Callback<Message> callback) {
        if (offsets == null || offsets.isEmpty()) {
            callback.onCompletion(new ConnectException("The offsets to be altered may not be null or empty"), null);
            return;
        }
        modifyConnectorOffsets(connName, offsets, callback);
    }

    @Override
    public void resetConnectorOffsets(String connName, Callback<Message> callback) {
        modifyConnectorOffsets(connName, null, callback);
    }

    /**
     * Service external requests to alter or reset connector offsets.
     * @param connName the name of the connector whose offsets are to be modified
     * @param offsets the offsets to be written; this should be {@code null} for offsets reset requests
     * @param cb callback to invoke upon completion
     */
    protected abstract void modifyConnectorOffsets(String connName, Map<Map<String, ?>, Map<String, ?>> offsets, Callback<Message> cb);

    @Override
    public LoggerLevel loggerLevel(String logger) {
        return loggers.level(logger);
    }

    @Override
    public Map<String, LoggerLevel> allLoggerLevels() {
        return loggers.allLevels();
    }

    @Override
    public List<String> setWorkerLoggerLevel(String namespace, String desiredLevelStr) {
        Level level = Level.toLevel(desiredLevelStr.toUpperCase(Locale.ROOT), null);

        if (level == null) {
            log.warn("Ignoring request to set invalid level '{}' for namespace {}", desiredLevelStr, namespace);
            return Collections.emptyList();
        }

        return loggers.setLevel(namespace, level);
    }
}
