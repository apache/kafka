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
package org.apache.kafka.log4j2appender;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.status.StatusLogger;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Sends log events to Kafka topic.
 */
@Plugin(name = "KafkaAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class KafkaAppender extends AbstractAppender {

    private final KafkaProducerManager manager;

    @PluginBuilderFactory
    public static KafkaAppenderBuilder newBuilder() {
        return new KafkaAppenderBuilder();
    }

    public static class KafkaAppenderBuilder implements org.apache.logging.log4j.core.util.Builder<KafkaAppender> {

        @PluginBuilderAttribute
        private boolean ignoreExceptions = true;

        @PluginBuilderAttribute
        @Required(message = "No appender name provided")
        private String name;

        @PluginElement("Layout")
        private Layout<String> layout;

        @PluginElement("Filter")
        private Filter filter;

        // Properties from log4j-appender (common)
        @PluginBuilderAttribute
        private String brokerList; // deprecated, in preference of bootstrapServers.
        @PluginBuilderAttribute
        private String topic;
        @PluginBuilderAttribute
        private String compressionType;
        @PluginBuilderAttribute
        private Integer maxBlockMs;
        @PluginBuilderAttribute
        private Integer retries;
        @PluginBuilderAttribute
        private String requiredNumAcks; // deprecated, in preference of acks.
        @PluginBuilderAttribute
        private Integer deliveryTimeoutMs;
        @PluginBuilderAttribute
        private Integer lingerMs;
        @PluginBuilderAttribute
        private Integer batchSize;
        @PluginBuilderAttribute
        private boolean syncSend;

        // Properties from log4j-appender (security)
        @PluginBuilderAttribute
        private String securityProtocol;
        @PluginBuilderAttribute
        private String sslEngineFactoryClass;
        @PluginBuilderAttribute
        private String sslTruststoreLocation;
        @PluginBuilderAttribute
        private String sslTruststorePassword;
        @PluginBuilderAttribute
        private String sslKeystoreType;
        @PluginBuilderAttribute
        private String sslKeystoreLocation;
        @PluginBuilderAttribute
        private String sslKeystorePassword;
        @PluginBuilderAttribute
        private String saslKerberosServiceName;
        @PluginBuilderAttribute
        private String clientJaasConfPath;
        @PluginBuilderAttribute
        private String saslMechanism;
        @PluginBuilderAttribute
        private String clientJaasConf;
        @PluginBuilderAttribute
        private String kerb5ConfPath;

        // Properties from log4j2-appender
        @PluginBuilderAttribute
        private String bootstrapServers;
        @PluginBuilderAttribute
        private String acks;
        @PluginBuilderAttribute
        private String producerClass = KafkaProducer.class.getName();

        public boolean isIgnoreExceptions() {
            return ignoreExceptions;
        }

        public String getName() {
            return this.name;
        }

        public Layout<String> getLayout() {
            return this.layout;
        }

        public Layout<? extends Serializable> getOrCreateLayout() {
            if (layout == null) {
                return PatternLayout.createDefaultLayout();
            }
            return layout;
        }

        public Layout<? extends Serializable> getOrCreateLayout(final Charset charset) {
            if (layout == null) {
                return PatternLayout.newBuilder().withCharset(charset).build();
            }
            return layout;
        }

        public Filter getFilter() {
            return this.filter;
        }

        public String getBrokerList() {
            return brokerList;
        }

        public String getTopic() {
            return topic;
        }

        public String getCompressionType() {
            return compressionType;
        }

        public Integer getMaxBlockMs() {
            return maxBlockMs;
        }

        public Integer getRetries() {
            return retries;
        }

        public String getRequiredNumAcks() {
            return requiredNumAcks;
        }

        public Integer getDeliveryTimeoutMs() {
            return deliveryTimeoutMs;
        }

        public Integer getLingerMs() {
            return lingerMs;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public boolean isSyncSend() {
            return syncSend;
        }

        public String getSecurityProtocol() {
            return securityProtocol;
        }

        public String getSslEngineFactoryClass() {
            return sslEngineFactoryClass;
        }

        public String getSslTruststoreLocation() {
            return sslTruststoreLocation;
        }

        public String getSslTruststorePassword() {
            return sslTruststorePassword;
        }

        public String getSslKeystoreType() {
            return sslKeystoreType;
        }

        public String getSslKeystoreLocation() {
            return sslKeystoreLocation;
        }

        public String getSslKeystorePassword() {
            return sslKeystorePassword;
        }

        public String getSaslKerberosServiceName() {
            return saslKerberosServiceName;
        }

        public String getClientJaasConfPath() {
            return clientJaasConfPath;
        }

        public String getSaslMechanism() {
            return saslMechanism;
        }

        public String getClientJaasConf() {
            return clientJaasConf;
        }

        public String getKerb5ConfPath() {
            return kerb5ConfPath;
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public String getAcks() {
            return acks;
        }

        public String getProducerClass() {
            return producerClass;
        }

        public KafkaAppenderBuilder setIgnoreExceptions(final boolean ignoreExceptions) {
            this.ignoreExceptions = ignoreExceptions;
            return this;
        }

        public KafkaAppenderBuilder setName(String value) {
            this.name = value;
            return this;
        }

        public KafkaAppenderBuilder setLayout(Layout<String> value) {
            this.layout = value;
            return this;
        }

        public KafkaAppenderBuilder setFilter(Filter value) {
            this.filter = value;
            return this;
        }

        public KafkaAppenderBuilder setBrokerList(final String brokerList) {
            this.brokerList = brokerList;
            return this;
        }

        public KafkaAppenderBuilder setTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public KafkaAppenderBuilder setCompressionType(final String compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public KafkaAppenderBuilder setMaxBlockMs(final Integer maxBlockMs) {
            this.maxBlockMs = maxBlockMs;
            return this;
        }

        public KafkaAppenderBuilder setRetries(final Integer retries) {
            this.retries = retries;
            return this;
        }

        public KafkaAppenderBuilder setRequiredNumAcks(final String requiredNumAcks) {
            this.requiredNumAcks = requiredNumAcks;
            return this;
        }

        public KafkaAppenderBuilder setDeliveryTimeoutMs(final Integer deliveryTimeoutMs) {
            this.deliveryTimeoutMs = deliveryTimeoutMs;
            return this;
        }

        public KafkaAppenderBuilder setLingerMs(final Integer lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public KafkaAppenderBuilder setBatchSize(final Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public KafkaAppenderBuilder setSyncSend(final boolean syncSend) {
            this.syncSend = syncSend;
            return this;
        }

        public KafkaAppenderBuilder setSecurityProtocol(final String securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public KafkaAppenderBuilder setSslEngineFactoryClass(final String sslEngineFactoryClass) {
            this.sslEngineFactoryClass = sslEngineFactoryClass;
            return this;
        }

        public KafkaAppenderBuilder setSslTruststoreLocation(final String sslTruststoreLocation) {
            this.sslTruststoreLocation = sslTruststoreLocation;
            return this;
        }

        public KafkaAppenderBuilder setSslTruststorePassword(final String sslTruststorePassword) {
            this.sslTruststorePassword = sslTruststorePassword;
            return this;
        }

        public KafkaAppenderBuilder setSslKeystoreType(final String sslKeystoreType) {
            this.sslKeystoreType = sslKeystoreType;
            return this;
        }

        public KafkaAppenderBuilder setSslKeystoreLocation(final String sslKeystoreLocation) {
            this.sslKeystoreLocation = sslKeystoreLocation;
            return this;
        }

        public KafkaAppenderBuilder setSslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        public KafkaAppenderBuilder setSaslKerberosServiceName(final String saslKerberosServiceName) {
            this.saslKerberosServiceName = saslKerberosServiceName;
            return this;
        }

        public KafkaAppenderBuilder setClientJaasConfPath(final String clientJaasConfPath) {
            this.clientJaasConfPath = clientJaasConfPath;
            return this;
        }

        public KafkaAppenderBuilder setSaslMechanism(final String saslMechanism) {
            this.saslMechanism = saslMechanism;
            return this;
        }

        public KafkaAppenderBuilder setClientJaasConf(final String clientJaasConf) {
            this.clientJaasConf = clientJaasConf;
            return this;
        }

        public KafkaAppenderBuilder setKerb5ConfPath(final String kerb5ConfPath) {
            this.kerb5ConfPath = kerb5ConfPath;
            return this;
        }

        public KafkaAppenderBuilder setBootstrapServers(final String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public KafkaAppenderBuilder setAcks(final String acks) {
            this.acks = acks;
            return this;
        }

        public KafkaAppenderBuilder setProducerClass(final String producerClass) {
            this.producerClass = producerClass;
            return this;
        }

        @Override
        @SuppressWarnings({"checkstyle:Complexity", "checkstyle:NPathComplexity"})
        public KafkaAppender build() {
            final Properties properties = new Properties();

            if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
                if (brokerList != null && !brokerList.isEmpty()) {
                    StatusLogger.getLogger().warn("brokerList property is deprecated; Using bootstrapServers property.");
                }
                properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            } else {
                if (brokerList != null && !brokerList.isEmpty()) {
                    StatusLogger.getLogger().warn("brokerList property is deprecated; Use bootstrapServers property.");
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
                } else {
                    throw new ConfigException("The bootstrapServers property should be specified");
                }
            }

            if (topic == null || topic.isEmpty()) {
                throw new ConfigException("Topic must be specified by the Kafka log4j2 appender");
            }

            if (compressionType != null)
                properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

            if (maxBlockMs != null)
                properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);

            if (retries != null)
                properties.put(ProducerConfig.RETRIES_CONFIG, retries);

            if (acks != null) {
                if (requiredNumAcks != null) {
                    StatusLogger.getLogger().warn("requiredNumAcks property is deprecated; Using acks property.");
                }
                properties.put(ProducerConfig.ACKS_CONFIG, acks);
            } else {
                if (requiredNumAcks != null) {
                    StatusLogger.getLogger().warn("requiredNumAcks property is deprecated; Use acks property.");
                    properties.put(ProducerConfig.ACKS_CONFIG, requiredNumAcks);
                }
            }

            if (deliveryTimeoutMs != null)
                properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);

            if (lingerMs != null)
                properties.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);

            if (batchSize != null)
                properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);

            if (securityProtocol != null) {
                properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

                Set<SecurityProtocol> protocols = Stream.of(securityProtocol.split(","))
                    .map((String token) -> SecurityProtocol.forName(token))
                    .collect(Collectors.toSet());

                if (protocols.contains(SecurityProtocol.SSL)
                    || protocols.contains(SecurityProtocol.SASL_PLAINTEXT)
                    || protocols.contains(SecurityProtocol.SASL_SSL)) {
                    if (sslEngineFactoryClass != null) {
                        properties.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, sslEngineFactoryClass);
                    }
                }

                if (protocols.contains(SecurityProtocol.SSL)) {
                    if (sslTruststoreLocation != null && sslTruststorePassword != null) {
                        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
                        properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
                    }

                    if (sslKeystoreType != null && sslKeystoreLocation != null && sslKeystorePassword != null) {
                        properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
                        properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
                        properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
                    }
                }

                if (protocols.contains(SecurityProtocol.SASL_PLAINTEXT)
                    || protocols.contains(SecurityProtocol.SASL_SSL)) {
                    if (saslKerberosServiceName != null && clientJaasConfPath != null) {
                        properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, saslKerberosServiceName);
                        System.setProperty("java.security.auth.login.config", clientJaasConfPath);
                    }
                }
            }

            if (saslMechanism != null)
                properties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

            if (clientJaasConf != null)
                properties.put(SaslConfigs.SASL_JAAS_CONFIG, clientJaasConf);

            if (kerb5ConfPath != null)
                System.setProperty("java.security.krb5.conf", kerb5ConfPath);

            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.ByteArray().serializer().getClass().getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.ByteArray().serializer().getClass().getName());

            final MethodHandle producerConstructor;
            try {
                producerConstructor = MethodHandles.lookup().findConstructor(
                    Class.forName(producerClass),
                    MethodType.methodType(void.class, Properties.class));
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }

            final KafkaProducerManager producerManager =
                new KafkaProducerManager(producerConstructor, properties, topic, syncSend, isIgnoreExceptions());

            StatusLogger.getLogger().debug("Kafka producer connected to " + properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            StatusLogger.getLogger().debug("Logging for topic: " + topic);

            return new KafkaAppender(getName(), getLayout(), getFilter(), isIgnoreExceptions(), producerManager);
        }
    }

    private KafkaAppender(final String name, final Layout<? extends Serializable> layout, final Filter filter,
                          final boolean ignoreExceptions, final KafkaProducerManager manager) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        Objects.requireNonNull(layout, "layout");
        this.manager = Objects.requireNonNull(manager, "manager");
    }

    String getTopic() {
        return manager.getTopic();
    }

    Producer<byte[], byte[]> getProducer() {
        return manager.getProducer();
    }

    Properties getProperties() {
        return manager.getProperties();
    }

    @Override
    public void append(LogEvent event) {
        final byte[] data = getLayout().toByteArray(event);
        StatusLogger.getLogger().debug("[" + new Date(event.getTimeMillis()) + "]" + new String(data, StandardCharsets.UTF_8));
        manager.send(data);
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        setStopping();
        boolean stopped = super.stop(timeout, timeUnit, false);
        try {
            manager.close();
        } catch (Exception e) {
            StatusLogger.getLogger().error("Unable to close KafkaManager", e);
        }
        setStopped();
        return stopped;
    }
}
