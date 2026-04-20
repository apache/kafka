package org.apache.kafka.server.policy;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ClientConfigPolicyException;
import org.apache.kafka.common.errors.ConfigTooLargeException;
import org.apache.kafka.common.errors.UnknownConfigProfileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PocClientConfigPolicy implements ClientConfigPolicy {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Optional<ClientConfigProfileKeys> profileKeys(ClientProfile clientProfile) throws UnknownConfigProfileException {
        log.debug("GetConfigProfileKeys - profileKeys() called 😀");
        log.debug("metadata:");
        clientProfile.clientMetadata().entrySet().forEach(e -> log.debug("    {}", e));

        String role = clientProfile.clientMetadata().get("apache-kafka-java.role");
        String workloadId = clientProfile.clientMetadata().get("workload.id");

        SortedSet<String> standardConfigKeys = new TreeSet<>(
            Set.of(
                "client.id",
                "request.timeout.ms",
                "retry.backoff.ms",
                "metadata.max.age.ms",
                "send.buffer.bytes",
                "receive.buffer.bytes",
                "reconnect.backoff.ms",
                "reconnect.backoff.max.ms"
            )
        );

        if (role != null) {
            if (role.equals("producer")) {
                standardConfigKeys.add("acks");
                standardConfigKeys.add("compression.type");

                if (workloadId != null && workloadId.equals("fastest"))
                    standardConfigKeys.add("linger.ms");
            } else if (role.equals("consumer")) {
                standardConfigKeys.add("enable.auto.commit");
                standardConfigKeys.add("group.protocol");

                if (workloadId != null && workloadId.equals("fastest"))
                    standardConfigKeys.add("max.poll.records");
            }
        } else {
            // What to do here?
        }

        long crc = configurationProfileCrc(standardConfigKeys);

        log.debug("configKeys:");
        standardConfigKeys.forEach(c -> log.debug("    {}", c));
        log.debug("crc: {}", crc);

        return Optional.of(new ClientConfigProfileKeys(standardConfigKeys, crc));
    }

    @Override
    public void process(ClientPushConfigData pushConfigData) throws UnknownConfigProfileException, ConfigTooLargeException, ClientConfigPolicyException {
        log.debug("PushConfig - process() called 😀");

        log.debug("metadata:");
        pushConfigData.clientProfile().clientMetadata().entrySet().forEach(e -> log.debug("    {}", e));

        log.debug("configs:");
        pushConfigData.configs().entrySet().forEach(e -> log.debug("    {}={}", e.getKey(), e.getValue()));
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Set.of();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

    }

    @Override
    public void reconfigure(Map<String, ?> configs) {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() throws Exception {
        ClientConfigPolicy.super.close();
    }
}
