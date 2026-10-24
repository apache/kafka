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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.server.util.MirrorUtils.compilePattern;
import static org.apache.kafka.server.util.MirrorUtils.compilePatternList;

/**
 * Cluster Mirror configurations.
 * Set when creating/altering a specific mirror.
 * Stored in cluster metadata.
 */
public final class ClusterMirrorConfig extends AbstractConfig {
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    private static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the source cluster. " +
            "This is a required configuration when creating a cluster mirror.";

    public static final String SOURCE_CLUSTER_ID_CONFIG = CommonClientConfigs.SOURCE_CLUSTER_ID_CONFIG;
    private static final String SOURCE_CLUSTER_ID_DOC = CommonClientConfigs.SOURCE_CLUSTER_ID_DOC;

    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    private static final int REQUEST_TIMEOUT_MS_DEFAULT = 30 * 1000;

    public static final String CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    private static final String CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;
    private static final long CONNECTION_SETUP_TIMEOUT_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;

    public static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    private static final String CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;
    private static final long CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
    private static final String CONNECTIONS_MAX_IDLE_MS_DOC = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC;
    private static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = 10 * 60 * 1000L;

    public static final String SOCKET_TIMEOUT_MS_CONFIG = "socket.timeout.ms";
    private static final String SOCKET_TIMEOUT_MS_DOC = "The socket timeout for network requests.";
    private static final int SOCKET_TIMEOUT_MS_DEFAULT = 30 * 1000;

    public static final String SOCKET_RECEIVE_BUFFER_BYTES_CONFIG = "socket.receive.buffer.bytes";
    private static final String SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The socket receive buffer for network requests to the source cluster.";
    private static final int SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 64 * 1024;

    public static final String FETCH_WAIT_MAX_MS_CONFIG = "fetch.wait.max.ms";
    private static final String FETCH_WAIT_MAX_MS_DOC = "The maximum wait time for each fetcher request issued by mirror replicas.";
    private static final int FETCH_WAIT_MAX_MS_DEFAULT = 1000;

    public static final String FETCH_BACKOFF_MS_CONFIG = "fetch.backoff.ms";
    private static final String FETCH_BACKOFF_MS_DOC = "The amount of time to wait before retrying mirror fetch requests after a failure. " +
            "This controls the backoff for mirror fetcher threads on connection errors or other exceptions from the source cluster.";
    private static final long FETCH_BACKOFF_MS_DEFAULT = 1000;

    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    private static final String FETCH_MIN_BYTES_DOC = "Minimum bytes expected for each fetch response.";
    private static final int FETCH_MIN_BYTES_DEFAULT = 1;

    public static final String FETCH_RESPONSE_MAX_BYTES_CONFIG = "fetch.response.max.bytes";
    private static final String FETCH_RESPONSE_MAX_BYTES_DOC = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition is larger than this value, the record batch will still be returned.";
    private static final int FETCH_RESPONSE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024;

    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    private static final String FETCH_MAX_BYTES_DOC = "The number of bytes of messages to attempt to fetch for each partition.";
    private static final int FETCH_MAX_BYTES_DEFAULT = 1024 * 1024;

    public static final String TOPIC_PROPERTIES_EXCLUDE_CONFIG = "topic.properties.exclude";
    private static final String TOPIC_PROPERTIES_EXCLUDE_DOC = "A comma-separated list of topic config property names and/or regexes to exclude from synchronization. "
            + "Properties in this list will not be replicated from the source cluster.";
    // The message.timestamp.difference.max.ms property was removed in 4.0 but may still be used by older clusters
    private static final String TOPIC_PROPERTIES_EXCLUDE_DEFAULT =
            "follower.replication.throttled.replicas,"
                    + "leader.replication.throttled.replicas,"
                    + "message.timestamp.difference.max.ms,"
                    + "message.timestamp.before.max.ms,"
                    + "message.timestamp.after.max.ms,"
                    + "message.timestamp.type,"
                    + "unclean.leader.election.enable,"
                    + "min.insync.replicas,"
                    + "mirror.replication.throttled.replicas,"
                    + "mirror.unclean.leader.election.enable";

    public static final String TOPICS_INCLUDE_CONFIG = "topics.include";
    private static final String TOPICS_INCLUDE_DOC = "A comma-separated list of regex patterns for topic names to include in mirroring. "
            + "Topics on the source cluster whose names match at least one of the patterns will be automatically discovered and mirrored.";
    private static final String TOPICS_INCLUDE_DEFAULT = ".*";

    public static final String TOPICS_EXCLUDE_CONFIG = "topics.exclude";
    private static final String TOPICS_EXCLUDE_DOC = "A comma-separated list of regex patterns for topic names to exclude from mirroring. "
            + "Internal topics are excluded by default. Exclusions take precedence over inclusions.";
    private static final String TOPICS_EXCLUDE_DEFAULT = "__.*";

    public static final String GROUPS_INCLUDE_CONFIG = "groups.include";
    private static final String GROUPS_INCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to include in offset synchronization. "
            + "Only consumer groups whose IDs match at least one of the patterns will have their offsets replicated from the source cluster.";
    private static final String GROUPS_INCLUDE_DEFAULT = ".*";

    public static final String GROUPS_EXCLUDE_CONFIG = "groups.exclude";
    private static final String GROUPS_EXCLUDE_DOC = "A comma-separated list of regex patterns for consumer group IDs to exclude from offset synchronization. " +
            "Exclusions take precedence over inclusions.";
    private static final String GROUPS_EXCLUDE_DEFAULT = "";

    public static final String ACLS_INCLUDE_CONFIG = "acls.include";
    private static final String ACLS_INCLUDE_DOC = "ACL include rules. " + MirrorAclRule.DOC
            + "Examples: .* (match all ACLs), TOPIC;orders.* (all ACLs for topics matching orders.*), "
            + "TOPIC;.*;READ;ALLOW||TOPIC;.*;WRITE;ALLOW (all topic READ and WRITE ALLOW ACLs), "
            + "ANY;.*;ANY;ANY;User:alice (all ACLs for principal User:alice).";
    private static final String ACLS_INCLUDE_DEFAULT = ".*";

    public static final String ACLS_EXCLUDE_CONFIG = "acls.exclude";
    private static final String ACLS_EXCLUDE_DOC = "ACL exclude rules. " + MirrorAclRule.DOC
            + "Exclusions take precedence over inclusions. "
            + "Examples: .* (exclude all ACLs), ANY;.*;WRITE (exclude all WRITE ACLs), "
            + "ANY;.*;ANY;ANY;User:bob (exclude all ACLs for User:bob).";
    private static final String ACLS_EXCLUDE_DEFAULT = "";

    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    private static final String SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;
    private static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name;

    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    private static final ConfigDef.Validator SOCKET_RECEIVE_BUFFER_BYTES_VALIDATOR = ConfigDef.LambdaValidator.with(
            (name, value) -> {
                int size = (int) value;
                if (size != -1 && size < 1) {
                    throw new ConfigException(
                            name,
                            value,
                            "Must be -1 or a positive integer."
                    );
                }
            },
            () -> "Must be -1 or a positive integer"
    );

    @SuppressWarnings("unchecked")
    private static final ConfigDef.Validator REGEX_LIST_VALIDATOR = ConfigDef.LambdaValidator.with(
            (name, value) -> {
                if (value == null) return;
                List<String> patterns = (List<String>) value;
                for (String pattern : patterns) {
                    String trimmed = pattern.trim();
                    if (!trimmed.isEmpty()) {
                        try {
                            Pattern.compile(trimmed);
                        } catch (PatternSyntaxException e) {
                            throw new ConfigException(name, value, "Invalid regex pattern: " + e.getMessage());
                        }
                    }
                }
            },
            () -> "Valid comma-separated list of regex patterns"
    );

    private static final ConfigDef.Validator ACL_RULES_VALIDATOR = ConfigDef.LambdaValidator.with(
            (name, value) -> {
                if (value == null) return;
                String rules = (String) value;
                try {
                    MirrorAclRule.parseList(rules);
                } catch (Exception e) {
                    throw new ConfigException(name, value, "Invalid ACL rule: " + e.getMessage());
                }
            },
            () -> "Valid ACL rules separated by || with five semicolon-separated fields"
    );

    private static final ConfigDef CONFIG_DEF = buildConfigDef();

    private static ConfigDef buildConfigDef() {
        ConfigDef configDef = new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, LIST, null, HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(SOURCE_CLUSTER_ID_CONFIG, STRING, null, HIGH, SOURCE_CLUSTER_ID_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG, INT, REQUEST_TIMEOUT_MS_DEFAULT,
                        atLeast(0), MEDIUM, REQUEST_TIMEOUT_MS_DOC)
                .define(CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, CONNECTION_SETUP_TIMEOUT_MS_DEFAULT,
                        atLeast(0L), MEDIUM, CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, CONNECTION_SETUP_TIMEOUT_MAX_MS_DEFAULT,
                        atLeast(0L), MEDIUM, CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG, LONG, CONNECTIONS_MAX_IDLE_MS_DEFAULT, MEDIUM, CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(FETCH_WAIT_MAX_MS_CONFIG, INT, FETCH_WAIT_MAX_MS_DEFAULT, atLeast(0), MEDIUM, FETCH_WAIT_MAX_MS_DOC)
                .define(FETCH_BACKOFF_MS_CONFIG, LONG, FETCH_BACKOFF_MS_DEFAULT, atLeast(0L), MEDIUM, FETCH_BACKOFF_MS_DOC)
                .define(FETCH_MIN_BYTES_CONFIG, INT, FETCH_MIN_BYTES_DEFAULT, atLeast(0), MEDIUM, FETCH_MIN_BYTES_DOC)
                .define(FETCH_RESPONSE_MAX_BYTES_CONFIG, INT, FETCH_RESPONSE_MAX_BYTES_DEFAULT,
                        atLeast(0), MEDIUM, FETCH_RESPONSE_MAX_BYTES_DOC)
                .define(FETCH_MAX_BYTES_CONFIG, INT, FETCH_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, FETCH_MAX_BYTES_DOC)
                .define(SOCKET_TIMEOUT_MS_CONFIG, INT, SOCKET_TIMEOUT_MS_DEFAULT, atLeast(0), MEDIUM, SOCKET_TIMEOUT_MS_DOC)
                .define(SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT,
                        SOCKET_RECEIVE_BUFFER_BYTES_VALIDATOR, MEDIUM, SOCKET_RECEIVE_BUFFER_BYTES_DOC)
                .define(TOPIC_PROPERTIES_EXCLUDE_CONFIG, LIST, TOPIC_PROPERTIES_EXCLUDE_DEFAULT,
                        REGEX_LIST_VALIDATOR, LOW, TOPIC_PROPERTIES_EXCLUDE_DOC)
                .define(TOPICS_INCLUDE_CONFIG, LIST, TOPICS_INCLUDE_DEFAULT, REGEX_LIST_VALIDATOR, LOW, TOPICS_INCLUDE_DOC)
                .define(TOPICS_EXCLUDE_CONFIG, LIST, TOPICS_EXCLUDE_DEFAULT, REGEX_LIST_VALIDATOR, LOW, TOPICS_EXCLUDE_DOC)
                .define(GROUPS_INCLUDE_CONFIG, LIST, GROUPS_INCLUDE_DEFAULT, REGEX_LIST_VALIDATOR, LOW, GROUPS_INCLUDE_DOC)
                .define(GROUPS_EXCLUDE_CONFIG, LIST, GROUPS_EXCLUDE_DEFAULT, REGEX_LIST_VALIDATOR, LOW, GROUPS_EXCLUDE_DOC)
                .define(ACLS_INCLUDE_CONFIG, STRING, ACLS_INCLUDE_DEFAULT, ACL_RULES_VALIDATOR, LOW, ACLS_INCLUDE_DOC)
                .define(ACLS_EXCLUDE_CONFIG, STRING, ACLS_EXCLUDE_DEFAULT, ACL_RULES_VALIDATOR, LOW, ACLS_EXCLUDE_DOC)
                .define(SECURITY_PROTOCOL_CONFIG, STRING, SECURITY_PROTOCOL_DEFAULT,
                        ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(SecurityProtocol.class)),
                        MEDIUM, SECURITY_PROTOCOL_DOC)
                .define(SECURITY_PROVIDERS_CONFIG, STRING, null, LOW, SECURITY_PROVIDERS_DOC);
        SslConfigs.addClientSslSupport(configDef);
        SaslConfigs.addClientSaslSupport(configDef);
        return configDef;
    }

    private final Pattern topicPropertiesExcludePattern;
    private final Pattern topicsIncludePattern;
    private final Pattern topicsExcludePattern;
    private final Pattern groupsIncludePattern;
    private final Pattern groupsExcludePattern;
    private final List<MirrorAclRule> aclIncludeRules;
    private final List<MirrorAclRule> aclExcludeRules;

    /**
     * Creates a ClusterMirrorConfig from mirror metadata properties stored in the cluster metadata.
     * Filter configs are loaded from the metadata, with defaults applied for any missing values.
     *
     * @param properties the mirror metadata properties
     * @return a new ClusterMirrorConfig instance
     */
    public static ClusterMirrorConfig fromProperties(Properties properties) {
        return fromProperties(properties, false);
    }

    /**
     * Creates a ClusterMirrorConfig from mirror metadata properties stored in the cluster metadata.
     * Filter configs are loaded from the metadata, with defaults applied for any missing values.
     *
     * @param properties the mirror metadata properties
     * @param doLog whether to log all configuration values
     * @return a new ClusterMirrorConfig instance
     */
    public static ClusterMirrorConfig fromProperties(Properties properties, boolean doLog) {
        return new ClusterMirrorConfig(CONFIG_DEF, properties, doLog);
    }

    private ClusterMirrorConfig(ConfigDef configDef, Properties properties, boolean doLog) {
        super(configDef, properties, doLog);
        this.topicPropertiesExcludePattern = compilePatternList(getList(TOPIC_PROPERTIES_EXCLUDE_CONFIG));
        this.topicsIncludePattern = compilePatternList(getList(TOPICS_INCLUDE_CONFIG));
        this.topicsExcludePattern = compilePatternList(getList(TOPICS_EXCLUDE_CONFIG));
        this.groupsIncludePattern = compilePatternList(getList(GROUPS_INCLUDE_CONFIG));
        this.groupsExcludePattern = compilePatternList(getList(GROUPS_EXCLUDE_CONFIG));
        this.aclIncludeRules = MirrorAclRule.parseList(getString(ACLS_INCLUDE_CONFIG));
        this.aclExcludeRules = MirrorAclRule.parseList(getString(ACLS_EXCLUDE_CONFIG));
    }

    /**
     * Supported mirror configurations.
     */
    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG_DEF);
    }

    /**
     * Checks whether a given configuration name is valid.
     * @param name configuration name
     * @return true if it is a known configuration name, false otherwise
     */
    public static boolean isValidConfigName(String name) {
        return CONFIG_DEF.names().contains(name);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toHtml(4, config -> "clustermirrorconfigs_" + config));
    }

    public int requestTimeoutMs() {
        return getInt(REQUEST_TIMEOUT_MS_CONFIG);
    }

    public long connectionSetupTimeoutMs() {
        return getLong(CONNECTION_SETUP_TIMEOUT_MS_CONFIG);
    }

    public long connectionSetupTimeoutMaxMs() {
        return getLong(CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG);
    }

    public long connectionsMaxIdleMs() {
        return getLong(CONNECTIONS_MAX_IDLE_MS_CONFIG);
    }

    public int socketTimeoutMs() {
        return getInt(SOCKET_TIMEOUT_MS_CONFIG);
    }

    public int socketReceiveBufferBytes() {
        return getInt(SOCKET_RECEIVE_BUFFER_BYTES_CONFIG);
    }

    public int fetchWaitMaxMs() {
        return getInt(FETCH_WAIT_MAX_MS_CONFIG);
    }

    public long fetchBackoffMs() {
        return getLong(FETCH_BACKOFF_MS_CONFIG);
    }

    public int fetchMinBytes() {
        return getInt(FETCH_MIN_BYTES_CONFIG);
    }

    public int fetchResponseMaxBytes() {
        return getInt(FETCH_RESPONSE_MAX_BYTES_CONFIG);
    }

    public int fetchMaxBytes() {
        return getInt(FETCH_MAX_BYTES_CONFIG);
    }

    public Pattern topicPropertiesExcludePattern() {
        return topicPropertiesExcludePattern;
    }

    public Pattern topicsIncludePattern() {
        return topicsIncludePattern;
    }

    public Pattern topicsExcludePattern() {
        return topicsExcludePattern;
    }

    public Pattern groupsIncludePattern() {
        return groupsIncludePattern;
    }

    public Pattern groupsExcludePattern() {
        return groupsExcludePattern;
    }

    public List<MirrorAclRule> aclIncludeRules() {
        return aclIncludeRules;
    }

    public List<MirrorAclRule> aclExcludeRules() {
        return aclExcludeRules;
    }

    public String securityProtocol() {
        return getString(SECURITY_PROTOCOL_CONFIG);
    }

    public String saslMechanism() {
        return getString(SaslConfigs.SASL_MECHANISM);
    }

    public record MirrorAclRule(
            ResourceType resourceType,
            Pattern resourceNamePattern,
            AclOperation operation,
            AclPermissionType permissionType,
            String principal,
            Pattern hostPattern
    ) {
        private static final String SEPARATOR = ";";
        private static final String WILDCARD = ".*";
        public static final MirrorAclRule MATCH_ALL_RULE =
                new MirrorAclRule(null, null, null, null, null, null);

        // Using a small domain-specific language to make ACLs filtering easier
        private static final String DOC = "Rules are separated by || (double pipe). "
                + "Each rule uses semicolon-separated fields: "
                + "resourceType;resourceName;operation;permissionType;principal;host. "
                + "The resourceType, operation, and permissionType fields use enum values (e.g. TOPIC, READ, ALLOW). "
                + "Use ANY or empty to match all values for these fields. "
                + "The resourceName and host fields support regex patterns (e.g. test-.*, 192\\.168\\..*). "
                + "The principal field uses exact string matching (e.g. User:alice, User:*). "
                + "Empty principal means match all. "
                + "Use .* as the entire value to match all ACLs.";

        public static List<MirrorAclRule> parseList(String rules) {
            if (rules == null || rules.isEmpty()) {
                return List.of();
            }
            return Arrays.stream(rules.split("\\|\\|", -1))
                    .map(String::trim)
                    .filter(rule -> !rule.isEmpty())
                    .map(MirrorAclRule::parse)
                    .toList();
        }

        public static MirrorAclRule parse(String rule) {
            String trimmed = rule.trim();
            if (trimmed.equals(WILDCARD)) {
                return MATCH_ALL_RULE;
            }
            // Split with limit 6 to preserve any extra semicolons in the principal (5th field).
            // This ensures that principals containing semicolons are preserved as-is.
            String[] parts = trimmed.split(SEPARATOR, 6);
            return new MirrorAclRule(
                    parseResourceType(parts, 0),
                    parseResourceName(parts, 1),
                    parseOperation(parts, 2),
                    parsePermissionType(parts, 3),
                    parsePrincipal(parts, 4),
                    parseHost(parts, 5)
            );
        }

        private static ResourceType parseResourceType(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty() || value.equalsIgnoreCase(ResourceType.ANY.name())) return null;
            if (value.equals(ResourceType.UNKNOWN.name())) {
                throw new IllegalArgumentException("UNKNOWN resource type is not allowed");
            }
            return ResourceType.valueOf(value);
        }

        private static Pattern parseResourceName(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty()) return null;
            return compilePattern(value);
        }

        private static AclOperation parseOperation(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty() || value.equalsIgnoreCase(AclOperation.ANY.name())) return null;
            if (value.equals(AclOperation.UNKNOWN.name())) {
                throw new IllegalArgumentException("UNKNOWN operation is not allowed");
            }
            return AclOperation.valueOf(value);
        }

        private static AclPermissionType parsePermissionType(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty() || value.equalsIgnoreCase(AclPermissionType.ANY.name())) return null;
            if (value.equals(AclPermissionType.UNKNOWN.name())) {
                throw new IllegalArgumentException("UNKNOWN permission type is not allowed");
            }
            return AclPermissionType.valueOf(value);
        }

        private static String parsePrincipal(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty()) return null;
            return value;
        }

        private static Pattern parseHost(String[] parts, int index) {
            if (index >= parts.length) return null;
            String value = parts[index].trim();
            if (value.isEmpty()) return null;
            return compilePattern(value);
        }

        /**
         * Tests whether the given AclBinding matches this rule.
         * A null field acts as a wildcard and matches any value.
         */
        @SuppressWarnings("NPathComplexity")    
        public boolean matches(AclBinding binding) {
            if (resourceType != null && binding.pattern().resourceType() != resourceType) {
                return false;
            }
            if (resourceNamePattern != null && !resourceNamePattern.matcher(binding.pattern().name()).matches()) {
                return false;
            }
            if (operation != null && binding.entry().operation() != operation) {
                return false;
            }
            if (permissionType != null && binding.entry().permissionType() != permissionType) {
                return false;
            }
            if (principal != null && !principal.equals(binding.entry().principal())) {
                return false;
            }
            if (hostPattern != null && !hostPattern.matcher(binding.entry().host()).matches()) {
                return false;
            }
            return true;
        }
    }
}
