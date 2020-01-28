package com.microsoft.azpubsub.security.auth;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;

/*
 * AzPubSub configuration class
 */
public class AzPubSubConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;
    private static final String AZPUBSUB_PROPERTIES_PROP = "azpubsub.properties";

    public static final String TOKEN_VALIDATOR_CLASS_CONFIG = "azpubsub.oauth.validator.class";
    private static final String TOKEN_VALIDATOR_CLASS_DOC = "AzPubSub oAuth validator class filename";

    public static final String CERT_IDENTIFIER_CLASS_CONFIG = "azpubsub.cert.identifier.class";
    private static final String CERT_IDENTIFIER_CLASS_DOC = "AzPubSub Certificate Identifier class filename";

    public static final String DSTS_METADATA_FILE_CONFIG = "azpubsub.dsts.config.file";
    private static final String DSTS_METADATA_FILE_DOC = "AzPubSub dSTS configuration file";

    static {
        CONFIG = new ConfigDef().define(TOKEN_VALIDATOR_CLASS_CONFIG,
                                        Type.STRING,
                                        "", 
                                        Importance.MEDIUM,
                                        TOKEN_VALIDATOR_CLASS_DOC)
                                .define(CERT_IDENTIFIER_CLASS_CONFIG,
                                        Type.STRING,
                                        "", 
                                        Importance.MEDIUM,
                                        CERT_IDENTIFIER_CLASS_DOC)
                                .define(DSTS_METADATA_FILE_CONFIG,
                                        Type.STRING,
                                        "", 
                                        Importance.MEDIUM,
                                        DSTS_METADATA_FILE_DOC)
                                ;
    }

    public static AzPubSubConfig fromProps(Map<String, ?> configProviderProps) {
        String azpubsubPropertiesFile = System.getProperty(AZPUBSUB_PROPERTIES_PROP);
        try {
            Properties props = Utils.loadProps(azpubsubPropertiesFile);
            return new AzPubSubConfig(props, configProviderProps);
        } catch (IOException ex) {
            throw new IllegalArgumentException(String.format("Failed to read azpubusb properties (%s) or merge it with Kafka global configs", azpubsubPropertiesFile), ex.getCause());
        }
    }

    public AzPubSubConfig(Map<?, ?> originals,  Map<String, ?> configProviderProps) {
        super(CONFIG, originals, configProviderProps, false);
    }
}
