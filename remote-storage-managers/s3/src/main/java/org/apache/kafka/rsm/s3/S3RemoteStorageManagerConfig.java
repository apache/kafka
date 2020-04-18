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
package org.apache.kafka.rsm.s3;

import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;

/**
 * A configuration for {@link S3RemoteStorageManager}.
 */
public class S3RemoteStorageManagerConfig extends AbstractConfig {
    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "The S3 Bucket.";

    public static final String S3_REGION_CONFIG = "s3.region";
    private static final String S3_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
    private static final String S3_REGION_DOC = "The AWS region.";

    public static final String S3_CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.credentials.provider.class";
    private static final Class<? extends AWSCredentialsProvider> S3_CREDENTIALS_PROVIDER_CLASS_DEFAULT = null;
    private static final String S3_CREDENTIALS_PROVIDER_CLASS_DOC = "The credentials provider to use for " +
        "authentication to AWS. If not set, AWS SDK uses the default " +
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";

    private static final ConfigDef CONFIG;
    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
            S3_BUCKET_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            S3_BUCKET_NAME_DOC
        );

        CONFIG.define(
            S3_REGION_CONFIG,
            ConfigDef.Type.STRING,
            S3_REGION_DEFAULT,
            new RegionValidator(),
            ConfigDef.Importance.MEDIUM,
            S3_REGION_DOC
        );

        CONFIG.define(
            S3_CREDENTIALS_PROVIDER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            S3_CREDENTIALS_PROVIDER_CLASS_DEFAULT,
            new CredentialsProviderValidator(),
            ConfigDef.Importance.LOW,
            S3_CREDENTIALS_PROVIDER_CLASS_DOC
        );

        // TODO consider adding:
        // - common prefix
        // - storage class
    }

    public S3RemoteStorageManagerConfig(final Map<String, ?> props) {
        super(CONFIG, props);
    }

    public String s3BucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }

    public Regions s3Region() {
        final String regionStr = getString(S3_REGION_CONFIG);
        if (regionStr == null) {
            return null;
        }
        return Regions.fromName(regionStr);
    }

    public AWSCredentialsProvider awsCredentialsProvider() {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends AWSCredentialsProvider> providerClass = (Class<? extends AWSCredentialsProvider>)
                getClass(S3_CREDENTIALS_PROVIDER_CLASS_CONFIG);
            if (providerClass == null) {
                return null;
            }
            return providerClass.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new KafkaException(e);
        }
    }

    private static class RegionValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            // Does null check as well.
            if (!(value instanceof String)) {
                throw new ConfigException(name, value);
            }

            final String regionStr = (String) value;
            try {
                Regions.fromName(regionStr);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(name, value);
            }
        }
    }

    private static class CredentialsProviderValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                return;
            }

            if (!(value instanceof Class<?>)) {
                throw new ConfigException(name, value);
            }

            final Class<?> providerClass = (Class<?>) value;
            if (!AWSCredentialsProvider.class.isAssignableFrom(providerClass)) {
                throw new ConfigException(name, value, "Class must extend " + AWSCredentialsProvider.class);
            }
        }
    }
}
