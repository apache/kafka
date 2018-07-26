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
package org.apache.kafka.common.security.scram.internal;

import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.utils.Base64;

/**
 * SCRAM Credential persistence utility functions. Implements format conversion used
 * for the credential store implemented in Kafka. Credentials are persisted as a comma-separated
 * String of key-value pairs:
 * <pre>
 *   salt=<i>salt</i>,stored_key=<i>stored_key</i>,server_key=<i>server_key</i>,iterations=<i>iterations</i>
 * </pre>
 *
 */
public final class ScramCredentialUtils {
    private static final String SALT = "salt";
    private static final String STORED_KEY = "stored_key";
    private static final String SERVER_KEY = "server_key";
    private static final String ITERATIONS = "iterations";

    private ScramCredentialUtils() {}

    public static String credentialToString(ScramCredential credential) {
        return String.format("%s=%s,%s=%s,%s=%s,%s=%d",
               SALT,
               Base64.encoder().encodeToString(credential.salt()),
               STORED_KEY,
                Base64.encoder().encodeToString(credential.storedKey()),
               SERVER_KEY,
                Base64.encoder().encodeToString(credential.serverKey()),
               ITERATIONS,
               credential.iterations());
    }

    public static ScramCredential credentialFromString(String str) {
        Properties props = toProps(str);
        if (props.size() != 4 || !props.containsKey(SALT) || !props.containsKey(STORED_KEY) ||
                !props.containsKey(SERVER_KEY) || !props.containsKey(ITERATIONS)) {
            throw new IllegalArgumentException("Credentials not valid: " + str);
        }
        byte[] salt = Base64.decoder().decode(props.getProperty(SALT));
        byte[] storedKey = Base64.decoder().decode(props.getProperty(STORED_KEY));
        byte[] serverKey = Base64.decoder().decode(props.getProperty(SERVER_KEY));
        int iterations = Integer.parseInt(props.getProperty(ITERATIONS));
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }

    private static Properties toProps(String str) {
        Properties props = new Properties();
        String[] tokens = str.split(",");
        for (String token : tokens) {
            int index = token.indexOf('=');
            if (index <= 0)
                throw new IllegalArgumentException("Credentials not valid: " + str);
            props.put(token.substring(0, index), token.substring(index + 1));
        }
        return props;
    }

    public static void createCache(CredentialCache cache, Collection<String> mechanisms) {
        for (String mechanism : ScramMechanism.mechanismNames()) {
            if (mechanisms.contains(mechanism))
                cache.createCache(mechanism, ScramCredential.class);
        }
    }
}
