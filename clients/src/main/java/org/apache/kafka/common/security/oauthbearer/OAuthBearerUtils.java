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
package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfigurable;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class OAuthBearerUtils {

    public static <T> T getConfiguredInstanceOrDefault(Map<String, ?> configs,
                                                       String saslMechanism,
                                                       List<AppConfigurationEntry> jaasConfigEntries,
                                                       String configName,
                                                       Class<T> clazz) {
        Object classOrClassName = configs.get(configName);
        Object o;

        if (classOrClassName instanceof String) {
            try {
                o = Utils.newInstance((String) classOrClassName, clazz);
            } catch (ClassNotFoundException e) {
                throw new KafkaException("Class " + classOrClassName + " cannot be found", e);
            }
        } else if (classOrClassName instanceof Class<?>) {
            o = Utils.newInstance((Class<?>) classOrClassName);
        } else if (classOrClassName != null) {
            throw new KafkaException("Unexpected element of type " + classOrClassName.getClass().getName() + ", expected String or Class");
        } else {
            throw new KafkaException("Unexpectedly found no configuration for " + configName + ", expected String or Class");
        }

        if (!clazz.isInstance(o))
            throw new KafkaException(classOrClassName + " is not an instance of " + clazz.getName());

        try {
            if (o instanceof OAuthBearerConfigurable)
                ((OAuthBearerConfigurable) o).configure(configs, saslMechanism, jaasConfigEntries);
        } catch (Exception e) {
            Utils.closeQuietly((AutoCloseable) o, "AutoCloseable object constructed and configured during failed call to configure()");
            throw e;
        }

        return clazz.cast(o);
    }
}