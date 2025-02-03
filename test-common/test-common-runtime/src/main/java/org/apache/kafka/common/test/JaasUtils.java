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
package org.apache.kafka.common.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.login.Configuration;

public class JaasUtils {
    public record JaasSection(String contextName, List<JaasModule> modules) {
        @Override
        public String toString() {
            return String.format(
                "%s {%n  %s%n};%n",
                contextName,
                modules.stream().map(Object::toString).collect(Collectors.joining("\n  "))
            );
        }
    }

    public static final String KAFKA_SERVER_CONTEXT_NAME = "KafkaServer";

    public static final String KAFKA_PLAIN_USER1 = "plain-user1";
    public static final String KAFKA_PLAIN_USER1_PASSWORD = "plain-user1-secret";
    public static final String KAFKA_PLAIN_ADMIN = "plain-admin";
    public static final String KAFKA_PLAIN_ADMIN_PASSWORD = "plain-admin-secret";

    public static File writeJaasContextsToFile(Set<JaasSection> jaasSections) throws IOException {
        File jaasFile = TestUtils.tempFile();
        try (FileOutputStream fileStream = new FileOutputStream(jaasFile);
             OutputStreamWriter writer = new OutputStreamWriter(fileStream, StandardCharsets.UTF_8);) {
            writer.write(String.join("", jaasSections.stream().map(Object::toString).toArray(String[]::new)));
        }
        return jaasFile;
    }

    public static void refreshJavaLoginConfigParam(File file) {
        System.setProperty(org.apache.kafka.common.security.JaasUtils.JAVA_LOGIN_CONFIG_PARAM, file.getAbsolutePath());
        // This will cause a reload of the Configuration singleton when `getConfiguration` is called
        Configuration.setConfiguration(null);
    }
}
