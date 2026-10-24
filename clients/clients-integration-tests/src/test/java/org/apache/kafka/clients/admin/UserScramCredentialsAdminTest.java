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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(types = {Type.KRAFT})
public class UserScramCredentialsAdminTest {

    @ClusterTest
    public void testDescribeUserScramCredentials(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            // Add a new user
            String targetUserName = "tom";
            admin.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion(targetUserName, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "123456")
            )).all().get();

            TestUtils.waitForCondition(
                () -> admin.describeUserScramCredentials().all().get().size() == 1,
                "Add one user scram credential timeout"
            );

            Map<String, UserScramCredentialsDescription> result = admin.describeUserScramCredentials().all().get();
            result.forEach((userName, scramDescription) -> {
                assertEquals(targetUserName, userName);
                assertEquals(targetUserName, scramDescription.name());
                List<ScramCredentialInfo> credentialInfos = scramDescription.credentialInfos();
                assertEquals(1, credentialInfos.size());
                assertEquals(ScramMechanism.SCRAM_SHA_256, credentialInfos.get(0).mechanism());
                assertEquals(4096, credentialInfos.get(0).iterations());
            });

            // Add other users
            admin.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion("tom2", new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "123456"),
                new UserScramCredentialUpsertion("tom3", new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "123456")
            )).all().get();

            TestUtils.waitForCondition(
                () -> admin.describeUserScramCredentials().all().get().size() == 3,
                "Add user scram credential timeout"
            );

            // Alter user info
            admin.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion(targetUserName, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), "123456")
            )).all().get();

            TestUtils.waitForCondition(
                () -> admin.describeUserScramCredentials().all().get().get(targetUserName).credentialInfos().size() == 2,
                "Alter user scram credential timeout"
            );

            Map<String, UserScramCredentialsDescription> userTomResult = admin.describeUserScramCredentials().all().get();
            assertEquals(3, userTomResult.size());
            UserScramCredentialsDescription userScramCredential = userTomResult.get(targetUserName);
            assertEquals(targetUserName, userScramCredential.name());
            List<ScramCredentialInfo> credentialInfos = userScramCredential.credentialInfos();
            assertEquals(2, credentialInfos.size());

            List<ScramCredentialInfo> sortedCredentialList = credentialInfos.stream()
                .sorted(Comparator.comparing(s -> s.mechanism().type()))
                .toList();
            assertEquals(ScramMechanism.SCRAM_SHA_256, sortedCredentialList.get(0).mechanism());
            assertEquals(4096, sortedCredentialList.get(0).iterations());
            assertEquals(ScramMechanism.SCRAM_SHA_512, sortedCredentialList.get(1).mechanism());
            assertEquals(8192, sortedCredentialList.get(1).iterations());

            // Test describeUserScramCredentials(List<String> users)
            Map<String, UserScramCredentialsDescription> userAndScramMap = admin.describeUserScramCredentials(List.of("tom2")).all().get();
            assertEquals(1, userAndScramMap.size());
            UserScramCredentialsDescription scram = userAndScramMap.get("tom2");
            assertNotNull(scram);
            ScramCredentialInfo credentialInfo = scram.credentialInfos().get(0);
            assertEquals(ScramMechanism.SCRAM_SHA_256, credentialInfo.mechanism());
            assertEquals(4096, credentialInfo.iterations());
        }
    }

    @ClusterTest
    public void testDescribeUserScramCredentialsTimeout() {
        Map<String, Object> config = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234"
        );
        try (Admin invalidAdmin = Admin.create(config)) {
            ExecutionException exception = assertThrows(ExecutionException.class, () ->
                invalidAdmin.describeUserScramCredentials(List.of("tom4"),
                    new DescribeUserScramCredentialsOptions().timeoutMs(0)).all().get()
            );
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }
}
