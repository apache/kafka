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

package org.apache.kafka.image;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.image.node.ScramImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.ScramCredentialData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents the SCRAM credentials in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public record ScramImage(Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms) {
    public static final ScramImage EMPTY = new ScramImage(Map.of());

    public ScramImage {
        mechanisms = Collections.unmodifiableMap(mechanisms);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (options.metadataVersion().isScramSupported()) {
            for (var mechanismEntry : mechanisms.entrySet()) {
                for (var userEntry : mechanismEntry.getValue().entrySet()) {
                    writer.write(0, userEntry.getValue().toRecord(userEntry.getKey(), mechanismEntry.getKey()));
                }
            }
        } else {
            boolean isEmpty = true;
            StringBuilder scramImageString = new StringBuilder("ScramImage({");
            for (var mechanismEntry : mechanisms.entrySet()) {
                if (!mechanismEntry.getValue().isEmpty()) {
                    scramImageString.append(mechanismEntry.getKey()).append(":");
                    List<String> users = new ArrayList<>(mechanismEntry.getValue().keySet());
                    scramImageString.append(String.join(", ", users));
                    scramImageString.append("},{");
                    isEmpty = false;
                }
            }

            if (!isEmpty) {
                scramImageString.append("})");
                options.handleLoss(scramImageString.toString());
            }
        }
    }

    private static final String DESCRIBE_DUPLICATE_USER = "Cannot describe SCRAM credentials for the same user twice in a single request: ";
    private static final String DESCRIBE_USER_THAT_DOES_NOT_EXIST = "Attempt to describe a user credential that does not exist: ";

    public DescribeUserScramCredentialsResponseData describe(DescribeUserScramCredentialsRequestData request) {
        List<UserName> users = request.users();
        Map<String, Boolean> uniqueUsers = new HashMap<>();

        if ((users == null) || (users.isEmpty())) {
            // If there are no users listed then get all the users
            for (var scramCredentialDataSet : mechanisms.values()) {
                for (String user : scramCredentialDataSet.keySet()) {
                    uniqueUsers.put(user, false);
                }
            }
        } else {
            // Filter out duplicates
            for (UserName user : users) {
                if (uniqueUsers.containsKey(user.name())) {
                    uniqueUsers.put(user.name(), true);
                } else {
                    uniqueUsers.put(user.name(), false);
                }
            }
        }

        DescribeUserScramCredentialsResponseData retval = new DescribeUserScramCredentialsResponseData();

        for (Entry<String, Boolean> user : uniqueUsers.entrySet()) {
            DescribeUserScramCredentialsResult result = new DescribeUserScramCredentialsResult().setUser(user.getKey());

            if (!user.getValue()) {
                boolean dataFound = false;
                List<CredentialInfo> credentialInfos = new ArrayList<>();
                for (var mechanismsEntry : mechanisms.entrySet()) {
                    Map<String, ScramCredentialData> credentialDataSet = mechanismsEntry.getValue();
                    if (credentialDataSet.containsKey(user.getKey())) {
                        credentialInfos.add(new CredentialInfo().setMechanism(mechanismsEntry.getKey().type())
                            .setIterations(credentialDataSet.get(user.getKey()).iterations()));
                        dataFound = true;
                    }
                }
                if (dataFound) {
                    result.setCredentialInfos(credentialInfos);
                } else {
                    result.setErrorCode(Errors.RESOURCE_NOT_FOUND.code())
                        .setErrorMessage(DESCRIBE_USER_THAT_DOES_NOT_EXIST + user.getKey());
                }
            } else {
                result.setErrorCode(Errors.DUPLICATE_RESOURCE.code())
                    .setErrorMessage(DESCRIBE_DUPLICATE_USER + user.getKey());
            }
            retval.results().add(result);
        }
        return retval;
    }

    public boolean isEmpty() {
        return mechanisms.isEmpty();
    }

    @Override
    public String toString() {
        return new ScramImageNode(this).stringify();
    }
}
