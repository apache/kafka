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

import org.apache.kafka.image.node.ScramImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.metadata.ScramCredentialData;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;


/**
 * Represents the SCRAM credentials in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ScramImage {
    public static final ScramImage EMPTY = new ScramImage(Collections.emptyMap());

    private final Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms;

    public ScramImage(Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms) {
        this.mechanisms = Collections.unmodifiableMap(mechanisms);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (options.metadataVersion().isScramSupported()) {
            for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : mechanisms.entrySet()) {
                for (Entry<String, ScramCredentialData> userEntry : mechanismEntry.getValue().entrySet()) {
                    writer.write(0, userEntry.getValue().toRecord(userEntry.getKey(), mechanismEntry.getKey()));
                }
            }
        } else {
            boolean isEmpty = true;
            StringBuffer scramImageString = new StringBuffer("ScramImage({");
            for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : mechanisms.entrySet()) {
                if (!mechanismEntry.getValue().isEmpty()) {
                    scramImageString.append(mechanismEntry.getKey() + ":");
                    List<String> users = new ArrayList<>(mechanismEntry.getValue().keySet());
                    scramImageString.append(users.stream().collect(Collectors.joining(", ")));
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
        Map<String, Boolean> uniqueUsers = new HashMap<String, Boolean>();

        if ((users == null) || (users.size() == 0)) {
            // If there are no users listed then get all the users
            for (Map<String, ScramCredentialData> scramCredentialDataSet : mechanisms.values()) {
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

        for (Map.Entry<String, Boolean> user : uniqueUsers.entrySet()) {
            DescribeUserScramCredentialsResult result = new DescribeUserScramCredentialsResult().setUser(user.getKey());

            if (!user.getValue()) {
                boolean datafound = false;
                List<CredentialInfo> credentialInfos = new ArrayList<CredentialInfo>();
                for (Map.Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismsEntry : mechanisms.entrySet()) {
                    Map<String, ScramCredentialData> credentialDataSet = mechanismsEntry.getValue();
                    if (credentialDataSet.containsKey(user.getKey())) {
                        credentialInfos.add(new CredentialInfo().setMechanism(mechanismsEntry.getKey().type())
                                                                .setIterations(credentialDataSet.get(user.getKey()).iterations()));
                        datafound = true;
                    }
                }
                if (datafound) {
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

    public Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms() {
        return mechanisms;
    }

    public boolean isEmpty() {
        return mechanisms.isEmpty();
    }

    @Override
    public int hashCode() {
        return mechanisms.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!o.getClass().equals(ScramImage.class)) return false;
        ScramImage other = (ScramImage) o;
        return mechanisms.equals(other.mechanisms);
    }

    @Override
    public String toString() {
        return new ScramImageNode(this).stringify();
    }
}
