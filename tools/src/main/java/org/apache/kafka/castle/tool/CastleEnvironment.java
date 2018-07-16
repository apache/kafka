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

package org.apache.kafka.castle.tool;

import java.nio.file.Paths;

public final class CastleEnvironment {
    private final String clusterInputPath;
    private final String clusterOutputPath;
    private final String awsSecurityGroup;
    private final String awsSecurityKeyPair;
    private final int timeoutSecs;
    private final String kafkaPath;
    private final String outputDirectory;

    public CastleEnvironment(String clusterInputPath, String clusterOutputPath,
                           String awsSecurityGroup, String awsSecurityKeyPair,
                           int timeoutSecs, String kafkaPath,
                           String outputDirectory) {
        this.clusterInputPath = toAbsolutePath(clusterInputPath);
        this.clusterOutputPath = toAbsolutePath(clusterOutputPath);
        this.awsSecurityGroup = awsSecurityGroup;
        this.awsSecurityKeyPair = awsSecurityKeyPair;
        this.timeoutSecs = timeoutSecs;
        this.kafkaPath = toAbsolutePath(kafkaPath);
        this.outputDirectory = toAbsolutePath(outputDirectory);
    }

    private String toAbsolutePath(String path) {
        if (path == null) {
            path = "";
        }
        return Paths.get(path).toAbsolutePath().toString();
    }

    public String clusterInputPath() {
        return clusterInputPath;
    }

    public String clusterOutputPath() {
        return clusterOutputPath;
    }

    public String awsSecurityGroup() {
        return awsSecurityGroup;
    }

    public String awsSecurityKeyPair() {
        return awsSecurityKeyPair;
    }

    public int timeoutSecs() {
        return timeoutSecs;
    }

    public String kafkaPath() {
        return kafkaPath;
    }

    public String outputDirectory() {
        return outputDirectory;
    }
};
