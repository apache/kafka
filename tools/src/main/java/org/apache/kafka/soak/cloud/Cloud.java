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

package org.apache.kafka.soak.cloud;

import org.apache.kafka.soak.cluster.SoakNode;

import java.util.Collection;

public interface Cloud extends AutoCloseable {
    abstract class Runner {
        String instanceType = "";
        String imageId = "";

        public Runner instanceType(String instanceType) {
            this.instanceType = instanceType == null ? "" : instanceType;
            return this;
        }

        public Runner imageId(String imageId) {
            this.imageId = imageId == null ? "" : imageId;
            return this;
        }

        abstract public String run() throws Exception;
    }

    /**
     * Create an instance runner.
     **/
    Runner newRunner();

    class InstanceDescription {
        private final String instanceId;
        private final String privateDns;
        private final String publicDns;
        private final String state;

        InstanceDescription(String instanceId, String privateDns, String publicDns, String state) {
            this.instanceId = instanceId == null ? "" : instanceId;
            this.privateDns = privateDns == null ? "" : privateDns;
            this.publicDns = publicDns == null ? "" : publicDns;
            this.state = state == null ? "" : state;
        }

        public String instanceId() {
            return instanceId;
        }

        public String privateDns() {
            return privateDns;
        }

        public String publicDns() {
            return publicDns;
        }

        public String state() {
            return state;
        }

        @Override
        public String toString() {
            return "(instanceId=" + instanceId +
                ", privateDns=" + privateDns +
                ", publicDns=" + publicDns +
                ", state=" + state + ")";
        }
    }

    /**
     * Describe an instance.
     *
     * @param instanceId    The instance ID.
     * @return              The instance description, or null if the instance could not be found.
     *
     * @throws Exception    If the instance description could not be retrieved.
     */
    InstanceDescription describeInstance(String instanceId) throws Exception;

    /**
     * Describe all instances.
     *
     * @return              The instance descriptions.
     *
     * @throws Exception    If the instance description could not be retrieved.
     */
    Collection<InstanceDescription> describeInstances() throws Exception;

    /**
     * Terminates instances.
     *
     * @param instanceIds   The instance IDs.
     *
     * @throws Exception    If any of the instances could not be terminated.
     */
    void terminateInstances(String... instanceIds) throws Throwable;

    /**
     * Run a remote command on a cloud node.
     *
     * @param node          The node to run the command on.
     *
     * @return              The command object.  You must call run() to initiate the session.
     */
    RemoteCommand remoteCommand(SoakNode node);
}
