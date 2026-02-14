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
package org.apache.kafka.server.quota;

/**
 * The ControllerMutationQuota trait defines a quota for a given user/clientId pair. Such
 * quota is not meant to be cached forever but rather during the lifetime of processing
 * a request.
 */
public interface ControllerMutationQuota {
    boolean isExceeded();
    void record(double permits);
    int throttleTime();

    ControllerMutationQuota UNBOUNDED_CONTROLLER_MUTATION_QUOTA = new ControllerMutationQuota() {
        @Override
        public boolean isExceeded() {
            return false;
        }

        @Override
        public void record(double permits) {
        }

        @Override
        public int throttleTime() {
            return 0;
        }
    };
}
