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

package kafka.testkit;

import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Kip500BrokerNode implements TestKitNode {
    public static class Builder {
        private int id = -1;
        private Uuid incarnationId = null;
        private List<String> logDirectories = null;

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setLogDirectories(List<String> logDirectories) {
            this.logDirectories = logDirectories;
            return this;
        }

        public Kip500BrokerNode build() {
            if (id == -1) {
                throw new RuntimeException("You must set the node id");
            }
            if (incarnationId == null) {
                incarnationId = Uuid.randomUuid();
            }
            if (logDirectories == null) {
                logDirectories  = Collections.singletonList(String.format("kip500broker_%d", id));
            }
            return new Kip500BrokerNode(id, incarnationId, logDirectories);
        }
    }

    private final int id;
    private final Uuid incarnationId;
    private final List<String> logDirectories;

    private Kip500BrokerNode(int id, Uuid incarnationId, List<String> logDirectories) {
        this.id = id;
        this.incarnationId = incarnationId;
        this.logDirectories = new ArrayList<>(logDirectories);
    }

    @Override
    public int id() {
        return id;
    }

    public Uuid incarnationId() {
        return incarnationId;
    }

    @Override
    public List<String> logDirectories() {
        return logDirectories;
    }
}
