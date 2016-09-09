/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common;

import java.util.ArrayList;
import java.util.List;

public class ClusterResourceListeners {

    private final List<ClusterResourceListener> clusterResourceListeners;

    public ClusterResourceListeners() {
        this.clusterResourceListeners = new ArrayList<>();
    }


    public void add(Object candidate) {
        if (candidate instanceof ClusterResourceListener) {
            clusterResourceListeners.add((ClusterResourceListener) candidate);
        }
    }

    public void addAll(List candidateList) {
        for (Object candidate : candidateList) {
            if (candidate instanceof ClusterResourceListener)
                clusterResourceListeners.add((ClusterResourceListener) candidate);
        }
    }

    public void onUpdate(ClusterResource cluster) {
        for (ClusterResourceListener clusterResourceListener : clusterResourceListeners) {
            clusterResourceListener.onUpdate(cluster);
        }
    }

}
