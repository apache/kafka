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
package org.apache.kafka.migration;

public class ZkControllerState {
    public static final ZkControllerState EMPTY = new ZkControllerState(-1, -1, -1);

    private final int controllerId;
    private final int controllerEpoch;
    private final int zkVersion;

    public ZkControllerState(int controllerId, int controllerEpoch, int zkVersion) {
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.zkVersion = zkVersion;
    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public int zkVersion() {
        return zkVersion;
    }

    @Override
    public String toString() {
        return "ZkControllerState{" +
                "controllerId=" + controllerId +
                ", controllerEpoch=" + controllerEpoch +
                ", zkVersion=" + zkVersion +
                '}';
    }
}
