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

package org.apache.kafka.trogdor.fault;

import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.Topology;

import java.util.Objects;
import java.util.Set;

public abstract class AbstractFault implements Fault {
    private final String id;
    private final FaultSpec spec;
    private FaultState state;

    public AbstractFault(String id, FaultSpec spec) {
        this.id = id;
        this.spec = spec;
        this.state = new PendingState();
    }

    @Override
    public final String id() {
        return id;
    }

    @Override
    public final FaultSpec spec() {
        return spec;
    }

    @Override
    public synchronized FaultState state() {
        return state;
    }

    @Override
    public synchronized void setState(FaultState state) {
        this.state = state;
    }

    @Override
    public final void activate(long now, Platform platform) throws Exception {
        try {
            handleActivation(now, platform);
            setState(new RunningState(now));
        } catch (Exception e) {
            setState(new DoneState(now, e.getMessage()));
            throw e;
        }
    }

    protected abstract void handleActivation(long now, Platform platform) throws Exception;

    @Override
    public final void deactivate(long now, Platform platform) throws Exception {
        try {
            handleDeactivation(now, platform);
            setState(new DoneState(now, ""));
        } catch (Exception e) {
            setState(new DoneState(now, e.getMessage()));
            throw e;
        }
    }

    protected abstract void handleDeactivation(long now, Platform platform) throws Exception;

    @Override
    public abstract Set<String> targetNodes(Topology topology);

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return toString().equals(o.toString());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(toString());
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "(id=" + id +
            ", spec=" + JsonUtil.toJsonString(spec) +
            ", state=" + JsonUtil.toJsonString(state()) +
            ")";
    }
}
