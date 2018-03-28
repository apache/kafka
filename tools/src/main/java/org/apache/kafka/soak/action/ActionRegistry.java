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

package org.apache.kafka.soak.action;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A registry which stores action classes.
 */
public class ActionRegistry {
    public static final ActionRegistry INSTANCE = new ActionRegistry();

    static {
        INSTANCE.register(DaemonStartAction.class);
        INSTANCE.register(DaemonStatusAction.class);
        INSTANCE.register(DaemonStopAction.class);
        INSTANCE.register(DestroyAction.class);
        INSTANCE.register(DownAction.class);
        INSTANCE.register(InitAction.class);
        INSTANCE.register(LinuxSetupAction.class);
        INSTANCE.register(SaveLogsAction.class);
        INSTANCE.register(SetupAction.class);
        INSTANCE.register(SourceSetupAction.class);
        INSTANCE.register(StartAction.class);
        INSTANCE.register(StatusAction.class);
        INSTANCE.register(StopAction.class);
        INSTANCE.register(UpAction.class);
    }

    private final List<Class<? extends Action>> classes = new ArrayList<>();

    public synchronized void register(Class<? extends Action> clazz) {
        classes.add(clazz);
    }

    public synchronized Collection<Action> actions(Collection<String> nodeNames) throws Exception {
        List<Action> actions = new ArrayList<>();
        for (Class<? extends Action> clazz : classes) {
            Constructor<? extends Action> ctor = clazz.getConstructor(String.class);
            for (String nodeName : nodeNames) {
                actions.add(ctor.newInstance(nodeName));
            }
        }
        return actions;
    }
}
