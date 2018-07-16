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

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the shutdown of the castle tool.
 *
 * Stores the final return code of the tool.  Tracks whatever hooks need to be
 * run before shutdown.
 */
public class CastleShutdownManager {
    private final Logger log;
    private HashMap<String, CastleShutdownHook> hooks;
    private CastleReturnCode returnCode = CastleReturnCode.SUCCESS;

    public CastleShutdownManager(Logger log) {
        this.log = log;
        this.hooks = new HashMap<>();
    }

    public synchronized  void addHookIfMissing(CastleShutdownHook hook) {
        if (hooks == null) {
            throw new RuntimeException("Shutdown has already occurred; " +
                "can't add any more shutdown hooks.");
        }
        if (!hooks.containsKey(hook.name())) {
            this.hooks.put(hook.name(), hook);
        }
    }

    public void shutdown() {
        Map<String, CastleShutdownHook> toRun = null;
        synchronized (this) {
            if (hooks == null) {
                return;
            }
            toRun = hooks;
            hooks = null;
        }
        for (CastleShutdownHook hook : toRun.values()) {
            try {
                hook.run(returnCode());
            } catch (Throwable e) {
                log.error("Error running shutdown hook {}", hook.toString(), e);
                changeReturnCode(CastleReturnCode.TOOL_FAILED);
            }
        }
    }

    public synchronized void changeReturnCode(CastleReturnCode returnCode) {
        this.returnCode = CastleReturnCode.worstOf(this.returnCode, returnCode);
    }

    public synchronized CastleReturnCode returnCode() {
        return returnCode;
    }
}
