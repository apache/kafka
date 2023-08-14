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
package org.apache.kafka.tiered.storage;

import org.apache.kafka.tiered.storage.utils.DumpLocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public final class TieredStorageTestReport {

    private final TieredStorageTestContext context;
    private final List<TieredStorageTestAction> successfulActions = new ArrayList<>();
    private final List<TieredStorageTestAction> failedActions = new ArrayList<>();

    public TieredStorageTestReport(TieredStorageTestContext context) {
        this.context = context;
    }

    public synchronized void addSucceeded(TieredStorageTestAction action) {
        successfulActions.add(action);
    }

    public synchronized void addFailed(TieredStorageTestAction action) {
        failedActions.add(action);
    }

    public void print(PrintStream output) {
        output.println();
        int seqNo = 0;
        List<List<TieredStorageTestAction>> actionsLists = new ArrayList<>();
        actionsLists.add(successfulActions);
        actionsLists.add(failedActions);

        List<String> statusList = new ArrayList<>();
        statusList.add("SUCCESS");
        statusList.add("FAILURE");

        for (int i = 0; i < actionsLists.size(); i++) {
            List<TieredStorageTestAction> actions = actionsLists.get(i);
            String ident = statusList.get(i);
            for (TieredStorageTestAction action : actions) {
                seqNo++;
                output.print("[" + ident + "] (" + seqNo + ") ");
                action.describe(output);
                output.println();
            }
        }
        String lts = "";
        if (!context.remoteStorageManagers().isEmpty()) {
            LocalTieredStorage tieredStorage = context.remoteStorageManagers().get(0);
            lts = DumpLocalTieredStorage.dump(tieredStorage, context.de(), context.de());
        }
        output.printf("Content of local tiered storage:%n%n%s%n", lts);
    }
}
