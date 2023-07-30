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

package org.apache.kafka.tools.reassign;

import java.util.Objects;

/**
 * A replica log directory move state where there is no move in progress, but we did not
 * reach the target log directory.
 */
final class CancelledMoveState implements LogDirMoveState {
    public final String currentLogDir;

    public final String targetLogDir;

    /**
     * @param currentLogDir       The current log directory.
     * @param targetLogDir        The log directory that we wanted the replica to move to.
     */
    public CancelledMoveState(String currentLogDir, String targetLogDir) {
        this.currentLogDir = currentLogDir;
        this.targetLogDir = targetLogDir;
    }

    @Override
    public boolean done() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelledMoveState that = (CancelledMoveState) o;
        return Objects.equals(currentLogDir, that.currentLogDir) && Objects.equals(targetLogDir, that.targetLogDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentLogDir, targetLogDir);
    }
}
