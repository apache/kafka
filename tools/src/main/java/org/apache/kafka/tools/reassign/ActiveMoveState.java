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
 * A replica log directory move state where the move is in progress.
 */
final class ActiveMoveState implements LogDirMoveState {
    public final String currentLogDir;

    public final String targetLogDir;

    public final String futureLogDir;

    /**
     * @param currentLogDir       The current log directory.
     * @param futureLogDir        The log directory that the replica is moving to.
     * @param targetLogDir        The log directory that we wanted the replica to move to.
     */
    public ActiveMoveState(String currentLogDir, String targetLogDir, String futureLogDir) {
        this.currentLogDir = currentLogDir;
        this.targetLogDir = targetLogDir;
        this.futureLogDir = futureLogDir;
    }

    @Override
    public boolean done() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActiveMoveState that = (ActiveMoveState) o;
        return Objects.equals(currentLogDir, that.currentLogDir) && Objects.equals(targetLogDir, that.targetLogDir) && Objects.equals(futureLogDir, that.futureLogDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentLogDir, targetLogDir, futureLogDir);
    }
}
