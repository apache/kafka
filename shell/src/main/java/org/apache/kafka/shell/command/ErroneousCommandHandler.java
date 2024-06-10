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

package org.apache.kafka.shell.command;

import org.apache.kafka.shell.InteractiveShell;
import org.apache.kafka.shell.state.MetadataShellState;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.Optional;

/**
 * Handles erroneous commands.
 */
public final class ErroneousCommandHandler implements Commands.Handler {
    private final String message;

    public ErroneousCommandHandler(String message) {
        this.message = message;
    }

    @Override
    public void run(
        Optional<InteractiveShell> shell,
        PrintWriter writer,
        MetadataShellState state
    ) {
        writer.println(message);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(message);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ErroneousCommandHandler)) return false;
        ErroneousCommandHandler o = (ErroneousCommandHandler) other;
        if (!Objects.equals(o.message, message)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "ErroneousCommandHandler(" + message + ")";
    }
}
