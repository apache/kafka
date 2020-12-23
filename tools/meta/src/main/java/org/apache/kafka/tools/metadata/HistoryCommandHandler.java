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

package org.apache.kafka.tools.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Implements the history command.
 */
public final class HistoryCommandHandler implements Command.Handler {
    private static final Logger log = LoggerFactory.getLogger(HistoryCommandHandler.class);

    private final int numEntriesToShow;

    public HistoryCommandHandler(int numEntriesToShow) {
        this.numEntriesToShow = numEntriesToShow;
    }

    @Override
    public void run(Optional<MetadataShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) throws Exception {
        if (!shell.isPresent()) {
            throw new RuntimeException("The history command requires a shell.");
        }
        Iterator<Map.Entry<Integer, String>> iter = shell.get().history(numEntriesToShow);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            writer.printf("% 5d  %s%n", entry.getKey(), entry.getValue());
        }
    }
}
