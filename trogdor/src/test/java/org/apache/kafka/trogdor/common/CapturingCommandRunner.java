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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.basic.BasicPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CapturingCommandRunner implements BasicPlatform.CommandRunner {
    private static final Logger log = LoggerFactory.getLogger(CapturingCommandRunner.class);

    private final Map<String, List<String>> commands = new HashMap<>();

    private synchronized List<String> getOrCreate(String nodeName) {
        List<String> lines = commands.get(nodeName);
        if (lines != null) {
            return lines;
        }
        lines = new LinkedList<>();
        commands.put(nodeName, lines);
        return lines;
    }

    @Override
    public String run(Node curNode, String[] command) throws IOException {
        String line = Utils.join(command, " ");
        synchronized (this) {
            getOrCreate(curNode.name()).add(line);
        }
        log.debug("RAN {}: {}", curNode, Utils.join(command, " "));
        return "";
    }

    public synchronized List<String> lines(String nodeName) {
        return new ArrayList<String>(getOrCreate(nodeName));
    }
}
