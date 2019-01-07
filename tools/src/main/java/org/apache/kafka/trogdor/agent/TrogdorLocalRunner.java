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
package org.apache.kafka.trogdor.agent;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.SpecUtils;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.task.AgentWorkerStatusTracker;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.io.IOException;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class TrogdorLocalRunner {
    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("TrogdorLocalRunner").defaultHelp(true).description("Run Trogdor Spec locally.");
        parser.addArgument("--spec")
                .action(store())
                .required(true)
                .dest("spec")
                .metavar("SPEC_JSON")
                .help("Trogdor SPEC JSON string or a SPEC file path starting with @ (@/spec/file/path)");
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

        String specString = null;
        try {
            specString = SpecUtils.readSpec(res.getString("spec"));
        } catch (IOException e){
            System.err.print("Failed to read Trogdor SPEC argument, exception: " + e);
            parser.printHelp();
            Exit.exit(1);
        }

        CreateWorkerRequest req = JsonUtil.JSON_SERDE.readValue(specString, CreateWorkerRequest.class);
        System.out.println("Start a task " + req.taskId() + " with SPEC: " + req.spec());
        runSpec(req);
    }

    private static void runSpec(CreateWorkerRequest request){
        final TaskSpec spec = request.spec();
        final TaskWorker worker = spec.newTaskWorker("0");
        final AgentWorkerStatusTracker status = new AgentWorkerStatusTracker();
        KafkaFutureImpl<String> haltFuture = new KafkaFutureImpl<>();
        haltFuture.thenApply((KafkaFuture.BaseFunction<String, Void>) errorString -> {
            if (errorString == null)
                errorString = "";
            if (errorString.isEmpty()) {
                System.out.println("Result: " + status.get().toString());
                System.exit(0);
            } else {
                System.err.println("Finished SPEC with error " + errorString);
                System.exit(1);
            }
            return null;
        });
        try {
            worker.start(null, status, haltFuture);
        } catch (Exception e) {
            System.err.println("Exception happened: " + e);
            System.exit(1);
        }
    }
}
