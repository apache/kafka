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

package org.apache.kafka.trogdor.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.rest.CoordinatorFaultsResponse;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateCoordinatorFaultRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.JsonRestServer.HttpResponse;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * A client for the Trogdor coordinator.
 */
public class CoordinatorClient {
    /**
     * The URL target.
     */
    private final String target;

    public CoordinatorClient(String host, int port) {
        this(String.format("%s:%d", host, port));
    }

    public CoordinatorClient(String target) {
        this.target = target;
    }

    private String url(String suffix) {
        return String.format("http://%s%s", target, suffix);
    }

    public CoordinatorStatusResponse getStatus() throws Exception {
        HttpResponse<CoordinatorStatusResponse> resp =
            JsonRestServer.<CoordinatorStatusResponse>httpRequest(url("/coordinator/status"), "GET",
                null, new TypeReference<CoordinatorStatusResponse>() { });
        return resp.body();
    }

    public CoordinatorFaultsResponse getFaults() throws Exception {
        HttpResponse<CoordinatorFaultsResponse> resp =
            JsonRestServer.<CoordinatorFaultsResponse>httpRequest(url("/coordinator/faults"), "GET",
                null, new TypeReference<CoordinatorFaultsResponse>() { });
        return resp.body();
    }

    public void putFault(CreateCoordinatorFaultRequest request) throws Exception {
        HttpResponse<CreateCoordinatorFaultRequest> resp =
            JsonRestServer.<CreateCoordinatorFaultRequest>httpRequest(url("/coordinator/fault"), "PUT",
                request, new TypeReference<CreateCoordinatorFaultRequest>() { });
        resp.body();
    }

    public void shutdown() throws Exception {
        HttpResponse<Empty> resp =
            JsonRestServer.<Empty>httpRequest(url("/coordinator/shutdown"), "PUT",
                null, new TypeReference<Empty>() { });
        resp.body();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-coordinator-client")
            .defaultHelp(true)
            .description("The Trogdor fault injection coordinator client.");
        parser.addArgument("target")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("target")
            .metavar("TARGET")
            .help("A colon-separated host and port pair.  For example, example.com:8889");
        MutuallyExclusiveGroup actions = parser.addMutuallyExclusiveGroup();
        actions.addArgument("--status")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("status")
            .help("Get coordinator status.");
        actions.addArgument("--get-faults")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("get_faults")
            .help("Get coordinator faults.");
        actions.addArgument("--create-fault")
            .action(store())
            .type(String.class)
            .dest("create_fault")
            .metavar("FAULT_JSON")
            .help("Create a new fault.");
        actions.addArgument("--shutdown")
            .action(storeTrue())
            .type(Boolean.class)
            .dest("shutdown")
            .help("Trigger coordinator shutdown");

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
        String target = res.getString("target");
        CoordinatorClient client = new CoordinatorClient(target);
        if (res.getBoolean("status")) {
            System.out.println("Got coordinator status: " +
                JsonUtil.toPrettyJsonString(client.getStatus()));
        } else if (res.getBoolean("get_faults")) {
            System.out.println("Got coordinator faults: " +
                JsonUtil.toPrettyJsonString(client.getFaults()));
        } else if (res.getString("create_fault") != null) {
            client.putFault(JsonUtil.JSON_SERDE.readValue(res.getString("create_fault"),
                CreateCoordinatorFaultRequest.class));
            System.out.println("Created fault.");
        } else if (res.getBoolean("shutdown")) {
            client.shutdown();
            System.out.println("Sent shutdown request.");
        } else {
            System.out.println("You must choose an action. Type --help for help.");
            Exit.exit(1);
        }
    }
};
