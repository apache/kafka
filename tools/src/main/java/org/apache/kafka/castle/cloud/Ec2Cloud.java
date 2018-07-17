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

package org.apache.kafka.castle.cloud;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.castle.cluster.CastleNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class Ec2Cloud implements Cloud, Runnable {
    private static final Logger log = LoggerFactory.getLogger(Ec2Cloud.class);

    /**
     * The minimum delay to observe between queuing a request and performing it.
     * Having a coalsce delay helps increase the amount of batching we do.
     * By waiting for a short period, we may encounter other requests that could
     * be done in the same batch.
     */
    private final static int COALSCE_DELAY_MS = 20;

    /**
     * The minimum delay to observe between making a request and making a
     * subsequent request.  This helps avoid exceeding the ec2 rate limiting.
     */
    private final static int CALL_DELAY_MS = 500;

    private static final String IMAGE_ID_DEFAULT = "ami-29ebb519";

    private static final String INSTANCE_TYPE_DEFAULT = "m1.small";

    private final Settings settings;
    private final AmazonEC2 ec2;
    private final Thread thread;
    private final List<Ec2Runner> runs = new ArrayList<>();
    private final List<DescribeInstance> describes = new ArrayList<>();
    private final List<DescribeInstances> describeAlls = new ArrayList<>();
    private final List<TerminateInstance> terminates = new ArrayList<>();
    private boolean shouldExit = false;
    private long nextCallTimeMs = 0;

    public final static class Settings {
        private final String keyPair;
        private final String securityGroup;

        public Settings(String keyPair, String securityGroup) {
            this.keyPair = keyPair;
            this.securityGroup = securityGroup;
        }

        @Override
        public String toString() {
            return "Ec2Cloud(keyPair=" + keyPair +
                ", securityGroup=" + securityGroup + ")";
        }
    }

    private final class Ec2Runner extends Runner {
        private final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();

        @Override
        public String run() throws Exception {
            if (instanceType.isEmpty()) {
                instanceType = INSTANCE_TYPE_DEFAULT;
            }
            if (imageId.isEmpty()) {
                imageId = IMAGE_ID_DEFAULT;
            }
            synchronized (Ec2Cloud.this) {
                runs.add(this);
                updateNextCallTime(COALSCE_DELAY_MS);
                Ec2Cloud.this.notifyAll();
            }
            return future.get();
        }

        @Override
        public int hashCode() {
            return instanceType.hashCode() ^ imageId.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Ec2Runner other = (Ec2Runner) o;
            return instanceType.equals(other.instanceType) &&
                imageId.equals(other.imageId);
        }
    }

    private static final class DescribeInstance {
        private final KafkaFutureImpl<InstanceDescription> future;
        private final String instanceId;

        DescribeInstance(KafkaFutureImpl<InstanceDescription> future, String instanceId) {
            this.future = future;
            this.instanceId = instanceId;
        }
    }

    private static final class DescribeInstances {
        private final KafkaFutureImpl<Collection<InstanceDescription>> future;

        DescribeInstances(KafkaFutureImpl<Collection<InstanceDescription>> future) {
            this.future = future;
        }
    }

    private static final class TerminateInstance {
        private final KafkaFutureImpl<Void> future;
        private final String instanceId;

        TerminateInstance(KafkaFutureImpl<Void> future, String instanceId) {
            this.future = future;
            this.instanceId = instanceId;
        }
    }

    public Ec2Cloud(Settings settings) {
        this.settings = settings;
        this.ec2 = AmazonEC2ClientBuilder.defaultClient();
        this.thread = new Thread(this, "Ec2CloudThread");
        this.thread.start();
    }

    @Override
    public void run() {
        try {
            while (true) {
                synchronized (this) {
                    long delayMs = calculateDelayMs();
                    if (delayMs < 0) {
                        break;
                    } else if (delayMs > 0) {
                        log.trace("Ec2Cloud thread waiting for {} ms.", delayMs);
                        wait(delayMs);
                    } else {
                        makeCalls();
                    }
                }
            }
            log.trace("Ec2Cloud thread exiting");
        } catch (Exception e) {
            log.warn("Ec2Cloud thread exiting with error", e);
            throw new RuntimeException(e);
        } finally {
            synchronized (this) {
                RuntimeException e = new RuntimeException("Ec2Cloud is shutting down.");
                for (Ec2Runner run : runs) {
                    run.future.completeExceptionally(e);
                }
                for (DescribeInstance describe : describes) {
                    describe.future.completeExceptionally(e);
                }
                for (TerminateInstance terminate : terminates) {
                    terminate.future.completeExceptionally(e);
                }
            }
        }
    }

    private synchronized long calculateDelayMs() throws Exception {
        if (shouldExit) {
            // Should exit.
            return -1;
        } else if (runs.isEmpty() && describes.isEmpty() &&
                describeAlls.isEmpty() && terminates.isEmpty()) {
            // Nothing to do.
            return Long.MAX_VALUE;
        } else {
            long now = Time.SYSTEM.milliseconds();
            if (nextCallTimeMs > now) {
                return nextCallTimeMs - now;
            } else {
                return 0;
            }
        }
    }

    private synchronized void makeCalls() throws Exception {
        log.info("Ec2Cloud#makeCalls.  runs.size=" + runs.size());
        if (!runs.isEmpty()) {
            List<Ec2Runner> batchRuns = new ArrayList<>();
            Iterator<Ec2Runner> iter = runs.iterator();
            Ec2Runner firstRunner = iter.next();
            iter.remove();
            batchRuns.add(firstRunner);
            while (iter.hasNext()) {
                Ec2Runner runner = iter.next();
                if (runner.equals(firstRunner)) {
                    batchRuns.add(runner);
                    iter.remove();
                }
            }
            Iterator<Ec2Runner> runInstanceIterator = batchRuns.iterator();
            Exception failureException = new RuntimeException("Unable to create instance");
            try {
                if (settings.keyPair.isEmpty()) {
                    throw new RuntimeException("You must specify a keypair in " +
                        "order to create a new AWS instance.");
                }
                if (settings.securityGroup.isEmpty()) {
                    throw new RuntimeException("You must specify a security group in " +
                        "order to create a new AWS instance.");
                }
                log.info("Ec2Cloud#makeCalls.  batchRuns.size={}, imageId={}, keyName={}, securityGroups={}",
                    batchRuns.size(), firstRunner.imageId, settings.keyPair, settings.securityGroup);
                RunInstancesRequest req = new RunInstancesRequest()
                    .withInstanceType(firstRunner.instanceType)
                    .withImageId(firstRunner.imageId)
                    .withMinCount(batchRuns.size())
                    .withMaxCount(batchRuns.size())
                    .withKeyName(settings.keyPair)
                    .withSecurityGroups(settings.securityGroup);
                RunInstancesResult result = ec2.runInstances(req);
                Reservation reservation = result.getReservation();
                Iterator<Instance> instanceIterator = reservation.getInstances().iterator();
                while (runInstanceIterator.hasNext() && instanceIterator.hasNext()) {
                    Ec2Runner runInstance = runInstanceIterator.next();
                    Instance instance = instanceIterator.next();
                    runInstance.future.complete(instance.getInstanceId());
                }
            } catch (Exception e) {
                failureException = e;
            }
            while (runInstanceIterator.hasNext()) {
                Ec2Runner runInstance = runInstanceIterator.next();
                runInstance.future.completeExceptionally(failureException);
            }
        } else if (!describes.isEmpty()) {
            Map<String, DescribeInstance> idToDescribe = new HashMap<>();
            for (Iterator<DescribeInstance> iter = describes.iterator(); iter.hasNext();
                     iter.remove()) {
                DescribeInstance describe = iter.next();
                idToDescribe.put(describe.instanceId, describe);
            }
            Exception failureException = new RuntimeException("Result did not include instanceID.");
            try {
                DescribeInstancesRequest req = new DescribeInstancesRequest()
                    .withInstanceIds(idToDescribe.keySet());
                DescribeInstancesResult result = ec2.describeInstances(req);
                for (Reservation reservation : result.getReservations()) {
                    for (Instance instance : reservation.getInstances()) {
                        DescribeInstance describeInstance = idToDescribe.get(instance.getInstanceId());
                        if (describeInstance != null) {
                            InstanceDescription description =
                                new InstanceDescription(instance.getInstanceId(),
                                    instance.getPrivateDnsName(),
                                    instance.getPublicDnsName(),
                                    instance.getState().toString());
                            describeInstance.future.complete(description);
                            idToDescribe.remove(instance.getInstanceId());
                        }
                    }
                }
            } catch (Exception e) {
                failureException = e;
            }
            for (Map.Entry<String, DescribeInstance> entry : idToDescribe.entrySet()) {
                entry.getValue().future.completeExceptionally(failureException);
            }
        } else if (!describeAlls.isEmpty()) {
            try {
                if (settings.keyPair.isEmpty()) {
                    throw new RuntimeException("You must specify a keypair with --keypair in " +
                        "order to describe all AWS instances.");
                }
                DescribeInstancesRequest req = new DescribeInstancesRequest().withFilters(
                    new Filter("key-name", Collections.singletonList(settings.keyPair)));
                ArrayList<InstanceDescription> all = new ArrayList<>();
                DescribeInstancesResult result = ec2.describeInstances(req);
                for (Reservation reservation : result.getReservations()) {
                    for (Instance instance : reservation.getInstances()) {
                        all.add(new InstanceDescription(instance.getInstanceId(),
                            instance.getPrivateDnsName(),
                            instance.getPublicDnsName(),
                            instance.getState().toString()));
                    }
                }
                for (DescribeInstances describeAll : describeAlls) {
                    describeAll.future.complete(all);
                }
            } catch (Exception e) {
                for (DescribeInstances describeAll : describeAlls) {
                    describeAll.future.completeExceptionally(e);
                }
            }
        } else if (!terminates.isEmpty()) {
            Map<String, TerminateInstance> idToTerminate = new HashMap<>();
            for (Iterator<TerminateInstance> iter = terminates.iterator(); iter.hasNext();
                     iter.remove()) {
                TerminateInstance terminate = iter.next();
                idToTerminate.put(terminate.instanceId, terminate);
            }
            TerminateInstancesRequest req = new TerminateInstancesRequest()
                .withInstanceIds(idToTerminate.keySet());
            ec2.terminateInstances(req);
            for (TerminateInstance terminateInstance : idToTerminate.values()) {
                terminateInstance.future.complete(null);
            }
        }
        updateNextCallTime(CALL_DELAY_MS);
    }

    private synchronized void updateNextCallTime(long minDelay) {
        nextCallTimeMs = Math.max(nextCallTimeMs, Time.SYSTEM.milliseconds() + minDelay);
    }

    @Override
    public void close() throws InterruptedException {
        synchronized (this) {
            shouldExit = true;
            notifyAll();
        }
        thread.join();
        ec2.shutdown();
    }

    @Override
    public Runner newRunner() {
        return new Ec2Runner();
    }

    @Override
    public InstanceDescription describeInstance(String instanceId) throws Exception {
        KafkaFutureImpl<InstanceDescription> future = new KafkaFutureImpl<>();
        synchronized (this) {
            describes.add(new DescribeInstance(future, instanceId));
            updateNextCallTime(COALSCE_DELAY_MS);
            notifyAll();
        }
        return future.get();
    }

    @Override
    public Collection<InstanceDescription> describeInstances() throws Exception {
        KafkaFutureImpl<Collection<InstanceDescription>> future = new KafkaFutureImpl<>();
        synchronized (this) {
            describeAlls.add(new DescribeInstances(future));
            updateNextCallTime(COALSCE_DELAY_MS);
            notifyAll();
        }
        return future.get();
    }

    private KafkaFutureImpl<Void> terminateInstance(String instanceId) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        synchronized (this) {
            terminates.add(new TerminateInstance(future, instanceId));
            updateNextCallTime(COALSCE_DELAY_MS);
            notifyAll();
        }
        return future;
    }

    @Override
    public void terminateInstances(String... instanceIds) throws Throwable {
        List<KafkaFutureImpl<Void>> futures = new ArrayList<>();
        for (String instanceId : instanceIds) {
            futures.add(terminateInstance(instanceId));
        }
        Throwable exception = null;
        for (KafkaFutureImpl<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (exception == null) {
                    exception = e.getCause();
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public RemoteCommand remoteCommand(CastleNode node) {
        return new CastleRemoteCommand(node);
    }

    @Override
    public String toString() {
        return settings.toString();
    }
}
