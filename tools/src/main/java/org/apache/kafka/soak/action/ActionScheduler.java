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

package org.apache.kafka.soak.action;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.trogdor.common.ThreadUtils;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs roles for cluster nodes.
 */
public final class ActionScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ActionScheduler.class);

    /**
     * A mutable builder object used to construct the ActionScheduler.
     */
    public static class Builder {
        private final SoakCluster cluster;
        private final Set<String> targetNames = new HashSet<>();
        private final HashMap<ActionId, Action> actions = new HashMap<>();

        public Builder(SoakCluster cluster) {
            this.cluster = cluster;
        }

        public Builder addTargetName(String targetName) {
            targetNames.add(targetName);
            return this;
        }

        public Builder addTargetNames(Collection<String> targetNames) {
            for (String targetName : targetNames) {
                addTargetName(targetName);
            }
            return this;
        }

        public Builder addActions(Collection<Action> actions) {
            for (Action action : actions) {
                addAction(action);
            }
            return this;
        }

        public Builder addAction(Action action) {
            actions.put(action.id(), action);
            return this;
        }

        public ActionScheduler build() {
            Set<ActionId> targetActions = findTargetActions();
            Map<ActionId, ActionData> universe = findUniverse(targetActions);
            if (log.isDebugEnabled()) {
                log.debug("Building scheduler with targetActions {}, universe {}",
                    Utils.join(targetActions, ", "), Utils.join(universe.keySet(), ", "));
            }
            return new ActionScheduler(cluster, targetActions, universe);
        }

        private Set<ActionId> findTargetActions() {
            HashSet<ActionId> targetActions = new HashSet<>();
            for (String targetName : targetNames) {
                // Parse the target ID string.
                TargetId targetId = TargetId.parse(targetName);
                // Convert "ALL" target IDs into concrete action IDs.
                if (targetId.hasGlobalScope()) {
                    Set<ActionId> actionIds = targetId.toActionIds(cluster.nodes().keySet());
                    // Check that the target IDs exist.
                    boolean missing = true;
                    for (ActionId actionId : actionIds) {
                        if (actions.containsKey(actionId)) {
                            missing = false;
                            targetActions.add(actionId);
                        }
                    }
                    if (missing) {
                        HashSet<String> allTypes = new HashSet<>();
                        for (ActionId actionId : actions.keySet()) {
                            allTypes.add(actionId.type());
                        }
                        throw new RuntimeException("Unknown target " + targetId +
                            ".  Valid target types are: " + Utils.join(allTypes, ", "));
                    }
                } else {
                    ActionId actionId = new ActionId(targetId.type(), targetId.scope());
                    if (!actions.containsKey(actionId)) {
                        throw new RuntimeException("Unknown target " + actionId +
                            ".  Valid targets are: " + Utils.join(actions.keySet(), ", "));
                    }
                    targetActions.add(actionId);
                }
            }
            return targetActions;
        }

        private Map<ActionId, ActionData> findUniverse(Set<ActionId> targetActions) {
            // Create an action universe with the targets, plus everything they contain.
            Map<ActionId, ActionData> universe = new HashMap<>();
            Deque<ActionId> toAdd = new ArrayDeque<>(targetActions);
            while (true) {
                ActionId id = toAdd.poll();
                if (id == null) {
                    break;
                }
                if (!universe.containsKey(id)) {
                    Action action = actions.get(id);
                    if (action != null) {
                        universe.put(id, new ActionData(action));
                        for (String type : action.contains()) {
                            toAdd.add(new ActionId(type, id.scope()));
                        }
                    }
                }
            }
            for (ActionData actionData : universe.values()) {
                for (ActionId containedId : actionData.action.containedIds()) {
                    ActionData childData = universe.get(containedId);
                    if (childData != null) {
                        actionData.children.add(containedId);
                        childData.parents.add(actionData.action.id());
                    }
                }
                // Convert "ALL" target IDs into concrete action IDs.
                Set<ActionId> beforeIds = TargetId.actionIds(actionData.action.comesAfter(),
                    cluster.nodes().keySet());
                for (ActionId beforeId : beforeIds) {
                    ActionData beforeData = universe.get(beforeId);
                    if (beforeData != null) {
                        actionData.comesAfter.add(beforeId);
                        beforeData.comesBefore.add(actionData.action.id());
                    }
                }
            }
            boolean canStart = false;
            for (ActionId id : targetActions) {
                ActionData actionData = universe.get(id);
                if (actionData.parents.isEmpty()) {
                    actionData.state = ActionState.RUNNABLE;
                    canStart = true;
                }
            }
            if ((!canStart) && (!universe.isEmpty())) {
                throw new RuntimeException("No tasks can be executed!  Check " +
                    "for circular dependencies.");
            }
            return universe;
        }
    }

    /**
     * Schedules an action, if the action is scheduleable.  This runnable takes
     * place in the context of the single-threaded schedulerExecutor, and can access
     * all scheduler fields.
     */
    private final class MaybeSchedule implements Runnable {
        private final ActionId actionId;

        MaybeSchedule(ActionId actionId) {
            this.actionId = actionId;
        }

        @Override
        public void run() {
            try {
                ActionData actionData = universe.get(actionId);
                if (actionData.state != ActionState.RUNNABLE) {
                    log.trace("Can't schedule {} because it is in state {}",
                        actionId, actionData.state);
                    return;
                }
                if (!actionData.comesAfter.isEmpty()) {
                    log.trace("Must complete {} before starting {}",
                        Utils.join(actionData.comesAfter, ", "), actionId);
                    return;
                }
                actionData.state = ActionState.EXECUTING;
                if (actionData.action.initialDelayMs() > 0) {
                    log.debug("Scheduling {} in {} ms", actionId, actionData.action.initialDelayMs());
                    nodeExecutors.get(actionId.scope()).schedule(
                        new ExecuteAction(actionData.action, cluster.nodes().get(actionId.scope())),
                        actionData.action.initialDelayMs(), TimeUnit.MILLISECONDS);
                } else {
                    log.debug("Scheduling {}", actionId);
                    nodeExecutors.get(actionId.scope()).submit(
                        new ExecuteAction(actionData.action, cluster.nodes().get(actionId.scope())));
                }
            } catch (Throwable throwable) {
                cluster.clusterLog().error("** MaybeSchedule got fatal exception", throwable);
                shutdownFuture.completeExceptionally(throwable);
            }
        }
    }

    /**
     #* Executes an Action.  Because this takes place in the context of a node executor.
     * It cannot access ActionScheduler fields directly.
     */
    private final class ExecuteAction implements Runnable {
        private final Action action;
        private final SoakNode node;

        ExecuteAction(Action action, SoakNode node) {
            this.action = action;
            this.node = node;
        }

        @Override
        public void run() {
            try {
                SoakLog.debugToAll(String.format("** Running %s", action.id()),
                    node.log(), cluster.clusterLog());
                action.call(cluster, node);
                schedulerExecutor.submit(new FinishRunningAction(action));
            } catch (Throwable throwable) {
                String msg = "** ExecuteAction " + action.id() + " failed";
                node.log().error(msg, throwable);
                cluster.clusterLog().error(msg, throwable);
                shutdownFuture.completeExceptionally(throwable);
            }
        }
    }

    /**
     * Finish running an action, once the Action#call method has completed.  This
     * runnable takes place in the context of the single-threaded schedulerExecutor,
     * and can access all scheduler fields.
     */
    private final class FinishRunningAction implements Runnable {
        private final Action action;

        FinishRunningAction(Action action) {
            this.action = action;
        }

        @Override
        public void run() {
            try {
                ActionData actionData = universe.get(action.id());
                if (actionData.state != ActionState.EXECUTING) {
                    throw new RuntimeException("FinishRunningAction invoked for " + action.id() +
                        ", which is not in EXECUTING state.");
                }
                actionData.state = ActionState.WAITING_FOR_CHILDREN;
                for (ActionId childId : actionData.children) {
                    ActionData childData = universe.get(childId);
                    if (childData.state == ActionState.PENDING) {
                        log.trace("Setting state for child action {} to RUNNABLE", childId);
                        childData.state = ActionState.RUNNABLE;
                        schedulerExecutor.submit(new MaybeSchedule(childId));
                    }
                }
                if (actionData.children.isEmpty()) {
                    schedulerExecutor.submit(new MaybeCompleteAction(action.id()));
                }
            } catch (Throwable throwable) {
                cluster.clusterLog().error("** FinishRunningAction got fatal exception", throwable);
                shutdownFuture.completeExceptionally(throwable);
            }
        }
    }

    /**
     * Complete an action if possible.  This runnable takes place in the context of the
     * single-threaded schedulerExecutor, and can access all scheduler fields.
     */
    private final class MaybeCompleteAction implements Runnable {
        private final ActionId actionId;

        MaybeCompleteAction(ActionId actionId) {
            this.actionId = actionId;
        }

        @Override
        public void run() {
            try {
                ActionData actionData = universe.get(actionId);
                if (actionData.state != ActionState.WAITING_FOR_CHILDREN) {
                    log.trace("Can't complete {} because it is not in WAITING_FOR_CHILDREN state.",
                        actionId);
                    return;
                }
                if (!actionData.children.isEmpty()) {
                    log.trace("Can't complete {} yet.  Waiting for children: {}",
                        actionId, Utils.join(actionData.children, ", "));
                    return;
                }
                SoakLog.debugToAll(String.format("** Finished %s", actionId),
                    cluster.nodes().get(actionId.scope()).log(), cluster.clusterLog());
                actionData.state = ActionState.COMPLETED;
                numCompleted++;
                for (Iterator<ActionId> iter = actionData.parents.iterator(); iter.hasNext(); ) {
                    ActionId parentId = iter.next();
                    ActionData parentData = universe.get(parentId);
                    parentData.children.remove(actionId);
                    if (parentData.state == ActionState.WAITING_FOR_CHILDREN) {
                        schedulerExecutor.submit(new MaybeCompleteAction(parentId));
                    }
                    iter.remove();
                }
                for (Iterator<ActionId> iter = actionData.comesBefore.iterator(); iter.hasNext(); ) {
                    ActionId afterId = iter.next();
                    ActionData afterData = universe.get(afterId);
                    afterData.comesAfter.remove(actionId);
                    schedulerExecutor.submit(new MaybeSchedule(afterId));
                    iter.remove();
                }
                if (numCompleted == universe.size()) {
                    shutdownFuture.complete(null);
                }
            } catch (Throwable throwable) {
                cluster.clusterLog().error("** MaybeCompleteAction got fatal exception", throwable);
                shutdownFuture.completeExceptionally(throwable);
            }
        }
    }

    enum ActionState {
        PENDING,
        RUNNABLE,
        EXECUTING,
        WAITING_FOR_CHILDREN,
        COMPLETED;
    }

    private static class ActionData {
        private final Action action;
        private ActionState state = ActionState.PENDING;
        private final Set<ActionId> comesBefore = new HashSet<>();
        private final Set<ActionId> comesAfter = new HashSet<>();
        private final Set<ActionId> parents = new HashSet<>();
        private final Set<ActionId> children = new HashSet<>();

        ActionData(Action action) {
            this.action = action;
        }
    }

    /**
     * The soak cluter to use for this scheduler.
     */
    private final SoakCluster cluster;

    /**
     * All actions which this scheduler will run.
     */
    private final Map<ActionId, ActionData> universe;

    /**
     * The number of completed actions.
     */
    private int numCompleted = 0;

    /**
     * True if the scheduler has shut down.
     */
    private KafkaFutureImpl<Void> shutdownFuture;

    /**
     * The single-threaded scheduler executor which coordinates running actions.
     */
    private final ExecutorService schedulerExecutor;

    /**
     * A map from node names to executor services.
     * Node executors run actions in separate threads.
     */
    private final Map<String, ScheduledExecutorService> nodeExecutors;

    private ActionScheduler(SoakCluster cluster,
                            Set<ActionId> targetActions,
                            Map<ActionId, ActionData> universe) {
        this.cluster = cluster;
        this.universe = universe;
        this.shutdownFuture = new KafkaFutureImpl<>();
        this.schedulerExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("ActionSchedulerThread", false));
        this.nodeExecutors = new HashMap<>();
        for (String nodeName : cluster.nodes().keySet()) {
            this.nodeExecutors.put(nodeName, Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory(
                    "ActionSchedulerNodeExecutor[" + nodeName + "]", false)));
        }
        if (universe.isEmpty()) {
            shutdownFuture.complete(null);
        } else {
            for (ActionId id : targetActions) {
                schedulerExecutor.submit(new MaybeSchedule(id));
            }
        }
    }

    /**
     * Wait for the scheduler to finish.
     *
     * @param duration  The duration.
     * @param timeUnit  The time unit to use for the duration.
     */
    public void await(long duration, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        shutdownFuture.get(duration, timeUnit);
    }

    /**
     * Shut down this scheduler.
     */
    @Override
    public void close() throws Exception {
        shutdownFuture.completeExceptionally(
            new InterruptedException("The scheduler is shutting down."));
        for (ExecutorService executorService : nodeExecutors.values()) {
            executorService.shutdownNow();
        }
        for (ExecutorService executorService : nodeExecutors.values()) {
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
        schedulerExecutor.shutdownNow();
        schedulerExecutor.awaitTermination(1, TimeUnit.DAYS);
    }
}
