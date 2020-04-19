package org.apache.kafka.connect.health;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class TaskStateTest {

    TaskState taskState;

    int taskId = 6;

    String state = "enabled";

    String workerId = "peasant";

    String trace = "";

    @Before
    public void setupBeforeTests() {
        taskState = new TaskState(taskId, state, workerId, trace);
    }

    @Test
    public void testTaskId() {
        Assert.assertEquals(taskId, taskState.taskId());
    }

    @Test
    public void testEqualsTrueSameObject() {
        Assert.assertEquals(taskState, taskState);
    }

    @Test
    public void testEqualsTrueDifferentObjects() {
        Assert.assertEquals(taskState, new TaskState(taskId, state, workerId, trace));
    }

    @Test
    public void testEqualsFalseNullObj() {
        Assert.assertNotEquals(taskState, null);
    }

    @Test
    public void testEqualsFalseOtherObjectType() {
        Assert.assertNotEquals(taskState, new HashMap<String, String>());
    }

    @Test
    public void testHashCodeNotEqual() {
        TaskState otherTaskState = new TaskState(7, state, workerId, trace);
        Assert.assertNotEquals(taskState.hashCode(), otherTaskState.hashCode());
    }

    @Test
    public void testHashCodeEqual() {
        TaskState otherTaskState = new TaskState(taskId, state, workerId, trace);
        Assert.assertEquals(taskState.hashCode(), otherTaskState.hashCode());
    }

    @Test
    public void testToString() {

        String expect = "TaskState{"
                + "taskId='" + taskState.taskId() + '\''
                + "state='" + taskState.state() + '\''
                + ", traceMessage='" + taskState.traceMessage() + '\''
                + ", workerId='" + taskState.workerId() + '\''
                + '}';

        Assert.assertEquals(expect, taskState.toString());
    }
}