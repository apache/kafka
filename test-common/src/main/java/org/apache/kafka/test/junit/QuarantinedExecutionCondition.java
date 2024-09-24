package org.apache.kafka.test.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class QuarantinedExecutionCondition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        extensionContext.getTestMethod().ifPresent(method -> {
            Class<?> clazz = extensionContext.getRequiredTestClass();

        });

        return ConditionEvaluationResult.enabled("default");
    }
}
