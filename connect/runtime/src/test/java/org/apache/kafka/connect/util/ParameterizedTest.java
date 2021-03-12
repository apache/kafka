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
package org.apache.kafka.connect.util;

import java.lang.annotation.Annotation;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Parameterized;

/**
 * Running a single parameterized test causes issue as explained in
 * http://youtrack.jetbrains.com/issue/IDEA-65966 and
 * https://stackoverflow.com/questions/12798079/initializationerror-with-eclipse-and-junit4-when-executing-a-single-test/18438718#18438718
 *
 * As a workaround, the original filter needs to be wrapped and then pass it a deparameterized
 * description which removes the parameter part (See deparametrizeName)
 */
public class ParameterizedTest extends Parameterized {

    public ParameterizedTest(Class<?> klass) throws Throwable {
        super(klass);
    }

    @Override
    public void filter(Filter filter) throws NoTestsRemainException {
        super.filter(new FilterDecorator(filter));
    }

    private static String deparametrizeName(String name) {
        //Each parameter is named as [0], [1] etc
        if (name.startsWith("[")) {
            return name;
        }

        //Convert methodName[index](className) to methodName(className)
        int indexOfOpenBracket = name.indexOf('[');
        int indexOfCloseBracket = name.indexOf(']') + 1;
        return name.substring(0, indexOfOpenBracket).concat(name.substring(indexOfCloseBracket));
    }

    private static Description wrap(Description description) {
        String fixedName = deparametrizeName(description.getDisplayName());
        Description clonedDescription = Description.createSuiteDescription(
            fixedName,
            description.getAnnotations().toArray(new Annotation[0])
        );
        description.getChildren().forEach(child -> clonedDescription.addChild(wrap(child)));
        return clonedDescription;
    }

    private static class FilterDecorator extends Filter {
        private final Filter delegate;

        private FilterDecorator(Filter delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean shouldRun(Description description) {
            return delegate.shouldRun(wrap(description));
        }

        @Override
        public String describe() {
            return delegate.describe();
        }
    }
}