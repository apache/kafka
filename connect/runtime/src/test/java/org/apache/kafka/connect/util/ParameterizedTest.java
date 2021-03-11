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

  public ParameterizedTest (Class<?> klass) throws Throwable {
    super(klass);
  }

  @Override
  public void filter(Filter filter) throws NoTestsRemainException {
    super.filter(new FilterDecorator(filter));
  }

  private static String deparametrizeName(String name) {
    //Each parameter is named as [0], [1] etc
    if(name.startsWith("[")){
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