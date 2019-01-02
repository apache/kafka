package kafka.utils

import java.util.Properties

import joptsimple.{OptionSet, OptionSpec}

object ConfigUtils {
  /**
    * Merge the options into {@code props} for key {@code key}, with the following precedence, from high to low:
    * 1) if {@code spec} is specified on {@code options} explicitly, use the value;
    * 2) if {@code props} already has {@code key} set, keep it;
    * 3) otherwise, use the default value of {@code spec}.
    * A {@code null} value means to remove {@code key} from the {@code props}.
    */
  def maybeMergeOptions[V](props: Properties, key: String, options: OptionSet, spec: OptionSpec[V]) {
    if (options.has(spec) || !props.containsKey(key)) {
      val value = options.valueOf(spec)
      if (value == null)
        props.remove(key)
      else
        props.put(key, value.toString)
    }
  }
}
