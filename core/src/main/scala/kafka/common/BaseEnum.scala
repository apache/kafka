package kafka.common

/*
 * We inherit from `Product` and `Serializable` because `case` objects and classes inherit from them and if we don't
 * do it here, the compiler will infer types that unexpectedly include `Product` and `Serializable`, see
 * http://underscore.io/blog/posts/2015/06/04/more-on-sealed.html for more information.
 */
trait BaseEnum extends Product with Serializable {
  def name: String
}
