package kafka.admin

/**
  * Created by awang on 2/17/16.
  */
object RackAwareMode {
  case object Disabled extends RackAwareMode
  case object Enforced extends RackAwareMode
  case object Default extends RackAwareMode
}

sealed trait RackAwareMode
