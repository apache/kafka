package kafka.utils

import joptsimple.{OptionSpec, OptionSet, OptionParser}

/**
 * Helper functions for dealing with command line utilities
 */
object CommandLineUtils {

    def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*) {
      for(arg <- required) {
        if(!options.has(arg)) {
          error("Missing required argument \"" + arg + "\"")
          parser.printHelpOn(System.err)
          System.exit(1)
        }
      }
    }
  
}