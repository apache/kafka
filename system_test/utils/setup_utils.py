#!/usr/bin/env python

import logging
import kafka_system_test_utils
import sys

class SetupUtils():

    # dict to pass user-defined attributes to logger argument: "extra"
    # to use: just update "thisClassName" to the appropriate value
    thisClassName = '(ReplicaBasicTest)'
    d = {'name_of_class': thisClassName}

    logger     = logging.getLogger("namedLogger")
    anonLogger = logging.getLogger("anonymousLogger")

    def __init__(self):
        d = {'name_of_class': self.__class__.__name__}
        self.logger.info("constructor", extra=SetUpUtils.d)


    def log_message(self, message):
        print
        self.anonLogger.info("======================================================")
        self.anonLogger.info(message)
        self.anonLogger.info("======================================================")

