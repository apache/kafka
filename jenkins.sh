#!/bin/bash
./gradlew clean compileJava compileScala compileTestJava compileTestScala checkstyleMain checkstyleTest unitTest integrationTest $@
