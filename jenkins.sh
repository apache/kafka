#!/bin/bash
./gradlew clean compileJava compileScala compileTestJava compileTestScala checkstyleMain checkstyleTest unitTest integrationTest --no-daemon -Dorg.gradle.project.testLoggingEvents=started,passed,skipped,failed $@
