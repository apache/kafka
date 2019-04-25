#!/bin/bash
./gradlew clean
gradle
./gradlew -PscalaVersion=2.11 releaseTarGz


