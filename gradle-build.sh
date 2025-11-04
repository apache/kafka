#!/bin/bash
# Gradle build script for Cursor IDE

# Source the shell environment to get jenv
source ~/.zshrc

# Change to the kafka directory
cd "$(dirname "$0")"

# Run gradle with the provided arguments
./gradlew "$@"

