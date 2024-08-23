#!/bin/bash

echo "Running gradlew with timeout"

timeout 5m ./gradlew --build-cache --scan --continue \
  -PtestLoggingEvents=started,passed,skipped,failed \
  -PignoreFailures=true -PmaxParallelForks=2 \
  -PmaxTestRetries=1 -PmaxTestRetryFailures=10 \
  test

exitCode=$?
if [ exitCode -eq 124 ]; then
    echo "Timed out. Attempting to publish build scan"
    ./gradlew buildScanPublishPrevious
    exit 1
elif [ exitCode -eq 127 ]; then
    echo "Killed. Attempting to publish build scan"
    ./gradlew buildScanPublishPrevious
    exit 1
fi

exit exitCode