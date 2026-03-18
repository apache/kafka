# Verify replication-setup without Docker (config, topics, script structure).
# Run: .\verify-setup.ps1
# Exit 0 = all checks passed.

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$failed = 0

function Test-Check {
    param([string]$Name, [bool]$Ok, [string]$Message)
    if ($Ok) {
        Write-Host "[PASS] $Name" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] $Name - $Message" -ForegroundColor Red
        $script:failed++
    }
}

Write-Host "`n--- Replication setup verification (no Docker) ---`n"

# 1. docker-compose.yml exists and has required services
$composePath = Join-Path $scriptDir "docker-compose.yml"
$composeExists = Test-Path $composePath
Test-Check "docker-compose.yml exists" $composeExists "File not found"

if ($composeExists) {
    $content = Get-Content $composePath -Raw
    $hasPrimary = $content -match "kafka-primary"
    $hasStandby = $content -match "kafka-standby"
    $hasMM2 = $content -match "mirror-maker"
    $hasTools = $content -match "kafka-tools"
    $hasOrderTopic = $content -match "order-events"
    $hasPaymentTopic = $content -match "payment-events"
    Test-Check "docker-compose: kafka-primary" $hasPrimary "Service not found"
    Test-Check "docker-compose: kafka-standby" $hasStandby "Service not found"
    Test-Check "docker-compose: mirror-maker" $hasMM2 "Service not found"
    Test-Check "docker-compose: kafka-tools" $hasTools "Service not found"
    Test-Check "docker-compose: order-events ref" $hasOrderTopic "Topic ref not found"
    Test-Check "docker-compose: payment-events ref" $hasPaymentTopic "Topic ref not found"
}

# 2. mm2.properties exists and config matches
$mm2Path = Join-Path $scriptDir "config\mm2.properties"
$mm2Exists = Test-Path $mm2Path
Test-Check "config/mm2.properties exists" $mm2Exists "File not found"

if ($mm2Exists) {
    $mm2 = Get-Content $mm2Path -Raw
    $mm2Order = $mm2 -match "order-events"
    $mm2Payment = $mm2 -match "payment-events"
    $mm2Primary = $mm2 -match "primary\.bootstrap\.servers"
    $mm2Standby = $mm2 -match "standby\.bootstrap\.servers"
    Test-Check "mm2: order-events" $mm2Order "Topic not in mm2"
    Test-Check "mm2: payment-events" $mm2Payment "Topic not in mm2"
    Test-Check "mm2: primary cluster" $mm2Primary "primary.bootstrap.servers missing"
    Test-Check "mm2: standby cluster" $mm2Standby "standby.bootstrap.servers missing"
}

# 3. run_challenge.sh exists and has scenarios + topics
$runPath = Join-Path $scriptDir "run_challenge.sh"
$runExists = Test-Path $runPath
Test-Check "run_challenge.sh exists" $runExists "File not found"

if ($runExists) {
    $run = Get-Content $runPath -Raw
    Test-Check "run_challenge: ORDER_TOPIC" ($run -match "ORDER_TOPIC=.order-events") "order-events not defined"
    Test-Check "run_challenge: PAYMENT_TOPIC" ($run -match "PAYMENT_TOPIC=.payment-events") "payment-events not defined"
    Test-Check "run_challenge: scenario normal" ($run -match "scenario_normal_replication") "normal scenario missing"
    Test-Check "run_challenge: scenario truncation" ($run -match "scenario_truncation_detection") "truncation scenario missing"
    Test-Check "run_challenge: scenario reset" ($run -match "scenario_topic_reset") "reset scenario missing"
    Test-Check "run_challenge: produce_order_events" ($run -match "produce_order_events") "order producer missing"
    Test-Check "run_challenge: produce_payment_events" ($run -match "produce_payment_events") "payment producer missing"
}

# 4. Event schema doc
$schemaPath = Join-Path $scriptDir "docs\EVENT_SCHEMA.md"
$schemaExists = Test-Path $schemaPath
Test-Check "docs/EVENT_SCHEMA.md exists" $schemaExists "File not found"
if ($schemaExists) {
    $schema = Get-Content $schemaPath -Raw
    Test-Check "EVENT_SCHEMA: ORDER_CREATED" ($schema -match "ORDER_CREATED") "Order event type missing"
    Test-Check "EVENT_SCHEMA: PAYMENT_SUCCESSFUL" ($schema -match "PAYMENT_SUCCESSFUL") "Payment event type missing"
}

Write-Host ""
if ($failed -eq 0) {
    Write-Host "All checks passed. (Docker is required to run full test: .\run_challenge.sh)" -ForegroundColor Green
    exit 0
} else {
    Write-Host "$failed check(s) failed." -ForegroundColor Red
    exit 1
}
