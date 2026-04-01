=== 🚀 MM2 Test Script (On-demand Producer) ===

🌐 [EVENT] Using Docker network: mirrormaker2_default

⏳ [EVENT] Waiting for primary Kafka...
✅ [EVENT] Primary Kafka is ready

⏳ [EVENT] Waiting for standby Kafka...
✅ [EVENT] Standby Kafka is ready

⏳ [EVENT] Waiting for MirrorMaker2...
✅ [EVENT] MirrorMaker2 is running

📌 [EVENT] Creating topic: commit-log
⏳ [EVENT] Waiting for MM2 topic discovery...

===============================
✅ Scenario 1: Normal Replication
================================

📤 [EVENT] Producing 20 messages...

⏳ [EVENT] Waiting for topic replication in standby...
📥 [EVENT] Consuming from standby...

🔍 [EVENT] Records replicated: 20
✅ TEST 1 PASSED

===============================
🔥 Scenario 2: Log Truncation
=============================

⏸️ [EVENT] Pausing MM2...

⚙️ [EVENT] Setting aggressive retention (10 sec)...
📤 [EVENT] Producing 500 messages...
📤 [EVENT] Producing 500 messages...

⏳ [EVENT] Waiting for truncation...

▶️ [EVENT] Resuming MM2...

📡 [EVENT] Capturing MM2 logs...

📄 [EVENT] Checking truncation detection...
✅ TEST 2 PASSED

📄 [MM2 LOG]
[MM2-FIX][TRUNCATION] Detected log truncation on topic commit-log partition 0
[MM2-FIX][TRUNCATION] Source offset moved backward. Triggering corrective action.

===============================
🔥 Scenario 3: Topic Reset
==========================

⏸️ [EVENT] Pausing MM2...

📤 [EVENT] Producing 500 messages...

🗑️ [EVENT] Deleting topic...
♻️ [EVENT] Recreating topic...

▶️ [EVENT] Resuming MM2...

📤 [EVENT] Producing 500 messages...

📡 [EVENT] Capturing MM2 logs...

📄 [EVENT] Checking topic reset detection...
✅ TEST 3 PASSED

📄 [EVENT] Checking recovery...
✅ RECOVERY PASSED

📄 [MM2 LOG]
[MM2-FIX][TOPIC-RESET] Topic commit-log recreated. Reset detected.
[MM2-FIX][RECOVERY] Reinitializing offsets and resuming replication from earliest.
[MM2-FIX][RECOVERY] Recovery successful. Replication resumed without data loss.

🎯 ALL TESTS COMPLETED
