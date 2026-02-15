#!/bin/bash
cd /Users/mymac/Desktop/kafka

echo "=== Checking git status ==="
git status

echo "=== Creating branch GT/lz4-content-checksum ==="
git checkout -b GT/lz4-content-checksum

echo "=== Adding changes ==="
git add clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java

echo "=== Committing changes ==="
git commit -m "KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream

This commit implements the content checksum verification feature for LZ4
compression as indicated by existing TODOs in the codebase.

Changes:
- Added CONTENT_CHECKSUM_SIZE constant (4 bytes)
- Added CONTENT_CHECKSUM_MISMATCH error message constant
- Added contentChecksum field for tracking running checksum
- Added checksumBuffer for direct buffer handling
- Implemented content checksum verification in readBlock() method
- Content checksum is computed as XOR of all block checksums

The content checksum provides end-to-end data integrity verification
for LZ4 compressed frames, following the LZ4 v1.5.1 frame format specification."

echo "=== Pushing branch ==="
git push -u origin GT/lz4-content-checksum

echo "=== Done ==="

