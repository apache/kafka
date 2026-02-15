#!/usr/bin/env python3
"""
Run all git operations to create PR
"""
import subprocess
import os

os.chdir('/Users/mymac/Desktop/kafka')

# Write output to a file
with open('git_setup.log', 'w') as log:
    def run(cmd, desc):
        log.write(f"\n=== {desc} ===\n")
        log.write(f"Command: {' '.join(cmd)}\n")
        result = subprocess.run(cmd, capture_output=True, text=True)
        log.write(f"Stdout: {result.stdout}\n")
        log.write(f"Stderr: {result.stderr}\n")
        log.write(f"Return code: {result.returncode}\n")
        return result

    # Configure git
    run(['git', 'config', 'user.name', 'GT'], 'Configure Git User Name')
    run(['git', 'config', 'user.email', 'gt@example.com'], 'Configure Git Email')

    # Check status
    run(['git', 'status'], 'Check Git Status')

    # Create and checkout branch
    result = run(['git', 'checkout', '-b', 'GT/lz4-content-checksum'], 'Create Branch')
    if result.returncode != 0:
        # Branch exists, just checkout
        run(['git', 'checkout', 'GT/lz4-content-checksum'], 'Checkout Existing Branch')

    # Add the modified file
    run(['git', 'add', 'clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java'], 'Add File')

    # Commit
    commit_msg = """KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream

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
for LZ4 compressed frames, following the LZ4 v1.5.1 frame format specification."""

    run(['git', 'commit', '-m', commit_msg], 'Commit Changes')

    # Push
    run(['git', 'push', '-u', 'origin', 'GT/lz4-content-checksum'], 'Push Branch')

print("Git operations completed. Check git_setup.log for details.")

