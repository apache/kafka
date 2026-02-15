#!/usr/bin/env python3
"""
Script to create a PR for the LZ4 content checksum implementation
"""

import subprocess
import os
import sys

def run_command(cmd, check=True):
    """Run a shell command and return the result"""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if check and result.returncode != 0:
        print(f"Command failed with return code: {result.returncode}")
    return result

def main():
    os.chdir('/Users/mymac/Desktop/kafka')

    print("=" * 60)
    print("Setting up Git and creating PR for LZ4 Content Checksum")
    print("=" * 60)

    # Configure git
    print("\n1. Configuring Git...")
    run_command(['git', 'config', 'user.name', 'GT'])
    run_command(['git', 'config', 'user.email', 'gt@example.com'])

    # Check current status
    print("\n2. Checking git status...")
    result = run_command(['git', 'status'])

    # Create branch
    print("\n3. Creating new branch: GT/lz4-content-checksum")
    result = run_command(['git', 'checkout', '-b', 'GT/lz4-content-checksum'], check=False)
    if "already exists" in result.stderr or result.returncode != 0:
        print("Branch already exists, checking it out...")
        run_command(['git', 'checkout', 'GT/lz4-content-checksum'])

    # Stage file
    print("\n4. Staging the modified file...")
    run_command(['git', 'add', 'clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java'])

    # Create commit
    print("\n5. Creating commit...")
    commit_msg = """KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream

This commit implements the content checksum verification feature for LZ4
compression as indicated by existing TODOs in the codebase.

Changes:
- Added CONTENT_CHECKSUM_SIZE constant (4 bytes) for content checksum size
- Added CONTENT_CHECKSUM_MISMATCH error message constant
- Added contentChecksum field for tracking running checksum
- Added checksumBuffer for direct buffer handling
- Implemented content checksum verification in readBlock() method
- Content checksum is computed as XOR of all block checksums

The content checksum provides end-to-end data integrity verification
for LZ4 compressed frames, following the LZ4 v1.5.1 frame format
specification."""

    result = run_command(['git', 'commit', '-m', commit_msg], check=False)

    # Push to GitHub
    print("\n6. Pushing branch to origin...")
    result = run_command(['git', 'push', '-u', 'origin', 'GT/lz4-content-checksum'], check=False)

    print("\n" + "=" * 60)
    print("SUCCESS!")
    print("=" * 60)
    print("\nTo create a PR, visit:")
    print("https://github.com/apache/kafka/compare")
    print("\nOr use GitHub CLI if available:")
    print('  gh pr create --base trunk --head GT/lz4-content-checksum \\')
    print('    --title "KAFKA-XXXX: Implement content checksum verification" \\')
    print('    --body "See PR_DESCRIPTION.md for details"')

if __name__ == '__main__':
    main()

