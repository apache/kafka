#!/bin/bash
# Script to create a PR for the LZ4 content checksum implementation

set -e

cd /Users/mymac/Desktop/kafka

echo "=========================================="
echo "Creating PR for LZ4 Content Checksum Implementation"
echo "=========================================="

# Check if git repo exists
if [ ! -d ".git" ]; then
    echo "Error: Not a git repository"
    exit 1
fi

# Configure git user if not set
if [ -z "$(git config user.name)" ]; then
    echo "Configuring git user..."
    git config user.name "GT"
    git config user.email "gt@example.com"
fi

echo ""
echo "1. Checking current git status..."
git status

echo ""
echo "2. Creating new branch: GT/lz4-content-checksum"
git checkout -b GT/lz4-content-checksum

echo ""
echo "3. Staging the modified file..."
git add clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java

echo ""
echo "4. Creating commit..."
git commit -m "$(cat <<'EOF'
KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream

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
specification.

Signed-off-by: $(git config user.name) <$(git config user.email)>
EOF
)"

echo ""
echo "5. Pushing branch to origin..."
git push -u origin GT/lz4-content-checksum

echo ""
echo "=========================================="
echo "Branch pushed successfully!"
echo "=========================================="
echo ""
echo "To create a PR, visit:"
echo "https://github.com/apache/kafka/compare"
echo ""
echo "Or use GitHub CLI (if installed):"
echo "  gh pr create --base trunk --head GT/lz4-content-checksum --title \"KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream\" --body-file - <<'EOF'"
echo "This PR implements the content checksum verification feature for LZ4 compression."
echo ""
echo "The content checksum provides end-to-end data integrity verification for LZ4 compressed frames."
echo ""
echo "## Changes"
echo "- Added CONTENT_CHECKSUM_SIZE constant (4 bytes)"
echo "- Added CONTENT_CHECKSUM_MISMATCH error message constant"
echo "- Added contentChecksum field for tracking running checksum"
echo "- Added checksumBuffer for direct buffer handling"
echo "- Implemented content checksum verification in readBlock() method"
echo ""
echo "## Testing"
echo "- Existing LZ4 compression tests should pass"
echo "- No regressions in backward compatibility"
echo "EOF"

