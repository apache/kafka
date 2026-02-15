# LZ4 Content Checksum Implementation PR Plan

## Goal
Implement content checksum verification in LZ4 compression as indicated by existing TODOs in the codebase.

## Files to Modify
1. `clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java`

## Implementation Steps

### Step 1: Implement content checksum verification in Lz4BlockInputStream ✅ DONE
- Added `CONTENT_CHECKSUM_SIZE = 4` constant
- Added `CONTENT_CHECKSUM_MISMATCH` error message constant
- Added `contentChecksum` field for tracking running checksum
- Added `checksumBuffer` for direct buffer handling
- Initialized `contentChecksum = 0` in constructor
- Implemented content checksum verification in `readBlock()` method
- Content checksum is computed as XOR of all block checksums

### Step 2: Run existing tests
- Run LZ4 compression tests to ensure backward compatibility
- Ensure no regressions in existing functionality

### Step 3: Create git branch and commit changes
- Branch: `GT/lz4-content-checksum`
- Push to remote and create PR

## Progress
- [x] Implement content checksum verification in Lz4BlockInputStream
- [ ] Run tests to verify implementation
- [ ] Create git branch and commit changes
- [ ] Push to remote and create PR

## PR Description
See `PR_DESCRIPTION.md` for full details.

## Commands to Run

```bash
# Configure git
git config user.name "GT"
git config user.email "gt@example.com"

# Create and checkout branch
git checkout -b GT/lz4-content-checksum

# Stage changes
git add clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java

# Commit
git commit -m "KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream"

# Push
git push -u origin GT/lz4-content-checksum

# Create PR at: https://github.com/apache/kafka/compare
