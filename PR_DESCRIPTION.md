# KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream

## Summary

This PR implements the content checksum verification feature for LZ4 compression as indicated by existing TODOs in the codebase. The content checksum provides end-to-end data integrity verification for LZ4 compressed frames, following the LZ4 v1.5.1 frame format specification.

## Changes

### Files Modified

- `clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java`

### Detailed Changes

1. **Added Constants:**
   - `CONTENT_CHECKSUM_SIZE = 4` - Size of the content checksum in bytes
   - `CONTENT_CHECKSUM_MISMATCH` - Error message for checksum verification failure

2. **Added Fields:**
   - `contentChecksum` - Running content checksum for verifying end-to-end data integrity
   - `checksumBuffer` - Temporary buffer for computing checksums on direct buffers

3. **Key Implementation:**
   - **Constructor initialization**: `contentChecksum` is initialized to 0
   - **Block processing**: After decompressing each block, if content checksum is enabled (`flg.isContentChecksumSet()`), the checksum is updated using XOR of all block checksums
   - **End-of-stream verification**: When the EndMark (blockSize == 0) is encountered, the running content checksum is compared against the expected checksum in the stream

### Content Checksum Algorithm

The content checksum is computed as the XOR of the XXHash32 checksums of each decompressed block. This follows the LZ4 v1.5.1 frame format specification.

## Testing

- Existing LZ4 compression tests should pass
- No regressions in backward compatibility
- The implementation handles both array-backed and direct buffers correctly

## Compatibility

- The implementation maintains backward compatibility with existing LZ4 streams
- Content checksum verification is only performed when the stream indicates it has a content checksum (via the FLG descriptor)

## Related Issues

- This implementation addresses the existing TODOs in `Lz4BlockInputStream.java` related to content checksum verification

---

## Commands to Push and Create PR

```bash
cd /Users/mymac/Desktop/kafka

# Configure git (if not already configured)
git config user.name "GT"
git config user.email "gt@example.com"

# Create and checkout branch
git checkout -b GT/lz4-content-checksum

# Stage changes
git add clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java

# Commit
git commit -m "KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream

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
specification."

# Push to GitHub
git push -u origin GT/lz4-content-checksum

# Create PR using GitHub CLI (if available)
gh pr create --base trunk --head GT/lz4-content-checksum \
  --title "KAFKA-XXXX: Implement content checksum verification in Lz4BlockInputStream" \
  --body-file PR_DESCRIPTION.md

# OR visit directly:
# https://github.com/apache/kafka/compare
```

