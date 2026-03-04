# Remote Log Metadata Topic Scenarios

This document describes detailed metadata changes for both `__remote_log_metadata` (audit/retention-based) and `__remote_log_metadata_compacted` (compacted) topics across various operational scenarios.

## RemoteLogMetadataMessage API Keys

The serialized messages use these API keys:

| API Key | Message Type | Description |
|---------|-------------|-------------|
| 0 | REMOTE_LOG_SEGMENT_METADATA | Initial segment upload (COPY_SEGMENT_STARTED) |
| 1 | REMOTE_LOG_SEGMENT_METADATA_UPDATE | State transitions (FINISHED, DELETE_*) |
| 2 | REMOTE_PARTITION_DELETE_METADATA | Partition deletion events |

## RemoteLogSegmentState Enum Values

| Value | State | Description |
|-------|-------|-------------|
| 0 | COPY_SEGMENT_STARTED | Upload in progress |
| 1 | (unused) | - |
| 2 | COPY_SEGMENT_FINISHED | Upload completed successfully |
| 3 | DELETE_SEGMENT_STARTED | Deletion in progress |
| 4 | DELETE_SEGMENT_FINISHED | Deletion completed |

## Key Format

All metadata keys follow the format:
```
topicId:partition:endOffset:brokerLeaderEpoch
```

For example: `a1b2c3d4-...:5:1000:3` means:
- Topic ID: a1b2c3d4-...
- Partition: 5
- End offset: 1000
- Broker leader epoch: 3

## Scenario 1: Normal Segment Upload

**Context**: Topic `orders` (ID: `abc123`), Partition 0, Broker 101 at epoch 3

### Timeline & Messages

| Time | Event | Key | Value |
|------|-------|-----|-------|
| T1 | Start upload | `abc123:0:1000:3` | apiKey=0, uuid=UUID-A, state=0 (STARTED) |
| T2 | Upload completes | `abc123:0:1000:3` | apiKey=1, uuid=UUID-A, state=2 (FINISHED) |

### After Compaction

**Audit topic**: Retains both messages (T1 STARTED + T2 FINISHED)

**Compacted topic**: Retains only latest → `abc123:0:1000:3` = FINISHED (T1 compacted away)

**Cache result**: 1 segment with uuid=UUID-A, endOffset=1000, state=FINISHED

---

## Scenario 2: Leadership Change During Upload

**Context**: Broker 101 (epoch 3) starts upload, leadership changes to Broker 102 (epoch 4), both complete

### Timeline & Messages

| Time | Event | Key | Value |
|------|-------|-----|-------|
| T1 | Broker 101 starts | `abc123:0:2000:3` | apiKey=0, uuid=UUID-A, state=0, epoch=3 |
| T2 | Leadership → 102 | - | - |
| T3 | Broker 102 starts | `abc123:0:2000:4` | apiKey=0, uuid=UUID-B, state=0, epoch=4 ← Different key! |
| T4 | Broker 101 finishes | `abc123:0:2000:3` | apiKey=1, uuid=UUID-A, state=2, epoch=3 |
| T5 | Broker 102 finishes | `abc123:0:2000:4` | apiKey=1, uuid=UUID-B, state=2, epoch=4 |

### After Compaction

**Audit topic**: Retains all 4 messages

**Compacted topic**: 2 keys, each with latest state
- `abc123:0:2000:3` → FINISHED (UUID-A, orphaned)
- `abc123:0:2000:4` → FINISHED (UUID-B, active)

**Cache result**: Returns both segments, selects UUID-B (epoch 4 > 3) for reads

---

## Scenario 3: Failed Upload (Retry Same Epoch)

**Context**: Broker 101 (epoch 5) tries upload, fails, retries with new UUID

### Timeline & Messages

| Time | Event | Key | Value |
|------|-------|-----|-------|
| T1 | First attempt | `abc123:0:3000:5` | apiKey=0, uuid=UUID-A, state=0 |
| T2 | Network timeout | - | (no message) |
| T3 | Retry | `abc123:0:3000:5` | apiKey=0, uuid=UUID-B, state=0 ← Same key, different UUID! |
| T4 | Success | `abc123:0:3000:5` | apiKey=1, uuid=UUID-B, state=2 |

### After Compaction

**Audit topic**: Retains all 3 messages (T1 failed, T3 retry, T4 success)

**Compacted topic**: Retains only latest → `abc123:0:3000:5` = FINISHED (UUID-B)

**Cache result**: Only UUID-B appears (UUID-A compacted away)

**Key insight**: Same epoch = same key, but new UUID for each attempt

---

## Scenario 4: Segment Deletion (Retention)

**Context**: Segment at endOffset=1000 has 3 upload keys (epochs 3,4,5), now being deleted by current leader (epoch 7)

### Initial State
```
abc123:0:1000:3 → FINISHED (UUID-A, orphaned)
abc123:0:1000:4 → FINISHED (UUID-B, orphaned)
abc123:0:1000:5 → FINISHED (UUID-C, active)
```

### Timeline & Messages

| Time | Event | Key | Value | Topic |
|------|-------|-----|-------|-------|
| T1 | Retention triggers | - | (identify segment) | - |
| T2 | Delete from S3 | - | (physical deletion) | - |
| T3 | Mark deletion | `abc123:0:1000:7` | apiKey=1, state=3 (DELETE_STARTED) | Both |
| T4 | Deletion finished | `abc123:0:1000:7` | apiKey=1, state=4 (DELETE_FINISHED) | Both |
| T5 | Tombstone epoch 3 | `abc123:0:1000:3` | null | Compacted only |
| T6 | Tombstone epoch 4 | `abc123:0:1000:4` | null | Compacted only |
| T7 | Tombstone epoch 5 | `abc123:0:1000:5` | null | Compacted only |
| T8 | Tombstone epoch 7 | `abc123:0:1000:7` | null | Compacted only |

### Tombstone Details (Compacted Topic ONLY)

After DELETE_FINISHED (T4), tombstones written for ALL 4 keys:
```
abc123:0:1000:3 → null  (epoch 3, UUID-A)
abc123:0:1000:4 → null  (epoch 4, UUID-B)
abc123:0:1000:5 → null  (epoch 5, UUID-C)
abc123:0:1000:7 → null  (epoch 7, deletion marker)
```

### After Compaction

**Audit topic**: Retains all messages (STARTED, FINISHED, DELETE_STARTED, DELETE_FINISHED)

**Compacted topic**: All 4 keys removed (segment completely forgotten)

**Cache result**: Empty (segment no longer exists)

**Why tombstone all 4 keys?**
- Remove all historical metadata (successful + orphaned uploads + deletion marker)
- Ensures compacted topic completely "forgets" this segment

---

## Scenario 5: Partition Deletion

**Context**: Entire partition deleted, cleanup all segments

### Timeline & Messages

| Time | Event | Key | Value |
|------|-------|-----|-------|
| T1 | Start partition delete | `abc123:0:<marker>:7` | apiKey=2, state=DELETE_PARTITION_STARTED |
| T2 | Partition delete done | `abc123:0:<marker>:7` | apiKey=2, state=DELETE_PARTITION_FINISHED |

### Cleanup Process

**Compacted topic**: All segment keys for `abc123:0:*:*` get tombstones, partition delete marker remains

---

## Summary Comparison

| Aspect | Audit Topic | Compacted Topic |
|--------|-------------|-----------------|
| **Purpose** | Complete audit trail | Current state only |
| **Retention** | Time-based (e.g., 7 days) | Compaction-based (indefinite) |
| **Failed uploads** | Preserved forever | Overwritten by retry |
| **Leadership changes** | Multiple attempts visible | Latest per epoch visible |
| **After deletion** | DELETE_FINISHED preserved | Tombstones remove all traces |
| **Query pattern** | Historical analysis | Fast current-state lookups |
| **Cache usage** | Not used | Primary source for cache |
| **Disk usage** | Grows unbounded | Bounded by active segments |

## Migration Considerations

When migrating from audit to compacted topic:

1. **COPY_SEGMENT_STARTED**: Don't migrate (incomplete, will be overwritten or retried)
2. **COPY_SEGMENT_FINISHED**: Migrate with highest broker leader epoch as key
3. **DELETE_SEGMENT_FINISHED**: Don't migrate (segment already gone)
4. **Multiple epochs for same endOffset**: Migrate all, cache will select highest epoch
5. **Key reconstruction**: Use highest segment leader epoch as `brokerLeaderEpoch` in key

This ensures the compacted topic represents the current state accurately after migration.
