# KIP-1320 candidate-discovery result

This directory contains the completed candidate-discovery result for Kafka commit
`ac0d7e245b213c20ab66bcea211d1cbd5ed2c95a`.

## Result at a glance

```text
4,222 inventory classes
        |
        v
2,227 active candidates
        |
        v
4,454 GitHub queries (Java + Scala)
        |
        v
75 candidates with at least 1,000 matching import-text files
```

- Query set: `JVM_SOURCE_IMPORT_TEXT_V1`
- Query window: `2026-07-19T01:11:20Z` to `2026-07-21T01:03:13Z`
- Completed queries: 4,454
- Incomplete-result queries: 0

GitHub counts matching source files. These are not counts of unique users,
applications, or repositories.

## How the candidate artifacts are generated

```text
Kafka source at pinned SHA
          |
          v
Build 10 client-facing JARs
          |
          v
Bytecode + source scanner
          |
          +--> inventory rows: 4,222 classes
          |      - KIP-1265 Public/Private status
          |      - signature/supertype/config tags
          |      - source and artifact
          |
          v
Eligibility filter
  candidate_non_public = true
  already_deprecated_or_moved = false
          |
          +--> 2,227 classes
          |
          v
Generate 2 queries per class
  Java exact import + Scala import prefix
          |
          +--> query manifest: 4,454 queries
          |
          v
Existing local GitHub crawl snapshot
          |
          +--> results.tsv: 4,454 complete query rows
          |      - exact query
          |      - total_count
          |      - incomplete_results
          |      - timestamps and response hash
          |
          v
Validated join by class
  require exactly 1 Java + 1 Scala result
  combined = Java count + Scala count
          |
          v
Selection filter: combined >= 1,000
          |
          +--> selected-candidates.tsv
          |      75 candidate rows
          |
          +--> filter crawl rows by selected class
                 |
                 +--> selected-candidate-github-results.tsv
                        150 rows (75 x 2)
```

## Review the result

1. [`selected-candidates.tsv`](selected-candidates.tsv) contains the 75 review
   candidates, ordered by combined Java and Scala import-text count. It includes
   source location, programmatic evidence tags, and separate language counts.
2. [`selected-candidate-github-results.tsv`](selected-candidate-github-results.tsv)
   contains the two crawler records behind every selected candidate. It preserves
   the exact query, count, completeness flag, timestamps, request ID, and raw-
   response checksum.
3. [`selected-result-manifest.json`](selected-result-manifest.json) records the Kafka
   SHA, threshold, query-set version, time window, and source snapshot lineage.

Both TSV files are deterministic outputs of `github-usage/build-deliverables.sh`.
Verify the committed files and their cross-file invariants without network access:

```bash
python3 api-checker/kip-1320-inventory-poc/verify-committed-result.py \
  api-checker/kip-1320-inventory-poc/results/ac0d7e245b213c20ab66bcea211d1cbd5ed2c95a
```

The raw REST responses and unselected classes are not committed. They are large
crawler-resume data rather than the reviewer-facing result.
