# KIP-1320 API inventory proof of concept

This local research tool creates a reproducible inventory of externally visible Kafka
classes that are not effectively Public under the KIP-1265 bytecode model. It records
observable characteristics such as:

- exposure through an effectively Public signature;
- an effectively Public Kafka supertype;
- a reference from Kafka configuration or service metadata;
- presence in Public Javadoc without an effectively Public annotation;
- deprecation or placement under an `internal` or `internals` package.

These characteristics are evidence for discussion. They do not automatically decide
whether a class is Public, internal, deprecated, or eligible for a compatibility shim.

## Scope

The current proof of concept scans ten client-facing artifacts:

- `kafka-clients`
- `kafka-streams`
- `kafka-streams-test-utils`
- `connect-api`
- `connect-transforms`
- `connect-json`
- `connect-file`
- `connect-mirror-client`
- `connect-mirror`
- `connect-basic-auth-extension`

This is comprehensive only within that declared artifact scope. It is not an inventory
of every Kafka Gradle module.

## Build the bytecode inventory

Run this command from any directory:

```bash
./api-checker/kip-1320-inventory-poc/run.sh
```

The script uses the current worktree HEAD and refuses tracked local changes by default.
Set `ALLOW_DIRTY_WORKTREE=1` only for local development of the scanner itself.

The generated files under `output/` include:

- `inventory.tsv`: all externally visible classes and their bytecode evidence;
- `query-manifest.tsv`: versioned Java and Scala GitHub exact-import queries per
  active candidate;
- `bytecode-summary.md`: counts of the observed characteristics;
- `input-manifest.tsv`: the Kafka SHA and checksums of the scanner, dependency, and JARs;
- `crawl-inputs.env`: checksums consumed by the GitHub usage crawler.

To confirm deterministic local output:

```bash
./api-checker/kip-1320-inventory-poc/verify-reproducibility.sh
```

## Add external Java and Scala source usage

The crawler executes two versioned queries for every active candidate:

```text
"import {FQCN};" language:Java NOT repo:apache/kafka NOT is:fork
"import {FQCN}" language:Scala NOT repo:apache/kafka NOT is:fork
```

Configuration-only references are not counted. For example, setting
`key.converter=org.apache.kafka.connect.json.JsonConverter` is a supported plugin
configuration and is not an accidental Java source dependency.

Run one query as a smoke test:

```bash
MAX_QUERIES=1 \
  ./api-checker/kip-1320-inventory-poc/github-usage/crawl.sh
```

Then run the complete manifest and build the discussion shortlist:

```bash
./api-checker/kip-1320-inventory-poc/github-usage/crawl.sh
USAGE_THRESHOLD=1000 \
  ./api-checker/kip-1320-inventory-poc/github-usage/build-deliverables.sh
```

The discussion threshold is the sum of the Java and Scala import-text file counts.
The two language counts and their evidence remain separate in the generated files.
The threshold reduces the manual discussion set. It is not a compatibility or
deprecation policy. GitHub's count is a matching-file count, not a count of unique
users or repositories.

The Scala V1 query covers a direct import such as `import
org.apache.kafka.common.utils.Utils`, but it is a text-prefix query. For example, the
outer-class query can also match `import org.example.Outer.Nested`. It does not cover
grouped imports, renamed imports, wildcard imports, or direct fully qualified
references. The Java query has similar blind spots for static imports, wildcard
imports, and direct fully qualified references. These queries are reproducible usage
signals, not a complete census of JVM-language usage.
The persisted query-version name `SCALA_EXACT_IMPORT_V1` predates this clarification;
the query text and its prefix semantics remain unchanged so existing evidence can be
reused without silently redefining the query.

The query asks GitHub to exclude forks with `NOT is:fork`. Copied or mirrored source
can still remain, and GitHub index contents can change over time. The first-page
repository examples cover at most 100 returned files for each language query.
The reported repository count is the unique union across those two first pages, so it
can exceed 100 while still covering no more than 100 files per language query.

GitHub currently reports that the REST code-search endpoint used by this proof of
concept is deprecated on `2026-03-27` and scheduled to sunset on `2026-09-27`. Every
saved response records these lifecycle headers so a future query-version change is
explicit.

## Reuse a completed snapshot

Crawler output defaults to a Kafka-SHA-keyed directory under
`github-usage/snapshots/`. `REUSE_OUTPUT_DIRS` accepts colon-separated completed
snapshot directories. A record is reused only when its query kind, class, query
version, and full query text match. The source snapshot manifest and evidence hashes
are validated, and the regenerated metadata records the source snapshot checksum,
Kafka SHA, record index, and metadata checksum.
If multiple snapshots contain the same query identity, reuse selection is independent
of CLI directory order: evidence from the target Kafka SHA is preferred, followed by
the earliest completion timestamp and then the source manifest checksum.
Already-valid target records are preserved before reuse is considered, so a newer
fresh response is never replaced by older reusable evidence. An existing target
snapshot whose Kafka, query, inventory, scanner, or API lineage differs from the
requested run is rejected instead of being overwritten.

For example, a completed Java-only snapshot can be upgraded to Java plus Scala
without repeating its Java requests:

```bash
./api-checker/kip-1320-inventory-poc/github-usage/prepare-combined-from-java-snapshot.sh \
  /path/to/java-only-poc

INPUTS_ENV=/path/to/snapshots/<kafka-sha>/inputs/crawl-inputs.env \
QUERY_MANIFEST=/path/to/snapshots/<kafka-sha>/inputs/query-manifest.tsv \
INVENTORY=/path/to/snapshots/<kafka-sha>/inputs/inventory.tsv \
REUSE_OUTPUT_DIRS=/path/to/java-evidence:/path/to/scala-evidence \
./api-checker/kip-1320-inventory-poc/github-usage/crawl.sh

INPUTS_ENV=/path/to/snapshots/<kafka-sha>/inputs/crawl-inputs.env \
QUERY_MANIFEST=/path/to/snapshots/<kafka-sha>/inputs/query-manifest.tsv \
INVENTORY=/path/to/snapshots/<kafka-sha>/inputs/inventory.tsv \
CRAWL_OUTPUT=/path/to/snapshots/<kafka-sha>/evidence \
OUTPUT_DIR=/path/to/snapshots/<kafka-sha>/deliverables \
USAGE_THRESHOLD=1000 \
./api-checker/kip-1320-inventory-poc/github-usage/build-deliverables.sh
```

Only queries absent from the reusable snapshots call GitHub. This keeps the combined
snapshot tied to the pre-decision Kafka inventory while avoiding a full external
usage rerun. Resume validation is performed in one local pass; a completed snapshot
therefore skips all GitHub requests without repeating thousands of shell-level
checksum commands or requiring GitHub authentication. Authentication is loaded only
if at least one query is actually missing.

## Inspect the committed result

The completed result for Kafka SHA
`ac0d7e245b213c20ab66bcea211d1cbd5ed2c95a` is committed under
[`results/ac0d7e245b213c20ab66bcea211d1cbd5ed2c95a`](results/ac0d7e245b213c20ab66bcea211d1cbd5ed2c95a/).
It contains the 75 selected candidates, their 150 Java and Scala GitHub query
results, snapshot provenance, and checksums. Reviewers can inspect the result
without repeating 4,454 GitHub queries.

Raw GitHub REST response bodies, response headers, and per-query metadata remain
ignored by Git. They form a large resumable crawler cache rather than a reviewer-
completion status, timestamp, request ID, and raw-response checksum for every
selected candidate.
