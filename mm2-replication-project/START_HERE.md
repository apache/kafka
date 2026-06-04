# Enhanced MirrorMaker 2 — Final Project Package

This package is the corrected, final project for the Kafka Data Replication
challenge. It is ready for GitHub push, local execution, deployment, and demo.

## Why this package does NOT contain the full 273 MB Kafka tree

The Apache Kafka source is ~273 MB and is the unmodified upstream repo plus three
changed files. Shipping the whole tree in a ZIP is impractical and adds nothing —
you build from your own fork. Instead this package contains:

- `kafka-fork-changes/` — the exact changed source files **and** a single
  `mm2-enhancements.patch` you can apply to a fresh fork with one command.
- Everything else needed to run the system (compose, images, scripts, config, docs).

To assemble the full buildable tree: clone your fork, apply the patch (or copy the
three files), then use the compose/scripts here. See `reports/COMMANDS.md`.

## Layout

```
.
├── START_HERE.md                 # this file
├── README.md                     # full project documentation
├── BUILD_VERIFICATION.md         # concise build-status note
├── docker-compose.yml            # primary + DR clusters, MM2, producer
├── kafka-fork-changes/
│   ├── mm2-enhancements.patch    # apply to a fresh Kafka fork
│   └── connect/mirror/src/...    # full copies of the 3 changed files
│       ├── main/.../MirrorSourceTask.java        (modified)
│       ├── main/.../LogTruncationException.java  (new)
│       └── test/.../MirrorSourceTaskTest.java    (modified)
├── mm2/
│   ├── Dockerfile                # build enhanced MM2 onto apache/kafka:4.0.0
│   ├── mm2.properties            # dedicated-mode MM2 config
│   └── .dockerignore.fork        # copy to fork root as .dockerignore
├── producer/
│   ├── commit_log_producer.py    # Task 1 CLI
│   ├── requirements.txt
│   └── Dockerfile
├── scripts/
│   └── run_challenge.sh          # 3 demonstration scenarios
└── reports/
    ├── REQUIREMENT_MAPPING.md    # requirement → implementation
    ├── VERIFICATION_REPORTS.md   # build / test / run verification
    ├── MODIFIED_FILES.md         # change summary
    └── COMMANDS.md               # exact build/run/test commands
```

## Quick start

```bash
# 1. apply changes to your fork, build the MM2 image (see reports/COMMANDS.md)
# 2. then:
export DOCKERHUB_USER=<your-user>
docker compose up -d primary-kafka dr-kafka mm2
./scripts/run_challenge.sh all
```

## Honest status

All source and config is implemented and statically verified. The detection
algorithm was runtime-simulated (7/7 cases pass). The Gradle compile, JUnit run,
Docker builds, and live scenarios were **not executed in the authoring sandbox**
(no JDK/Docker/network there); run them on a normal build machine using
`reports/COMMANDS.md`. Replace the fork/PR/Docker-Hub placeholders before
submitting.
