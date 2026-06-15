# Service Health Checks

This repository contains dependency-free health-check scripts for Linux VMs that run Kafka, Kafka Connect, Java Kafka consumer applications, Flink, and continuous Flink jobs.

The main script is [scripts/healthcheck.py](scripts/healthcheck.py). It reads a JSON config and exits non-zero when a required service, process, connector, consumer group, or Flink job is unhealthy.

## Quick Start

1. Copy [config/healthcheck.sample.json](config/healthcheck.sample.json) to `config/healthcheck.json`.
2. Edit service names, Kafka paths, consumer groups, connector names, Flink URL, and expected Flink job names.
3. Run it as the Linux user that owns the user-level systemd services:

```bash
python3 scripts/healthcheck.py --config config/healthcheck.json
```

Or use the wrapper:

```bash
scripts/run-healthcheck.sh config/healthcheck.json
```

For automation logs:

```bash
python3 scripts/healthcheck.py --config config/healthcheck.json --json > healthcheck.json.out
```

## What It Checks

- `systemd --user` units are active for Kafka, Kafka Connect, and Java consumer applications.
- Optional process patterns exist with `pgrep -af`.
- Kafka broker bootstrap is reachable through `kafka-broker-api-versions.sh`.
- Required Kafka topics exist through `kafka-topics.sh`.
- Kafka Connect REST API is reachable, required connectors exist, and connector/tasks are in allowed states.
- Kafka consumer groups have parseable lag and stay under configured total or per-partition lag limits.
- Flink REST API is reachable, the cluster has the expected TaskManagers/slots, and expected continuous jobs are `RUNNING`.

## Important Notes

Run the script as the service owner when checking `systemd --user` units. If an automation tool uses `sudo`, make sure the target user has a valid user systemd session and `XDG_RUNTIME_DIR` is set, otherwise `systemctl --user` cannot see the services.

Kafka checks use Kafka CLI tools from `kafka.cli_dir` or `PATH`. For SASL/TLS clusters, set `kafka.command_config` to a client properties file, for example `/etc/kafka/client.properties`.

Flink checks use the Flink REST endpoint, usually `http://localhost:8081`. Continuous streaming jobs should normally be listed under `flink.expected_jobs` with `allowed_states: ["RUNNING"]`.

## Section Filtering

Run only one or more sections when troubleshooting:

```bash
python3 scripts/healthcheck.py --config config/healthcheck.json --only kafka,consumer
python3 scripts/healthcheck.py --config config/healthcheck.json --only flink
```

Valid sections are `systemd`, `process`, `kafka`, `kafka_connect`, `consumer`, and `flink`.

## Exit Codes

- `0`: all required checks passed, warnings allowed.
- `1`: configuration file missing or invalid.
- `2`: one or more checks failed.
- `3`: warning treated as failure with `--warn-as-fail`.

## Pre/Post Update Usage

Typical update flow:

```bash
python3 scripts/healthcheck.py --config config/healthcheck.json --json > pre-update-health.json

# stop services, patch OS, update service artifacts, restart services/jobs

python3 scripts/healthcheck.py --config config/healthcheck.json --json > post-update-health.json
python3 scripts/healthcheck.py --config config/healthcheck.json
```

Use the human-readable output for operators and the JSON output for CI/CD gates or change-management evidence.

## Container Image

The repository includes a basic [Dockerfile](Dockerfile) and a GitHub Actions workflow at [.github/workflows/container-image.yml](.github/workflows/container-image.yml). The image contains Python, Bash, `pgrep`, `systemctl`, and a headless Java runtime so host-mounted Kafka CLI scripts can run.

Build locally:

```bash
docker build -t servicehc:local .
docker run --rm servicehc:local --help
```

Run with a mounted config:

```bash
docker run --rm \
  -v "$PWD/config/healthcheck.json:/etc/servicehc/healthcheck.json:ro" \
  servicehc:local \
  --config /etc/servicehc/healthcheck.json
```

For checks that only use network endpoints, such as Kafka Connect REST and Flink REST, a normal container run is usually enough. For host process checks, user systemd checks, and Kafka CLI checks, run the container on the target VM with the needed host access:

```bash
docker run --rm \
  --pid=host \
  --user "$(id -u):$(id -g)" \
  -e XDG_RUNTIME_DIR="/run/user/$(id -u)" \
  -v "/run/user/$(id -u):/run/user/$(id -u)" \
  -v "$PWD/config/healthcheck.json:/etc/servicehc/healthcheck.json:ro" \
  -v "/opt/kafka:/opt/kafka:ro" \
  ghcr.io/OWNER/REPOSITORY:latest \
  --config /etc/servicehc/healthcheck.json
```

Replace `ghcr.io/OWNER/REPOSITORY:latest` with the package name published by GitHub Actions. Keep `kafka.cli_dir` in the config aligned with the mounted Kafka path, for example `/opt/kafka/bin`.

If `kafka.command_config` points to a client properties file, truststore, or keystore on the host, mount those files into the container too and keep the same paths in `healthcheck.json`.

The workflow publishes to GitHub Container Registry (`ghcr.io`) on pushes to `main`, `master`, and `v*` tags. Pull requests build the image but do not push it. The workflow uses the repository `GITHUB_TOKEN`, so no separate registry secret is needed for publishing to the same repository package.

`config/healthcheck.json` is intentionally excluded by [.dockerignore](.dockerignore), because real configs often contain internal hostnames, topic names, or client properties paths.
