#!/usr/bin/env python3
"""Dependency-free health checks for Kafka, Kafka Connect, consumers, and Flink."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


OK = "OK"
WARN = "WARN"
FAIL = "FAIL"
SKIP = "SKIP"


@dataclass
class CheckResult:
    category: str
    name: str
    status: str
    detail: str
    elapsed_ms: int = 0


@dataclass
class CommandResult:
    args: Sequence[str]
    returncode: int
    stdout: str
    stderr: str
    elapsed_ms: int


def result(category: str, name: str, status: str, detail: str, elapsed_ms: int = 0) -> CheckResult:
    return CheckResult(category=category, name=name, status=status, detail=detail, elapsed_ms=elapsed_ms)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def enabled(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("enabled", True))


def elapsed(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def run_cmd(args: Sequence[str], timeout: int) -> CommandResult:
    start = time.monotonic()
    try:
        proc = subprocess.run(
            list(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
        return CommandResult(args=args, returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr, elapsed_ms=elapsed(start))
    except FileNotFoundError as exc:
        return CommandResult(args=args, returncode=127, stdout="", stderr=str(exc), elapsed_ms=elapsed(start))
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        return CommandResult(args=args, returncode=124, stdout=stdout, stderr=stderr or "command timed out", elapsed_ms=elapsed(start))


def clean_output(text: str, max_len: int = 240) -> str:
    compact = " ".join((text or "").strip().split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


def command_error(cmd: CommandResult) -> str:
    message = clean_output(cmd.stderr) or clean_output(cmd.stdout) or "no command output"
    return "exit {}: {}".format(cmd.returncode, message)


def find_executable(name: str, cli_dir: Optional[str] = None) -> Optional[str]:
    candidates: List[str] = []
    if cli_dir:
        candidates.append(os.path.join(cli_dir, name))
    found = shutil.which(name)
    if found:
        candidates.append(found)
    for candidate in candidates:
        if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def kafka_command_args(cfg: Dict[str, Any], command: str, tail: Sequence[str]) -> Optional[List[str]]:
    exe = find_executable(command, cfg.get("cli_dir"))
    if not exe:
        return None
    args = [exe]
    if cfg.get("bootstrap_servers"):
        args.extend(["--bootstrap-server", cfg["bootstrap_servers"]])
    if cfg.get("command_config"):
        args.extend(["--command-config", cfg["command_config"]])
    args.extend(tail)
    return args


def http_json(url: str, timeout: int) -> Tuple[Any, int]:
    start = time.monotonic()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    data = json.loads(raw) if raw else {}
    return data, elapsed(start)


def join_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def check_systemd_user_services(services: Iterable[Any], timeout: int) -> List[CheckResult]:
    results: List[CheckResult] = []
    for item in services:
        cfg = item if isinstance(item, dict) else {"name": str(item)}
        name = cfg.get("name")
        label = cfg.get("label", name)
        if not name:
            results.append(result("systemd", "unnamed service", FAIL, "missing systemd unit name"))
            continue

        cmd = run_cmd(["systemctl", "--user", "is-active", name], timeout)
        state = clean_output(cmd.stdout) or clean_output(cmd.stderr)
        if cmd.returncode == 0 and state == "active":
            results.append(result("systemd", label, OK, "{} is active".format(name), cmd.elapsed_ms))
        else:
            detail = "{} is not active ({})".format(name, state or command_error(cmd))
            results.append(result("systemd", label, FAIL, detail, cmd.elapsed_ms))

        pattern = cfg.get("process_pattern")
        if pattern:
            results.extend(check_processes([{"label": "{} process".format(label), "pattern": pattern, "min_count": cfg.get("min_process_count", 1)}], timeout))
    return results


def check_processes(processes: Iterable[Any], timeout: int) -> List[CheckResult]:
    results: List[CheckResult] = []
    for item in processes:
        cfg = item if isinstance(item, dict) else {"pattern": str(item)}
        pattern = cfg.get("pattern")
        label = cfg.get("label", pattern)
        min_count = int(cfg.get("min_count", 1))
        if not pattern:
            results.append(result("process", "unnamed process", FAIL, "missing process pattern"))
            continue

        cmd = run_cmd(["pgrep", "-af", pattern], timeout)
        matches = [line for line in cmd.stdout.splitlines() if line.strip()]
        if cmd.returncode == 0 and len(matches) >= min_count:
            detail = "found {} process(es), expected at least {}".format(len(matches), min_count)
            results.append(result("process", label, OK, detail, cmd.elapsed_ms))
        elif cmd.returncode == 1:
            detail = "found 0 process(es), expected at least {}".format(min_count)
            results.append(result("process", label, FAIL, detail, cmd.elapsed_ms))
        else:
            results.append(result("process", label, FAIL, command_error(cmd), cmd.elapsed_ms))
    return results


def parse_broker_count(text: str) -> int:
    broker_ids = set()
    for line in text.splitlines():
        match = re.search(r"\(id:\s*(-?\d+)", line)
        if match:
            broker_ids.add(match.group(1))
    return len(broker_ids)


def check_kafka(kafka_cfg: Dict[str, Any], timeout: int) -> List[CheckResult]:
    if not kafka_cfg or not enabled(kafka_cfg):
        return []

    results: List[CheckResult] = []
    bootstrap = kafka_cfg.get("bootstrap_servers")
    if not bootstrap:
        return [result("kafka", "broker", FAIL, "missing kafka.bootstrap_servers")]

    broker_args = kafka_command_args(kafka_cfg, "kafka-broker-api-versions.sh", [])
    if not broker_args:
        results.append(result("kafka", "broker", FAIL, "kafka-broker-api-versions.sh was not found; set kafka.cli_dir or PATH"))
    else:
        cmd = run_cmd(broker_args, timeout)
        if cmd.returncode == 0:
            broker_count = parse_broker_count(cmd.stdout)
            min_brokers = int(kafka_cfg.get("min_broker_count", 1))
            if broker_count >= min_brokers:
                detail = "bootstrap {} reachable; found {} broker(s)".format(bootstrap, broker_count)
                results.append(result("kafka", "broker", OK, detail, cmd.elapsed_ms))
            else:
                detail = "bootstrap {} reachable but found {} broker(s), expected at least {}".format(bootstrap, broker_count, min_brokers)
                results.append(result("kafka", "broker", FAIL, detail, cmd.elapsed_ms))
        else:
            results.append(result("kafka", "broker", FAIL, command_error(cmd), cmd.elapsed_ms))

    required_topics = [str(topic) for topic in as_list(kafka_cfg.get("required_topics"))]
    if required_topics:
        topic_args = kafka_command_args(kafka_cfg, "kafka-topics.sh", ["--list"])
        if not topic_args:
            results.append(result("kafka", "topics", FAIL, "kafka-topics.sh was not found; set kafka.cli_dir or PATH"))
        else:
            cmd = run_cmd(topic_args, timeout)
            if cmd.returncode == 0:
                topics = set(line.strip() for line in cmd.stdout.splitlines() if line.strip())
                missing = sorted(set(required_topics) - topics)
                if missing:
                    results.append(result("kafka", "topics", FAIL, "missing topic(s): {}".format(", ".join(missing)), cmd.elapsed_ms))
                else:
                    results.append(result("kafka", "topics", OK, "all required topics are present", cmd.elapsed_ms))
            else:
                results.append(result("kafka", "topics", FAIL, command_error(cmd), cmd.elapsed_ms))

    return results


def connector_status_detail(status_doc: Dict[str, Any], allowed_states: Sequence[str]) -> Tuple[bool, str]:
    allowed = set(allowed_states)
    connector = status_doc.get("connector", {})
    connector_state = connector.get("state", "UNKNOWN")
    bad_parts = []
    if connector_state not in allowed:
        bad_parts.append("connector={}".format(connector_state))

    tasks = status_doc.get("tasks", [])
    for task in tasks:
        task_state = task.get("state", "UNKNOWN")
        if task_state not in allowed:
            bad_parts.append("task {}={}".format(task.get("id", "?"), task_state))

    if bad_parts:
        return False, "; ".join(bad_parts)
    return True, "connector and {} task(s) in {}".format(len(tasks), "/".join(allowed_states))


def check_kafka_connect(connect_cfg: Dict[str, Any], timeout: int) -> List[CheckResult]:
    if not connect_cfg or not enabled(connect_cfg):
        return []

    results: List[CheckResult] = []
    base_url = connect_cfg.get("url")
    if not base_url:
        return [result("kafka_connect", "cluster", FAIL, "missing kafka_connect.url")]

    try:
        root_doc, ms = http_json(join_url(base_url, "/"), timeout)
        version = root_doc.get("version", "unknown")
        results.append(result("kafka_connect", "cluster", OK, "REST API reachable; version {}".format(version), ms))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return [result("kafka_connect", "cluster", FAIL, "REST API is not reachable: {}".format(exc))]

    try:
        connectors, ms = http_json(join_url(base_url, "/connectors"), timeout)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return results + [result("kafka_connect", "connectors", FAIL, "could not list connectors: {}".format(exc))]

    if not isinstance(connectors, list):
        return results + [result("kafka_connect", "connectors", FAIL, "unexpected /connectors response")]

    min_count = int(connect_cfg.get("min_connector_count", 0))
    if len(connectors) >= min_count:
        results.append(result("kafka_connect", "connectors", OK, "found {} connector(s)".format(len(connectors)), ms))
    else:
        detail = "found {} connector(s), expected at least {}".format(len(connectors), min_count)
        results.append(result("kafka_connect", "connectors", FAIL, detail, ms))

    required = [str(name) for name in as_list(connect_cfg.get("required_connectors"))]
    missing = sorted(set(required) - set(connectors))
    if missing:
        results.append(result("kafka_connect", "required connectors", FAIL, "missing connector(s): {}".format(", ".join(missing))))

    if connect_cfg.get("require_all_connectors_running", True):
        names_to_check = connectors
    else:
        names_to_check = required

    allowed_states = [str(state) for state in as_list(connect_cfg.get("allowed_states") or ["RUNNING"])]
    for name in names_to_check:
        quoted = urllib.parse.quote(str(name), safe="")
        try:
            status_doc, ms = http_json(join_url(base_url, "/connectors/{}/status".format(quoted)), timeout)
            healthy, detail = connector_status_detail(status_doc, allowed_states)
            status = OK if healthy else FAIL
            results.append(result("kafka_connect", str(name), status, detail, ms))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            results.append(result("kafka_connect", str(name), FAIL, "could not read connector status: {}".format(exc)))

    return results


def parse_consumer_lag(text: str, group: str) -> Tuple[int, int, int, bool]:
    total_lag = 0
    max_lag = 0
    partitions = 0
    unknown = False

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("GROUP") or stripped.startswith("Consumer group"):
            continue
        parts = stripped.split()
        if len(parts) < 6 or parts[0] != group:
            continue
        lag_token = parts[5]
        if lag_token.isdigit():
            lag = int(lag_token)
            total_lag += lag
            max_lag = max(max_lag, lag)
            partitions += 1
        else:
            unknown = True
    return total_lag, max_lag, partitions, unknown


def consumer_lag_result(label: str, cfg: Dict[str, Any], defaults: Dict[str, Any], timeout: int) -> CheckResult:
    group = cfg.get("consumer_group")
    if not group:
        return result("consumer", "{} lag".format(label), SKIP, "no consumer_group configured")

    kafka_cfg = dict(defaults)
    kafka_cfg.update(
        {
            "bootstrap_servers": cfg.get("bootstrap_servers", defaults.get("bootstrap_servers")),
            "cli_dir": cfg.get("kafka_cli_dir", defaults.get("cli_dir")),
            "command_config": cfg.get("command_config", defaults.get("command_config")),
        }
    )
    if not kafka_cfg.get("bootstrap_servers"):
        return result("consumer", "{} lag".format(label), FAIL, "missing bootstrap_servers for consumer group {}".format(group))

    args = kafka_command_args(kafka_cfg, "kafka-consumer-groups.sh", ["--describe", "--group", str(group)])
    if not args:
        return result("consumer", "{} lag".format(label), FAIL, "kafka-consumer-groups.sh was not found; set kafka.cli_dir or PATH")

    cmd = run_cmd(args, timeout)
    if cmd.returncode != 0:
        return result("consumer", "{} lag".format(label), FAIL, command_error(cmd), cmd.elapsed_ms)

    stdout = cmd.stdout
    if "does not exist" in stdout or "not found" in stdout:
        return result("consumer", "{} lag".format(label), FAIL, "consumer group {} does not exist".format(group), cmd.elapsed_ms)

    total_lag, max_lag, partitions, unknown = parse_consumer_lag(stdout, str(group))
    if partitions == 0:
        return result("consumer", "{} lag".format(label), FAIL, "no partition lag rows found for group {}".format(group), cmd.elapsed_ms)
    if unknown:
        return result("consumer", "{} lag".format(label), FAIL, "lag contained unknown values for group {}".format(group), cmd.elapsed_ms)

    no_active_members = "has no active members" in stdout
    if no_active_members and cfg.get("require_active_members", False):
        return result("consumer", "{} lag".format(label), FAIL, "consumer group {} has no active members".format(group), cmd.elapsed_ms)

    max_total = cfg.get("max_total_lag")
    max_partition = cfg.get("max_partition_lag")
    failures = []
    if max_total is not None and total_lag > int(max_total):
        failures.append("total lag {} > {}".format(total_lag, max_total))
    if max_partition is not None and max_lag > int(max_partition):
        failures.append("max partition lag {} > {}".format(max_lag, max_partition))
    if failures:
        return result("consumer", "{} lag".format(label), FAIL, "; ".join(failures), cmd.elapsed_ms)

    detail = "group {}; partitions {}; total lag {}; max partition lag {}".format(group, partitions, total_lag, max_lag)
    if no_active_members:
        return result("consumer", "{} lag".format(label), WARN, detail + "; no active members", cmd.elapsed_ms)
    return result("consumer", "{} lag".format(label), OK, detail, cmd.elapsed_ms)


def check_consumers(consumers: Iterable[Dict[str, Any]], kafka_defaults: Dict[str, Any], timeout: int) -> List[CheckResult]:
    results: List[CheckResult] = []
    for cfg in consumers:
        if not enabled(cfg):
            continue
        label = cfg.get("label") or cfg.get("consumer_group") or cfg.get("systemd_service") or "consumer"

        if cfg.get("systemd_service"):
            results.extend(check_systemd_user_services([{"label": "{} service".format(label), "name": cfg["systemd_service"]}], timeout))
        if cfg.get("process_pattern"):
            process_cfg = {"label": "{} process".format(label), "pattern": cfg["process_pattern"], "min_count": cfg.get("min_process_count", 1)}
            results.extend(check_processes([process_cfg], timeout))
        if cfg.get("consumer_group"):
            results.append(consumer_lag_result(label, cfg, kafka_defaults, timeout))
    return results


def job_matches(job: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    if expected.get("jid") and job.get("jid") == expected["jid"]:
        return True
    if expected.get("id") and job.get("jid") == expected["id"]:
        return True
    if expected.get("name") and job.get("name") == expected["name"]:
        return True
    if expected.get("name_regex") and re.search(str(expected["name_regex"]), str(job.get("name", ""))):
        return True
    return False


def check_flink(flink_cfg: Dict[str, Any], timeout: int) -> List[CheckResult]:
    if not flink_cfg or not enabled(flink_cfg):
        return []

    results: List[CheckResult] = []
    base_url = flink_cfg.get("rest_url")
    if not base_url:
        return [result("flink", "cluster", FAIL, "missing flink.rest_url")]

    try:
        overview, ms = http_json(join_url(base_url, "/overview"), timeout)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return [result("flink", "cluster", FAIL, "Flink REST API is not reachable: {}".format(exc))]

    taskmanagers = int(overview.get("taskmanagers", 0))
    slots_total = int(overview.get("slots-total", 0))
    min_taskmanagers = int(flink_cfg.get("min_taskmanagers", 1))
    min_slots = int(flink_cfg.get("min_slots_total", 0))

    cluster_failures = []
    if taskmanagers < min_taskmanagers:
        cluster_failures.append("taskmanagers {} < {}".format(taskmanagers, min_taskmanagers))
    if slots_total < min_slots:
        cluster_failures.append("slots-total {} < {}".format(slots_total, min_slots))
    if cluster_failures:
        results.append(result("flink", "cluster", FAIL, "; ".join(cluster_failures), ms))
    else:
        detail = "REST API reachable; taskmanagers {}; slots-total {}".format(taskmanagers, slots_total)
        results.append(result("flink", "cluster", OK, detail, ms))

    try:
        jobs_doc, ms = http_json(join_url(base_url, "/jobs/overview"), timeout)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return results + [result("flink", "jobs", FAIL, "could not list Flink jobs: {}".format(exc))]

    jobs = jobs_doc.get("jobs", [])
    if not isinstance(jobs, list):
        return results + [result("flink", "jobs", FAIL, "unexpected /jobs/overview response")]

    running_count = sum(1 for job in jobs if job.get("state") == "RUNNING")
    if flink_cfg.get("fail_if_no_running_jobs", False) and running_count == 0:
        results.append(result("flink", "running jobs", FAIL, "no RUNNING jobs found", ms))
    else:
        results.append(result("flink", "running jobs", OK, "found {} RUNNING job(s) out of {}".format(running_count, len(jobs)), ms))

    expected_jobs = [job for job in as_list(flink_cfg.get("expected_jobs")) if isinstance(job, dict)]
    for expected in expected_jobs:
        label = expected.get("label") or expected.get("name") or expected.get("jid") or expected.get("id") or expected.get("name_regex") or "expected job"
        matches = [job for job in jobs if job_matches(job, expected)]
        if not matches:
            results.append(result("flink", str(label), FAIL, "expected job was not found"))
            continue

        allowed_states = [str(state) for state in as_list(expected.get("allowed_states") or ["RUNNING"])]
        allowed = set(allowed_states)
        healthy = [job for job in matches if job.get("state") in allowed]
        if healthy:
            matched = healthy[0]
            detail = "job {} ({}) is {}".format(matched.get("name"), matched.get("jid"), matched.get("state"))
            results.append(result("flink", str(label), OK, detail))
        else:
            states = sorted(set(str(job.get("state")) for job in matches))
            detail = "found job but state {} not in {}".format("/".join(states), "/".join(allowed_states))
            results.append(result("flink", str(label), FAIL, detail))

    return results


def run_checks(config: Dict[str, Any], only: Sequence[str]) -> List[CheckResult]:
    timeout = int(config.get("command_timeout_seconds", 15))
    selected = set(only)
    results: List[CheckResult] = []

    def include(name: str) -> bool:
        return not selected or name in selected

    sections = [
        ("systemd", lambda: check_systemd_user_services(config.get("systemd_user_services", []), timeout)),
        ("process", lambda: check_processes(config.get("processes", []), timeout)),
        ("kafka", lambda: check_kafka(config.get("kafka", {}), timeout)),
        ("kafka_connect", lambda: check_kafka_connect(config.get("kafka_connect", {}), timeout)),
        ("consumer", lambda: check_consumers(config.get("kafka_consumers", []), config.get("kafka", {}), timeout)),
        ("flink", lambda: check_flink(config.get("flink", {}), timeout)),
    ]

    for name, fn in sections:
        if not include(name):
            continue
        try:
            results.extend(fn())
        except Exception as exc:  # Defensive boundary so one broken section does not hide the rest.
            results.append(result(name, "section", FAIL, "unhandled check error: {}".format(exc)))

    if not results:
        results.append(result("config", "checks", FAIL, "no checks were configured"))
    return results


def parse_only(values: Optional[Sequence[str]]) -> List[str]:
    if not values:
        return []
    selected: List[str] = []
    for value in values:
        selected.extend(part.strip() for part in value.split(",") if part.strip())
    return selected


def print_human(results: Sequence[CheckResult]) -> None:
    width = max(len(item.category) for item in results) if results else 0
    for item in results:
        print("[{:<4}] {:<{}} {}: {}".format(item.status, item.category, width, item.name, item.detail))
    counts = {status: sum(1 for item in results if item.status == status) for status in (OK, WARN, FAIL, SKIP)}
    print("")
    print("Summary: {OK} ok, {WARN} warn, {FAIL} fail, {SKIP} skip".format(**counts))


def exit_code(results: Sequence[CheckResult], warn_as_fail: bool) -> int:
    if any(item.status == FAIL for item in results):
        return 2
    if warn_as_fail and any(item.status == WARN for item in results):
        return 3
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run service health checks for Kafka, Kafka Connect, consumers, and Flink.")
    parser.add_argument("--config", default="config/healthcheck.json", help="Path to the JSON config file.")
    parser.add_argument("--only", action="append", help="Comma-separated sections to run: systemd,process,kafka,kafka_connect,consumer,flink.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--warn-as-fail", action="store_true", help="Return non-zero when a check returns WARN.")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print("Config file not found: {}".format(args.config), file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print("Config file is not valid JSON: {}".format(exc), file=sys.stderr)
        return 1

    results = run_checks(config, parse_only(args.only))
    if args.json:
        print(json.dumps([asdict(item) for item in results], indent=2, sort_keys=True))
    else:
        print_human(results)
    return exit_code(results, args.warn_as_fail)


if __name__ == "__main__":
    sys.exit(main())
