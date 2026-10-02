#!/usr/bin/env python3
"""Run the bounded reconnect experiment and retain only safe diagnostics."""

from __future__ import annotations

import argparse
import errno
import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import BinaryIO, Iterable


PACKAGE = "github.com/adambenhassen/telegram-server/test/e2e"
TARGET = "TestMessagingReconnectPushGap"
RUN_TEST_PATTERN = f"^{TARGET}$"
PG_CONTAINER_NAME = "tg-test-pg"
SAFE_CAUSE = (
    r"(?:canceled|deadline|injected probe|unexpected nil|net closed/EOF|"
    r"rpc\(code=-?\d{1,6},type=[A-Z0-9_]{1,64}\)|"
    r"other\([A-Za-z0-9_*./]+\))"
)
A2_FAILURE_LINE = re.compile(
    rf"^\s*[A-Za-z0-9_./-]+\.go:\d+:\s*"
    rf"A2 gap send failed \(cause=(?P<cause>{SAFE_CAUSE})\)\s*$"
)
SHA = re.compile(r"^[0-9a-f]{40}$")
DURATION_PATTERN = (
    r"(?:\d{1,2}h\d{1,2}m\d{1,2}(?:\.\d{1,3})?s|"
    r"\d{1,2}m\d{1,2}(?:\.\d{1,3})?s|"
    r"\d{1,3}(?:\.\d{1,3})?s|\d{1,3}ms)"
)
OUTPUT_LINE_PREFIX = re.compile(
    r"^\s*[A-Za-z0-9_./-]{1,160}\.go:\d{1,6}:\s*(?P<message>.*)$"
)
LIFECYCLE_FAILURE = re.compile(
    rf"^B1 (?P<phase>login|manager readiness|idle|command|update manager|client run) failed after "
    rf"(?P<elapsed>{DURATION_PATTERN}) "
    r"\((?P<registry>[^;\n]{1,256}); (?P<detail>[^\n]{1,512})\)$"
)
REGISTRY_COUNTS = re.compile(
    rf"^connections=(?P<connections>\d{{1,6}}) "
    rf"zero-key=(?P<zero_key>\d{{1,6}}) "
    rf"distinct-key=(?P<distinct_key>\d{{1,6}}) "
    rf"observation-age=(?P<observation_age>{DURATION_PATTERN})$"
)
CONTEXT_FAILURE = re.compile(
    rf"^cause=(?P<category>{SAFE_CAUSE}) "
    r"\((?P<registry>[^\n]{1,256})\)$"
)
RECONNECT_ASSERTION_LABEL = (
    r"(?:A2 gap result|B1 gap push|B2 gap push|"
    r"A2 live push|B1 live push|B2 live push)"
)
B1_MESSAGE_ASSERTION = re.compile(
    rf"^(?P<label>{RECONNECT_ASSERTION_LABEL}) message metadata mismatch "
    r"\(id=(?P<message_id>-?\d{1,10}) out=(?P<out>true|false)\)$"
)
B1_PTS_ASSERTION = re.compile(
    rf"^(?P<label>{RECONNECT_ASSERTION_LABEL}) pts = "
    r"(?P<observed_pts>-?\d{1,10}), want (?P<expected_pts>-?\d{1,10})$"
)
RECONNECT_WAIT_FAILURE = re.compile(
    rf"^timed out waiting for (?P<label>{RECONNECT_ASSERTION_LABEL}) "
    r"(?P<check>message|pts): "
    r"(?P<reason>.{1,512})$"
)
UNEXPECTED_ORIGIN_PUSH = re.compile(
    r"^A2 origin push during gap received an unexpected message$"
)
MAX_FAILURE_EVIDENCE = 16
MAX_REJECTED_OUTPUT_LINES = 999_999
MAX_REGISTRY_COUNT = 100_000
MAX_DIAGNOSTIC_SECONDS = 20 * 60
PROCESS_GROUP_TERM_TIMEOUT = 2.0
PROCESS_GROUP_KILL_TIMEOUT = 3.0
PROCESS_GROUP_POLL_INTERVAL = 0.05


class DiagnosticError(Exception):
    def __init__(self, category: str):
        super().__init__(category)
        self.category = category


def safe_a2_error_class(output: str) -> str | None:
    """Return only a recognized safe classifier from the A2 failure line."""
    match = A2_FAILURE_LINE.fullmatch(output.rstrip("\r\n"))
    if match is None:
        return None
    return match.group("cause")


def _safe_duration(value: str) -> str | None:
    if not value or len(value) > 32:
        return None
    hours = re.fullmatch(r"(\d{1,2})h(\d{1,2})m(\d{1,2}(?:\.\d{1,3})?)s", value)
    minutes = re.fullmatch(r"(\d{1,2})m(\d{1,2}(?:\.\d{1,3})?)s", value)
    whole_seconds = re.fullmatch(r"(\d{1,3}(?:\.\d{1,3})?)s", value)
    milliseconds = re.fullmatch(r"(\d{1,3})ms", value)
    if hours is not None:
        hour_count, minute_count, second_count = hours.groups()
        if (
            int(hour_count) == 0
            or int(minute_count) >= 60
            or float(second_count) >= 60
        ):
            return None
        seconds = int(hour_count) * 3600 + int(minute_count) * 60 + float(second_count)
    elif minutes is not None:
        minute_count, second_count = minutes.groups()
        if int(minute_count) == 0 or float(second_count) >= 60:
            return None
        seconds = int(minute_count) * 60 + float(second_count)
    elif whole_seconds is not None:
        seconds = float(whole_seconds.group(1))
        if value != "0s" and (seconds < 1 or seconds >= 60):
            return None
    elif milliseconds is not None:
        millisecond_count = int(milliseconds.group(1))
        if millisecond_count == 0 or millisecond_count >= 1000:
            return None
        seconds = millisecond_count / 1000
    else:
        return None
    if not math.isfinite(seconds) or seconds < 0 or seconds > MAX_DIAGNOSTIC_SECONDS:
        return None
    return value


def _registry_evidence(value: str) -> dict[str, object] | None:
    if value == "no valid pre-cancellation registry snapshot":
        return {"status": "unavailable"}
    match = REGISTRY_COUNTS.fullmatch(value)
    if match is None:
        return None
    connections = int(match.group("connections"))
    zero_key = int(match.group("zero_key"))
    distinct_key = int(match.group("distinct_key"))
    observation_age = _safe_duration(match.group("observation_age"))
    if (
        connections > MAX_REGISTRY_COUNT
        or zero_key > connections
        or distinct_key > connections - zero_key
        or observation_age is None
    ):
        return None
    return {
        "status": "available",
        "connections": connections,
        "zero_key": zero_key,
        "distinct_key": distinct_key,
        "observation_age": observation_age,
    }


def _lifecycle_evidence(message: str, depth: int = 0) -> dict[str, object] | None:
    if depth > 2:
        return None
    match = LIFECYCLE_FAILURE.fullmatch(message)
    if match is None:
        return None
    elapsed = _safe_duration(match.group("elapsed"))
    registry = _registry_evidence(match.group("registry"))
    if elapsed is None or registry is None:
        return None
    detail = match.group("detail")
    if detail.startswith("cause="):
        category = detail.removeprefix("cause=")
        if re.fullmatch(SAFE_CAUSE, category) is None:
            return None
    else:
        nested = _lifecycle_evidence(detail, depth + 1)
        if nested is None:
            return None
        return nested
    return {
        "kind": "b1_lifecycle_failure",
        "client": "B1",
        "phase": match.group("phase").replace(" ", "_"),
        "error_category": category,
        "elapsed": elapsed,
        "registry": registry,
    }


def _safe_failure_evidence(output: str) -> dict[str, object] | None:
    if len(output) > 2048:
        return None
    prefix = OUTPUT_LINE_PREFIX.fullmatch(output.rstrip("\r\n"))
    if prefix is None:
        return None
    message = prefix.group("message")

    a2_error = safe_a2_error_class(output)
    if a2_error is not None:
        return {"kind": "a2_send_failure", "error_category": a2_error}

    lifecycle = _lifecycle_evidence(message)
    if lifecycle is not None:
        return lifecycle

    message_assertion = B1_MESSAGE_ASSERTION.fullmatch(message)
    if message_assertion is not None:
        message_id = int(message_assertion.group("message_id"))
        if -2_147_483_648 <= message_id <= 2_147_483_647:
            label = message_assertion.group("label")
            return {
                "kind": "delivery_assertion_failure",
                "recipient": label.split()[0],
                "scenario": _reconnect_scenario(label),
                "check": "message_metadata",
                "message_id": message_id,
                "out": message_assertion.group("out") == "true",
            }

    pts_assertion = B1_PTS_ASSERTION.fullmatch(message)
    if pts_assertion is not None:
        observed_pts = int(pts_assertion.group("observed_pts"))
        expected_pts = int(pts_assertion.group("expected_pts"))
        if all(-2_147_483_648 <= value <= 2_147_483_647 for value in (observed_pts, expected_pts)):
            label = pts_assertion.group("label")
            return {
                "kind": "delivery_assertion_failure",
                "recipient": label.split()[0],
                "scenario": _reconnect_scenario(label),
                "check": "pts",
                "observed_pts": observed_pts,
                "expected_pts": expected_pts,
            }

    if UNEXPECTED_ORIGIN_PUSH.fullmatch(message):
        return {
            "kind": "delivery_assertion_failure",
            "recipient": "A2",
            "scenario": "origin_push_during_gap",
            "check": "unexpected_message",
        }

    wait_failure = RECONNECT_WAIT_FAILURE.fullmatch(message)
    if wait_failure is not None:
        reason = wait_failure.group("reason")
        lifecycle = _lifecycle_evidence(reason)
        if lifecycle is not None:
            return lifecycle
        context = CONTEXT_FAILURE.fullmatch(reason)
        if context is None:
            return None
        registry = _registry_evidence(context.group("registry"))
        if registry is None:
            return None
        label = wait_failure.group("label")
        return {
            "kind": "delivery_wait_failure",
            "recipient": label.split()[0],
            "scenario": _reconnect_scenario(label),
            "check": wait_failure.group("check"),
            "error_category": context.group("category"),
            "registry": registry,
        }
    return None


def _reconnect_scenario(label: str) -> str:
    if label == "A2 origin push during gap":
        return "origin_push_during_gap"
    words = label.split()
    if len(words) == 3 and words[1] in ("gap", "live") and words[2] in ("push", "result"):
        return f"{words[1]}_push"
    return "unknown"


def _elapsed(event: dict[str, object]) -> float | None:
    value = event.get("Elapsed")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        elapsed = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(elapsed) or elapsed < 0:
        return None
    return elapsed


def summarize_go_test_lines(
    lines: Iterable[str], expected_runs: int
) -> dict[str, object]:
    """Summarize the selected test without retaining unrecognized output."""
    counts = {"run": 0, "pass": 0, "fail": 0, "skip": 0}
    elapsed_seconds: list[float] = []
    error_classes: list[str] = []
    failure_evidence: list[dict[str, object]] = []
    rejected_output_lines = 0
    omitted_failure_evidence = 0
    package_result = "not_reported"
    package_elapsed: float | None = None

    for line in lines:
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(event, dict) or event.get("Package") != PACKAGE:
            continue

        action = event.get("Action")
        if not isinstance(action, str):
            continue
        test_name = event.get("Test") or ""
        if test_name == TARGET:
            if action in counts:
                counts[action] += 1
                if action in ("pass", "fail", "skip"):
                    elapsed = _elapsed(event)
                    if elapsed is not None:
                        elapsed_seconds.append(elapsed)
            elif action == "output":
                output = event.get("Output")
                if isinstance(output, str):
                    for output_line in output.splitlines():
                        evidence = _safe_failure_evidence(output_line)
                        if evidence is None:
                            if OUTPUT_LINE_PREFIX.fullmatch(output_line):
                                rejected_output_lines = min(
                                    MAX_REJECTED_OUTPUT_LINES, rejected_output_lines + 1
                                )
                            continue
                        if evidence["kind"] == "a2_send_failure":
                            error_class = evidence["error_category"]
                            if (
                                isinstance(error_class, str)
                                and error_class not in error_classes
                                and len(error_classes) < MAX_FAILURE_EVIDENCE
                            ):
                                error_classes.append(error_class)
                        if evidence not in failure_evidence:
                            if len(failure_evidence) < MAX_FAILURE_EVIDENCE:
                                failure_evidence.append(evidence)
                            else:
                                omitted_failure_evidence = min(
                                    MAX_REJECTED_OUTPUT_LINES,
                                    omitted_failure_evidence + 1,
                                )
        elif test_name == "" and action in ("pass", "fail"):
            package_result = action
            package_elapsed = _elapsed(event)

    if counts["skip"]:
        result = "skipped"
    elif counts["fail"]:
        result = "failed"
    elif counts["run"] == expected_runs and counts["pass"] == expected_runs:
        result = "passed"
    else:
        result = "incomplete"

    return {
        **counts,
        "elapsed_seconds": elapsed_seconds,
        "a2_error_classes": error_classes,
        "failure_evidence": failure_evidence,
        "rejected_output_lines": rejected_output_lines,
        "omitted_failure_evidence": omitted_failure_evidence,
        "package_result": package_result,
        "package_elapsed_seconds": package_elapsed,
        "result": result,
        "delivery_observation": (
            "b1_and_b2_gap_pushes_asserted" if result == "passed" else "not_established"
        ),
    }


def format_scenario_summary(label: str, summary: dict[str, object]) -> str:
    if label not in ("focused", "load"):
        raise ValueError("unsupported diagnostic label")

    elapsed = summary["elapsed_seconds"]
    assert isinstance(elapsed, list)
    elapsed_text = ",".join(f"{value:.3f}" for value in elapsed) or "none"
    error_classes = summary["a2_error_classes"]
    assert isinstance(error_classes, list)
    error_text = ",".join(error_classes) or "not_observed"
    failure_evidence = summary["failure_evidence"]
    assert isinstance(failure_evidence, list)
    package_elapsed = summary["package_elapsed_seconds"]
    package_elapsed_text = (
        f"{package_elapsed:.3f}"
        if isinstance(package_elapsed, float)
        else "not_reported"
    )

    lines = [
        f"{label}_scenario_run_events={summary['run']}",
        f"{label}_scenario_pass_events={summary['pass']}",
        f"{label}_scenario_fail_events={summary['fail']}",
        f"{label}_scenario_skip_events={summary['skip']}",
        f"{label}_scenario_elapsed_seconds={elapsed_text}",
        f"{label}_scenario_result={summary['result']}",
        f"{label}_delivery_observation={summary['delivery_observation']}",
        f"{label}_a2_error_classes={error_text}",
        f"{label}_package_result={summary['package_result']}",
        f"{label}_package_elapsed_seconds={package_elapsed_text}",
        f"{label}_rejected_output_lines={summary['rejected_output_lines']}",
        f"{label}_omitted_failure_evidence={summary['omitted_failure_evidence']}",
    ]
    for index, evidence in enumerate(failure_evidence, start=1):
        encoded = json.dumps(evidence, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        lines.append(f"{label}_failure_evidence_{index}={encoded}")
    return "\n".join(lines)


def _load_suite_outcome(
    requested: bool, exit_code: int | None, summary: dict[str, object] | None
) -> str:
    if not requested:
        return "not_requested"
    if exit_code is None:
        return "not_run"
    if exit_code != 0 or summary is None or summary.get("package_result") == "fail":
        return "failed"
    if summary.get("package_result") == "pass":
        return "passed"
    return "incomplete"


def _diagnostic_exit_code(
    statuses: dict[str, int],
    summaries: dict[str, dict[str, object]],
    interrupted: bool,
    runner_failure: bool,
    cleanup_status: str,
    parallel_load: bool,
) -> int:
    if interrupted or runner_failure or cleanup_status != "complete":
        return 1

    focused = summaries.get("focused", {})
    if (
        statuses.get("focused") != 0
        or focused.get("result") != "passed"
        or focused.get("package_result") != "pass"
    ):
        return 1

    if parallel_load:
        load = summaries.get("load", {})
        if (
            statuses.get("load") != 0
            or load.get("result") != "passed"
            or load.get("package_result") != "pass"
        ):
            return 1
    return 0


def _test_environment() -> dict[str, str]:
    # Deliberately do not pass GitHub tokens, deployed-service settings, or
    # caller-provided TG_*, DSN, AWS, or object-store credentials to test code.
    env = {
        name: os.environ[name]
        for name in (
            "PATH",
            "HOME",
            "GOPATH",
            "GOCACHE",
            "GOMODCACHE",
            "TMPDIR",
            "LANG",
            "LC_ALL",
        )
        if name in os.environ
    }
    env["CI"] = "true"
    env["GOTOOLCHAIN"] = "go1.27.1"
    env["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    return env


def _git_head(path: Path) -> str | None:
    result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and SHA.fullmatch(value) else None


def _is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool:
    result = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, descendant],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _container_ids() -> tuple[list[str] | None, str]:
    try:
        result = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"name=^/{PG_CONTAINER_NAME}$"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, "docker_unavailable"
    if result.returncode != 0:
        return None, "docker_unavailable"
    ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if any(re.fullmatch(r"[0-9a-f]{12,64}", value) is None for value in ids):
        return None, "container_id_invalid"
    return ids, "ok"


def _cleanup_container(preflight_clear: bool) -> str:
    if not preflight_clear:
        return "not_attempted_unowned"

    ids, status = _container_ids()
    if status != "ok":
        return status
    assert ids is not None
    if not ids:
        return "not_created"
    if len(ids) != 1:
        return "multiple_containers_unremoved"

    container_id = ids[0]
    try:
        inspect = subprocess.run(
            ["docker", "inspect", "--format", "{{json .Config.Labels}}", container_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "ownership_unverified"
    if inspect.returncode != 0:
        return "ownership_unverified"
    try:
        labels = json.loads(inspect.stdout)
    except json.JSONDecodeError:
        return "ownership_unverified"
    if not isinstance(labels, dict) or labels.get("org.testcontainers") != "true":
        return "ownership_unverified"
    if labels.get("org.testcontainers.lang") != "go":
        return "ownership_unverified"

    try:
        removed = subprocess.run(
            ["docker", "rm", "-f", container_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "remove_failed"
    if removed.returncode != 0:
        return "remove_failed"
    remaining, status = _container_ids()
    if status != "ok":
        return "verify_failed"
    assert remaining is not None
    if remaining:
        return "container_remains"
    return "removed_verified"


def _cleanup_container_after_groups(
    process_group_cleanup: dict[str, object], preflight_clear: bool
) -> str:
    remaining = process_group_cleanup.get("remaining_groups")
    if not isinstance(remaining, list) or remaining:
        return "not_attempted_process_groups_remain"
    return _cleanup_container(preflight_clear)


def _process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except OSError as error:
        if error.errno == errno.ESRCH:
            return False
        return True
    return True


def _remaining_process_groups(groups: dict[str, int]) -> list[str]:
    return [name for name, pgid in groups.items() if _process_group_exists(pgid)]


def _wait_for_process_groups(
    groups: dict[str, int],
    processes: dict[str, subprocess.Popen[bytes]],
    timeout: float,
) -> list[str]:
    deadline = time.monotonic() + timeout
    while True:
        for process in processes.values():
            process.poll()
        remaining = _remaining_process_groups(groups)
        if not remaining:
            return []
        delay = deadline - time.monotonic()
        if delay <= 0:
            return remaining
        time.sleep(min(PROCESS_GROUP_POLL_INTERVAL, delay))


def _stop_and_join(
    processes: dict[str, subprocess.Popen[bytes]],
    term_timeout: float = PROCESS_GROUP_TERM_TIMEOUT,
    kill_timeout: float = PROCESS_GROUP_KILL_TIMEOUT,
) -> dict[str, object]:
    """Terminate and verify every owned test process group, not just leaders."""
    groups = {name: process.pid for name, process in processes.items()}
    signal_failures: list[str] = []
    escalated_groups: list[str] = []

    for name, pgid in groups.items():
        if not _process_group_exists(pgid):
            continue
        try:
            os.killpg(pgid, signal.SIGTERM)
        except OSError as error:
            if error.errno != errno.ESRCH:
                signal_failures.append(f"{name}_sigterm")

    remaining = _wait_for_process_groups(groups, processes, term_timeout)
    for name in remaining:
        escalated_groups.append(name)
        try:
            os.killpg(groups[name], signal.SIGKILL)
        except OSError as error:
            if error.errno != errno.ESRCH:
                signal_failures.append(f"{name}_sigkill")

    remaining = _wait_for_process_groups(groups, processes, kill_timeout)
    return {
        "status": "verified" if not remaining and not signal_failures else "failed",
        "escalated_groups": escalated_groups,
        "remaining_groups": remaining,
        "signal_failures": signal_failures,
    }


def _start_test(
    command: list[str],
    source_dir: Path,
    env: dict[str, str],
    raw_path: Path,
) -> tuple[subprocess.Popen[bytes], BinaryIO, float]:
    raw_file = raw_path.open("wb")
    started = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            cwd=source_dir,
            env=env,
            stdout=raw_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError:
        raw_file.close()
        raise
    return process, raw_file, started


def _duration(started: float) -> str:
    return f"{max(0.0, time.monotonic() - started):.3f}"


def _write_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_artifact_ready(path: Path, ready: bool) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(f"artifact_ready={'true' if ready else 'false'}\n")


def _artifact_publication_ready(
    process_group_cleanup: dict[str, object], report_written: bool
) -> bool:
    remaining = process_group_cleanup.get("remaining_groups")
    return report_written and isinstance(remaining, list) and not remaining


def run_diagnostic(args: argparse.Namespace) -> int:
    repetitions = int(args.repetitions)
    parallel_load = args.parallel_e2e == "true"
    source_dir = Path(args.source_dir).resolve()
    harness_dir = Path(args.harness_dir).resolve()
    output_path = Path(args.output).resolve()
    artifact_ready_path = Path(args.artifact_ready_output)
    _write_artifact_ready(artifact_ready_path, False)

    if not SHA.fullmatch(args.source_sha) or not SHA.fullmatch(args.workflow_sha):
        _write_report(output_path, ["diagnostic_runner_status=invalid_revision"])
        print("diagnostic request rejected (details redacted)", file=sys.stderr)
        return 1
    if repetitions not in (1, 2, 3) or args.parallel_e2e not in ("true", "false"):
        _write_report(output_path, ["diagnostic_runner_status=invalid_bounded_input"])
        print("diagnostic request rejected (details redacted)", file=sys.stderr)
        return 1

    base_lines = [
        f"workflow_sha={args.workflow_sha}",
        f"tested_sha={args.source_sha}",
        f"repetition_count={repetitions}",
        f"load_mode={'parallel-e2e' if parallel_load else 'isolated'}",
        (
            "focused_command=GOTOOLCHAIN=go1.27.1 go test -race -json "
            f"-count={repetitions} -timeout 15m ./test/e2e -run '{RUN_TEST_PATTERN}'"
        ),
        (
            "load_command=GOTOOLCHAIN=go1.27.1 go test -race -json "
            "-count=1 -timeout 15m ./test/e2e"
            if parallel_load
            else "load_command=not_requested"
        ),
        "go_version=not_run",
        "focused_exit_code=not_run",
        "focused_process_elapsed_seconds=not_run",
        "load_exit_code=not_requested" if not parallel_load else "load_exit_code=not_run",
        "load_process_elapsed_seconds=not_requested"
        if not parallel_load
        else "load_process_elapsed_seconds=not_run",
        "focused_scenario_result=not_run",
        "load_suite_outcome=not_requested" if not parallel_load else "load_suite_outcome=not_run",
        "diagnostic_runner_status=starting",
    ]
    _write_report(output_path, base_lines)

    focused_command = [
        "go",
        "test",
        "-race",
        "-json",
        f"-count={repetitions}",
        "-timeout",
        "15m",
        "./test/e2e",
        "-run",
        RUN_TEST_PATTERN,
    ]
    load_command = [
        "go",
        "test",
        "-race",
        "-json",
        "-count=1",
        "-timeout",
        "15m",
        "./test/e2e",
    ]

    processes: dict[str, subprocess.Popen[bytes]] = {}
    raw_files: dict[str, BinaryIO] = {}
    started_at: dict[str, float] = {}
    raw_paths: dict[str, Path] = {}
    statuses: dict[str, int] = {}
    durations: dict[str, str] = {}
    preflight_clear = False
    cleanup_status = "failed"
    process_group_cleanup: dict[str, object] = {
        "status": "verified",
        "escalated_groups": [],
        "remaining_groups": [],
        "signal_failures": [],
    }
    container_cleanup_status = "not_attempted"
    interrupted = False
    received_signals: list[int] = []
    runner_failure = False
    previous_handlers: dict[int, object] = {}
    run_error = "none"
    summaries: dict[str, dict[str, object]] = {}
    groups_gone = False
    report_written = False

    with tempfile.TemporaryDirectory(
        prefix="reconnect-diagnostic-", dir=os.environ.get("RUNNER_TEMP")
    ) as temporary_dir:
        temp_dir = Path(temporary_dir)

        def on_signal(signum: int, _frame: object) -> None:
            received_signals.append(signum)

        def check_interrupt() -> None:
            if received_signals:
                raise InterruptedError(received_signals[0])

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, on_signal)

        try:
            actual_workflow_sha = _git_head(harness_dir)
            actual_source_sha = _git_head(source_dir)
            if actual_workflow_sha != args.workflow_sha or actual_source_sha != args.source_sha:
                raise DiagnosticError("checkout_mismatch")
            if not _is_ancestor(harness_dir, args.source_sha, args.workflow_sha):
                raise DiagnosticError("source_not_on_main_history")

            env = _test_environment()
            go_version = subprocess.run(
                ["go", "version"],
                cwd=source_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=False,
            )
            version_match = re.fullmatch(
                r"go version (go[0-9.]+) ([a-z0-9_]+)/([a-z0-9_]+)",
                go_version.stdout.strip(),
            )
            if go_version.returncode != 0 or version_match is None:
                raise DiagnosticError("toolchain_unavailable")
            check_interrupt()
            normalized_go_version = (
                f"{version_match.group(1)} {version_match.group(2)}/{version_match.group(3)}"
            )

            container_ids, container_status = _container_ids()
            if container_status != "ok" or container_ids is None:
                raise DiagnosticError("docker_preflight_failed")
            if container_ids:
                raise DiagnosticError("preexisting_pgtest_container")
            preflight_clear = True
            check_interrupt()

            if parallel_load:
                check_interrupt()
                raw_paths["load"] = temp_dir / "load.jsonl"
                process, raw_file, started = _start_test(load_command, source_dir, env, raw_paths["load"])
                processes["load"] = process
                raw_files["load"] = raw_file
                started_at["load"] = started

            check_interrupt()
            raw_paths["focused"] = temp_dir / "focused.jsonl"
            process, raw_file, started = _start_test(focused_command, source_dir, env, raw_paths["focused"])
            processes["focused"] = process
            raw_files["focused"] = raw_file
            started_at["focused"] = started

            pending = set(processes)
            while pending:
                if received_signals:
                    raise InterruptedError(received_signals[0])
                for name in tuple(pending):
                    code = processes[name].poll()
                    if code is not None:
                        statuses[name] = code
                        durations[name] = _duration(started_at[name])
                        pending.remove(name)
                if pending:
                    time.sleep(0.2)

        except InterruptedError:
            interrupted = True
            run_error = "interrupted"
        except DiagnosticError as error:
            runner_failure = True
            run_error = error.category
        except Exception:
            runner_failure = True
            run_error = "runner_exception"
        if received_signals:
            interrupted = True
            run_error = "interrupted"

        try:
            process_group_cleanup = _stop_and_join(processes)
        except Exception:
            process_group_cleanup = {
                "status": "failed",
                "escalated_groups": [],
                "remaining_groups": list(processes),
                "signal_failures": list(processes),
            }
        remaining_groups = process_group_cleanup["remaining_groups"]
        groups_gone = isinstance(remaining_groups, list) and not remaining_groups
        if process_group_cleanup["status"] != "verified":
            runner_failure = True
            if run_error == "none":
                run_error = "process_group_cleanup_failed"

        for name, process in processes.items():
            code = process.poll()
            if name not in statuses:
                statuses[name] = code if code is not None else (128 if interrupted else 1)
                durations[name] = _duration(started_at[name])
        for raw_file in raw_files.values():
            raw_file.close()

        container_cleanup_status = _cleanup_container_after_groups(
            process_group_cleanup, preflight_clear
        )
        if received_signals:
            interrupted = True
            run_error = "interrupted"

        cleanup_status = (
            "complete"
            if process_group_cleanup["status"] == "verified"
            and container_cleanup_status in ("removed_verified", "not_created")
            else "failed"
        )
        lines = list(base_lines[:6])
        lines.append(
            f"go_version={normalized_go_version if 'normalized_go_version' in locals() else 'unavailable'}"
        )
        if interrupted:
            lines.append("run_interrupted=true")
        if runner_failure:
            lines.append(f"diagnostic_runner_error={run_error}")

        if groups_gone:
            for name, expected_runs in (("focused", repetitions), ("load", 1)):
                if name not in raw_paths:
                    continue
                try:
                    with raw_paths[name].open("r", encoding="utf-8", errors="replace") as raw_log:
                        summary = summarize_go_test_lines(raw_log, expected_runs)
                except OSError:
                    summary = summarize_go_test_lines([], expected_runs)
                    runner_failure = True
                    run_error = "log_read_failure"
                summaries[name] = summary
                lines.append(format_scenario_summary(name, summary))
        for name in ("focused", "load"):
            if name == "load" and not parallel_load:
                continue
            if name in summaries:
                continue
            lines.extend(
                (
                    f"{name}_scenario_run_events=0",
                    f"{name}_scenario_pass_events=0",
                    f"{name}_scenario_fail_events=0",
                    f"{name}_scenario_skip_events=0",
                    f"{name}_scenario_elapsed_seconds=none",
                    f"{name}_scenario_result=not_run",
                    f"{name}_delivery_observation=not_established",
                    f"{name}_a2_error_classes=not_observed",
                    f"{name}_package_result=not_reported",
                    f"{name}_package_elapsed_seconds=not_reported",
                    f"{name}_rejected_output_lines=0",
                    f"{name}_omitted_failure_evidence=0",
                )
            )

        lines.append(
            "load_suite_outcome="
            + _load_suite_outcome(
                parallel_load,
                statuses.get("load"),
                summaries.get("load"),
            )
        )
        lines.append(f"focused_exit_code={statuses.get('focused', 'not_run')}")
        lines.append(
            "focused_process_elapsed_seconds="
            + durations.get("focused", "not_run")
        )
        lines.append(
            f"load_exit_code={statuses.get('load', 'not_requested' if not parallel_load else 'not_run')}"
        )
        lines.append(
            "load_process_elapsed_seconds="
            + durations.get("load", "not_requested" if not parallel_load else "not_run")
        )

        if interrupted and "run_interrupted=true" not in lines:
            lines.append("run_interrupted=true")
        if runner_failure and not any(line.startswith("diagnostic_runner_error=") for line in lines):
            lines.append(f"diagnostic_runner_error={run_error}")
        lines.append(f"process_group_cleanup={process_group_cleanup['status']}")
        for key, report_key in (
            ("escalated_groups", "process_group_cleanup_escalated_groups"),
            ("remaining_groups", "process_group_cleanup_remaining_groups"),
            ("signal_failures", "process_group_cleanup_signal_failures"),
        ):
            values = process_group_cleanup[key]
            assert isinstance(values, list)
            lines.append(f"{report_key}={','.join(values) or 'none'}")
        lines.append(f"pgtest_container_cleanup={container_cleanup_status}")
        lines.append(f"diagnostic_cleanup_status={cleanup_status}")
        lines.append(
            "diagnostic_runner_status="
            + ("interrupted" if interrupted else "failed" if runner_failure else "completed")
        )
        _write_report(output_path, lines)
        report_written = True
        print("\n".join(lines))

        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)

    if _artifact_publication_ready(process_group_cleanup, report_written):
        _write_artifact_ready(artifact_ready_path, True)
    return _diagnostic_exit_code(
        statuses,
        summaries,
        interrupted,
        runner_failure,
        cleanup_status,
        parallel_load,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--harness-dir", required=True)
    run_parser.add_argument("--source-dir", required=True)
    run_parser.add_argument("--workflow-sha", required=True)
    run_parser.add_argument("--source-sha", required=True)
    run_parser.add_argument("--repetitions", required=True)
    run_parser.add_argument("--parallel-e2e", choices=("true", "false"), required=True)
    run_parser.add_argument("--output", required=True)
    run_parser.add_argument("--artifact-ready-output", required=True)
    args = parser.parse_args()

    try:
        return run_diagnostic(args)
    except Exception:
        print("reconnect diagnostic runner failed (details redacted)", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
