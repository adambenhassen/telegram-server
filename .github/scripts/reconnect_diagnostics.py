#!/usr/bin/env python3
"""Run the bounded reconnect experiment and retain only safe diagnostics."""

from __future__ import annotations

import argparse
import json
import math
import os
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
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


def _elapsed(event: dict[str, object]) -> float | None:
    value = event.get("Elapsed")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return float(value)


def summarize_go_test_lines(
    lines: Iterable[str], expected_runs: int
) -> dict[str, object]:
    """Summarize the selected test without retaining unrecognized output."""
    counts = {"run": 0, "pass": 0, "fail": 0, "skip": 0}
    elapsed_seconds: list[float] = []
    error_classes: list[str] = []
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
                    error_class = safe_a2_error_class(output)
                    if error_class is not None and error_class not in error_classes:
                        error_classes.append(error_class)
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
    package_elapsed = summary["package_elapsed_seconds"]
    package_elapsed_text = (
        f"{package_elapsed:.3f}"
        if isinstance(package_elapsed, float)
        else "not_reported"
    )

    return "\n".join(
        (
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
        )
    )


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
    env["GOTOOLCHAIN"] = "go1.26.6"
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


def _stop_and_join(processes: dict[str, subprocess.Popen[bytes]]) -> None:
    active = [proc for proc in processes.values() if proc.poll() is None]
    for proc in active:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    for proc in active:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()


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


def _wait_and_report(
    name: str,
    process: subprocess.Popen[bytes],
    completed: queue.Queue[tuple[str, int, float]],
) -> None:
    code = process.wait()
    completed.put((name, code, time.monotonic()))


def _duration(started: float) -> str:
    return f"{max(0.0, time.monotonic() - started):.3f}"


def _write_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_diagnostic(args: argparse.Namespace) -> int:
    repetitions = int(args.repetitions)
    parallel_load = args.parallel_e2e == "true"
    source_dir = Path(args.source_dir).resolve()
    harness_dir = Path(args.harness_dir).resolve()
    output_path = Path(args.output).resolve()

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
            "focused_command=GOTOOLCHAIN=go1.26.6 go test -race -json "
            f"-count={repetitions} -timeout 15m ./test/e2e -run '{RUN_TEST_PATTERN}'"
        ),
        (
            "load_command=GOTOOLCHAIN=go1.26.6 go test -race -json "
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
    cleanup_status = "not_attempted"
    interrupted = False
    received_signals: list[int] = []
    runner_failure = False
    previous_handlers: dict[int, object] = {}
    run_error = "none"
    waiters: list[threading.Thread] = []

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

            completed: queue.Queue[tuple[str, int, float]] = queue.Queue()
            waiters = [
                threading.Thread(
                    target=_wait_and_report,
                    args=(name, process, completed),
                    daemon=True,
                )
                for name, process in processes.items()
            ]
            for waiter in waiters:
                waiter.start()
            while len(statuses) < len(waiters):
                if received_signals:
                    raise InterruptedError(received_signals[0])
                try:
                    name, code, ended = completed.get(timeout=0.2)
                except queue.Empty:
                    continue
                statuses[name] = code
                durations[name] = f"{max(0.0, ended - started_at[name]):.3f}"
            for waiter in waiters:
                waiter.join()
            for raw_file in raw_files.values():
                raw_file.close()

        except InterruptedError:
            interrupted = True
            _stop_and_join(processes)
            for waiter in waiters:
                if waiter.is_alive():
                    waiter.join()
            run_error = "interrupted"
            for name, process in processes.items():
                if name not in statuses:
                    statuses[name] = process.returncode if process.returncode is not None else 128
                    durations[name] = _duration(started_at[name])
            for raw_file in raw_files.values():
                raw_file.close()
        except DiagnosticError as error:
            runner_failure = True
            run_error = error.category
            _stop_and_join(processes)
            for waiter in waiters:
                if waiter.is_alive():
                    waiter.join()
            for name, process in processes.items():
                if name not in statuses:
                    statuses[name] = process.returncode if process.returncode is not None else 1
                    durations[name] = _duration(started_at[name])
            for raw_file in raw_files.values():
                raw_file.close()
        except Exception:
            runner_failure = True
            run_error = "runner_exception"
            _stop_and_join(processes)
            for waiter in waiters:
                if waiter.is_alive():
                    waiter.join()
            for name, process in processes.items():
                if name not in statuses:
                    statuses[name] = process.returncode if process.returncode is not None else 1
                    durations[name] = _duration(started_at[name])
            for raw_file in raw_files.values():
                raw_file.close()
        if received_signals:
            interrupted = True
            run_error = "interrupted"
        cleanup_status = _cleanup_container(preflight_clear)
        if received_signals:
            interrupted = True
            run_error = "interrupted"
        lines = list(base_lines[:6])
        lines.append(
            f"go_version={normalized_go_version if 'normalized_go_version' in locals() else 'unavailable'}"
        )
        if interrupted:
            lines.append("run_interrupted=true")
        if runner_failure:
            lines.append(f"diagnostic_runner_error={run_error}")

        summaries: dict[str, dict[str, object]] = {}
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
        lines.append(f"pgtest_container_cleanup={cleanup_status}")
        lines.append(
            "diagnostic_runner_status="
            + ("interrupted" if interrupted else "failed" if runner_failure else "completed")
        )
        _write_report(output_path, lines)
        print("\n".join(lines))

        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)

    if interrupted or runner_failure or cleanup_status not in ("removed_verified", "not_created"):
        return 1
    if statuses.get("focused") != 0 or summaries.get("focused", {}).get("result") != "passed":
        return 1
    if parallel_load and (
        statuses.get("load") != 0 or summaries.get("load", {}).get("result") != "passed"
    ):
        return 1
    return 0


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
    args = parser.parse_args()

    try:
        return run_diagnostic(args)
    except Exception:
        print("reconnect diagnostic runner failed (details redacted)", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
