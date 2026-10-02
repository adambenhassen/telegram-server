#!/usr/bin/env python3
"""Emit redacted diagnostics for failed TestSmoke scenarios."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import TextIO


GO_SOURCE_PATH = re.compile(r"test/e2e/[a-z0-9_]+\.go\Z")
ASSERTION_ID = re.compile(r"([a-z0-9-]+)\.([a-z0-9-]+)\Z")
ASSERTION_PAIR = re.compile(
    r"([a-z0-9-]+)\.([a-z0-9-]+)/([a-z0-9-]+)\.([a-z0-9-]+)\Z"
)
OUTPUT_PREFIX = re.compile(
    r"^(?P<indent> +)(?P<file>[a-z0-9_]+\.go):"
    r"(?P<line>[1-9][0-9]{0,5}): "
)
ASSERTION_CALL = re.compile(r"\bt\.(?:Error|Errorf|Fatal|Fatalf)\s*\(")
FUNCTION_DECL = re.compile(
    r"^\s*func\s+(?:\([^)]*\)\s*)?"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:\[[^\]]+\])?\s*\("
)
GO_STRING_LITERAL = re.compile(r'"((?:\\.|[^"\\])*)"|`([^`]*)`')
ASSERTION_MARKER = "[assert:"
RACE_OUTPUT = re.compile(r"(?i)(?:WARNING: DATA RACE|race detected during execution)")
TIMEOUT_OUTPUT = re.compile(r"panic: test timed out after")
JSON_ACTIONS = {"start", "run", "pause", "cont", "output", "pass", "bench", "fail", "skip"}


@dataclass(frozen=True)
class SourceLocation:
    path: str
    line: int
    function: str
    source_line: str


@dataclass(frozen=True)
class OutputRecord:
    test: str
    marker_count: int
    token: str | None
    location: tuple[str, int] | None
    safe_legacy_location: tuple[str, int] | None


def run_git(root: str, *args: str) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git", "-C", root, *args],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except OSError:
        return None


def checked_out_commit(root: str) -> str:
    result = run_git(root, "rev-parse", "--verify", "HEAD^{commit}")
    if result is None or result.returncode != 0:
        return "unavailable"
    commit = result.stdout.strip()
    return commit if re.fullmatch(r"[0-9a-f]{40}", commit) else "unavailable"


def read_event_stream(
    source: TextIO, package: str, indent: str
) -> tuple[dict[str, list[OutputRecord]], set[str], bool, bool]:
    records: dict[str, list[OutputRecord]] = {}
    failed_tests: set[str] = set()
    race_or_timeout = False
    has_events = False
    last_action = ""
    last_test = ""
    for raw_line in source:
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}, set(), False, False
        if (
            not isinstance(event, dict)
            or event.get("Package") != package
            or not isinstance(event.get("Action"), str)
            or event["Action"] not in JSON_ACTIONS
        ):
            return {}, set(), False, False
        if "Test" in event and event["Test"] is not None and not isinstance(event["Test"], str):
            return {}, set(), False, False
        if "Output" in event and not isinstance(event["Output"], str):
            return {}, set(), False, False
        has_events = True
        last_action = event["Action"]
        last_test = event.get("Test") or ""
        if last_action == "fail":
            failed_tests.add(last_test)
        if last_action == "output":
            test = event.get("Test") or ""
            output = event.get("Output", "")
            if RACE_OUTPUT.search(output) or TIMEOUT_OUTPUT.search(output):
                race_or_timeout = True
            if test.startswith("TestSmoke"):
                records.setdefault(test, []).append(output_record(test, output, indent))

    complete = has_events and last_test == "" and last_action in {"pass", "fail"}
    return records, failed_tests, complete, race_or_timeout


def output_record(test: str, output: str, indent: str) -> OutputRecord:
    marker_count = output.count(ASSERTION_MARKER)
    token: str | None = None
    reported_location: tuple[str, int] | None = None
    legacy_location: tuple[str, int] | None = None

    first_line, _separator, _continuation = output.partition("\n")
    if output.endswith("\n") and "\r" not in first_line:
        line = first_line
        prefix = OUTPUT_PREFIX.match(line)
        if prefix is not None:
            basename = prefix.group("file")
            number = int(prefix.group("line"))
            if line.startswith(indent + basename + ":" + str(number) + ": "):
                legacy_location = (basename, number)

            if indent and line.startswith(indent):
                rest = line[len(indent) :]
                source_match = re.match(
                    r"(?P<file>[a-z0-9_]+\.go):(?P<line>[1-9][0-9]{0,5}): "
                    r"\[assert:(?P<id>[^\]\r\n]+)\] ",
                    rest,
                )
                if source_match is not None and marker_count == 1:
                    token = source_match.group("id")
                    reported_location = (
                        source_match.group("file"),
                        int(source_match.group("line")),
                    )

    safe_legacy = legacy_location
    if (
        not output.endswith("\n")
        or "\n" in output[:-1]
        or "\r" in output
        or "\x1b" in output
        or "::" in output
        or re.search(r"(?i)%0[ad]", output) is not None
    ):
        safe_legacy = None

    return OutputRecord(
        test=test,
        marker_count=marker_count,
        token=token,
        location=reported_location,
        safe_legacy_location=safe_legacy,
    )


def source_paths(root: str) -> list[str] | None:
    result = run_git(root, "ls-tree", "-r", "-z", "--name-only", "HEAD", "--", "test/e2e")
    if result is None or result.returncode != 0:
        return None
    return [
        path
        for path in result.stdout.split("\0")
        if GO_SOURCE_PATH.fullmatch(path)
    ]


def source_tree_is_clean(root: str) -> bool:
    diff = run_git(root, "diff", "--quiet", "HEAD", "--", "test/e2e")
    if diff is None or diff.returncode != 0:
        return False
    untracked = run_git(root, "ls-files", "--others", "--", "test/e2e")
    return untracked is not None and untracked.returncode == 0 and not untracked.stdout


def resolve_literal(root: str, literal: str, paths: list[str]) -> SourceLocation | None:
    if not re.fullmatch(r"[a-z0-9.-]+", literal) or not paths:
        return None
    matches: list[tuple[str, int]] = []
    for path in paths:
        result = run_git(
            root,
            "grep",
            "--color=never",
            "-n",
            "-o",
            "-F",
            "--",
            literal,
            "HEAD",
            "--",
            path,
        )
        if result is None or result.returncode not in {0, 1}:
            return None
        if result.returncode == 1:
            continue
        for match in result.stdout.splitlines():
            match = re.sub(r"^HEAD:", "", match)
            parts = match.split(":", 2)
            if len(parts) != 3 or parts[0] != path or parts[2] != literal:
                return None
            if not parts[1].isdecimal() or int(parts[1]) < 1:
                return None
            matches.append((path, int(parts[1])))
    if len(matches) != 1:
        return None

    path, line_number = matches[0]
    source = run_git(root, "show", f"HEAD:{path}")
    if source is None or source.returncode != 0:
        return None
    source_lines = source.stdout.splitlines()
    if line_number > len(source_lines):
        return None

    function = ""
    for line in source_lines[:line_number]:
        declaration = FUNCTION_DECL.match(line)
        if declaration is not None:
            function = declaration.group("name")
    if not function:
        return None
    return SourceLocation(path, line_number, function, source_lines[line_number - 1])


def source_line_at(root: str, path: str, line_number: int) -> SourceLocation | None:
    if not GO_SOURCE_PATH.fullmatch(path) or line_number < 1:
        return None
    source = run_git(root, "show", f"HEAD:{path}")
    if source is None or source.returncode != 0:
        return None
    source_lines = source.stdout.splitlines()
    if line_number > len(source_lines):
        return None
    function = ""
    for line in source_lines[:line_number]:
        declaration = FUNCTION_DECL.match(line)
        if declaration is not None:
            function = declaration.group("name")
    if not function:
        return None
    return SourceLocation(path, line_number, function, source_lines[line_number - 1])


def source_has_literal(source_line: str, value: str) -> bool:
    return any(value in (double or raw) for double, raw in GO_STRING_LITERAL.findall(source_line))


def source_has_assertion_tag(source_line: str, assertion_id: str) -> bool:
    tag = f"[assert:{assertion_id}]"
    return any(
        (double or raw).startswith(tag)
        for double, raw in GO_STRING_LITERAL.findall(source_line)
    )


def parse_assertion_id(token: str, scenario: str) -> tuple[str, str | None] | None:
    direct = ASSERTION_ID.fullmatch(token)
    if direct is not None:
        if direct.group(1) != scenario:
            return None
        return token, None

    paired = ASSERTION_PAIR.fullmatch(token)
    if paired is None:
        return None
    callsite = f"{paired.group(1)}.{paired.group(2)}"
    check = f"{paired.group(3)}.{paired.group(4)}"
    if paired.group(1) != scenario or paired.group(3) != scenario:
        return None
    return callsite, check


def mapped_assertion(
    root: str, scenario: str, token: str, reported_location: tuple[str, int], sha: str
) -> str | None:
    if sha == "unavailable" or not source_tree_is_clean(root):
        return None
    parsed = parse_assertion_id(token, scenario)
    if parsed is None:
        return None
    source_file_list = source_paths(root)
    if source_file_list is None:
        return None
    first_id, second_id = parsed
    first = resolve_literal(root, first_id, source_file_list)
    if first is None:
        return None
    expected_output_location = (first.path.rsplit("/", 1)[-1], first.line)

    if second_id is None:
        if first.function.startswith("TestSmoke"):
            if reported_location != expected_output_location:
                return None
            if ASSERTION_CALL.search(first.source_line):
                if not source_has_assertion_tag(first.source_line, first_id):
                    return None
                category = "assertion"
                location = (first.path, first.line)
            else:
                if not source_has_literal(first.source_line, first_id):
                    return None
                category = "helper-call"
                location = (first.path, first.line)
        else:
            if not ASSERTION_CALL.search(first.source_line) or not source_has_assertion_tag(
                first.source_line, first_id
            ):
                return None
            caller = legacy_source_location(root, reported_location)
            if caller is None or re.search(
                rf"\b{re.escape(first.function)}\s*\(", caller.source_line
            ) is None:
                return None
            location = (caller.path, caller.line)
            category = "helper-call"
        path, line = location
        return (
            f"::error file={path},line={line}::TestSmoke/{scenario} failed "
            f"(category: {category}; ID: {first_id}; location: {path}:{line}; "
            f"checked-out commit: {sha}; details redacted)"
        )

    if reported_location != expected_output_location:
        return None
    second = resolve_literal(root, second_id, source_file_list)
    if second is None:
        return None
    callsite_is_test = first.function.startswith("TestSmoke")
    callsite_is_assertion = ASSERTION_CALL.search(first.source_line) is not None
    callsite_invokes_helper = re.search(
        rf"\b{re.escape(second.function)}\s*\(", first.source_line
    ) is not None
    check_is_helper = not second.function.startswith("TestSmoke")
    check_is_assertion = ASSERTION_CALL.search(second.source_line) is not None
    if (
        not callsite_is_test
        or callsite_is_assertion
        or not source_has_literal(first.source_line, first_id)
        or not callsite_invokes_helper
        or not check_is_helper
        or not check_is_assertion
        or not source_has_literal(second.source_line, second_id)
    ):
        return None
    return (
        f"::error file={second.path},line={second.line}::TestSmoke/{scenario} failed "
        f"(category: assertion; ID: {first_id}/{second_id}; "
        f"location: {second.path}:{second.line}; helper-call: {first.path}:{first.line}; "
        f"checked-out commit: {sha}; details redacted)"
    )


def legacy_location(root: str, basename: str, line_number: int) -> tuple[str, int] | None:
    location = legacy_source_location(root, (basename, line_number))
    return (location.path, location.line) if location is not None else None


def legacy_source_location(
    root: str, reported_location: tuple[str, int]
) -> SourceLocation | None:
    basename, line_number = reported_location
    if not re.fullmatch(r"[a-z0-9_]+\.go", basename) or line_number < 1:
        return None
    paths = source_paths(root)
    if paths is None:
        return None
    candidates = [path for path in paths if path.rsplit("/", 1)[-1] == basename]
    if len(candidates) != 1:
        return None
    return source_line_at(root, candidates[0], line_number)


def format_unavailable(scenario: str, sha: str) -> str:
    return (
        f"::error::TestSmoke/{scenario} failed (category: scenario-failure; "
        f"location-unavailable; checked-out commit: {sha}; details redacted)"
    )


def report_failure(
    status: int,
    package: str,
    root: str,
    indent: str,
    scenarios: list[str],
    source: TextIO,
) -> int:
    if status == 0:
        return 0

    sha = checked_out_commit(root)
    if len(set(scenarios)) != len(scenarios) or any(
        re.fullmatch(r"[a-z0-9-]+", scenario) is None for scenario in scenarios
    ):
        print(
            f"::error::E2E suite failed (category: execution-failure; "
            f"checked-out commit: {sha}; details redacted)"
        )
        return 0

    output_records, failed_tests, complete, race_or_timeout = read_event_stream(
        source, package, indent
    )
    if not complete:
        print(
            f"::error::E2E suite failed (category: execution-failure; "
            f"checked-out commit: {sha}; details redacted)"
        )
        return 0

    if race_or_timeout:
        print(
            f"::error::E2E suite failed (category: execution-failure; "
            f"checked-out commit: {sha}; details redacted)"
        )
        return 0

    failed_scenarios: set[str] = set()
    unknown_smoke_failure = False
    nested_failures: set[str] = set()
    parent_failed = "TestSmoke" in failed_tests
    for test in failed_tests:
        if not test.startswith("TestSmoke/"):
            continue
        suffix = test.removeprefix("TestSmoke/")
        scenario, separator, _nested = suffix.partition("/")
        if scenario not in scenarios:
            unknown_smoke_failure = True
        else:
            failed_scenarios.add(scenario)
            if separator:
                nested_failures.add(scenario)

    has_non_smoke_failure = any(
        test and not test.startswith("TestSmoke") for test in failed_tests
    )
    if not failed_scenarios:
        if parent_failed or unknown_smoke_failure:
            print(
                f"::error::TestSmoke failed (category: suite-failure; "
                f"checked-out commit: {sha}; details redacted)"
            )
        elif has_non_smoke_failure:
            print(
                f"::error::E2E suite failed (category: suite-failure; "
                f"checked-out commit: {sha}; details redacted)"
            )
        else:
            print(
                f"::error::E2E suite failed (category: execution-failure; "
                f"checked-out commit: {sha}; details redacted)"
            )
        return 0

    if unknown_smoke_failure:
        print(
            f"::error::TestSmoke failed (category: suite-failure; "
            f"checked-out commit: {sha}; details redacted)"
        )

    parent_records = output_records.get("TestSmoke", [])
    for scenario in scenarios:
        if scenario not in failed_scenarios:
            continue
        test_name = f"TestSmoke/{scenario}"
        exact_records = output_records.get(test_name, [])
        nested_records = [
            record
            for name, values in output_records.items()
            if name.startswith(test_name + "/")
            for record in values
        ]
        metadata_records = [
            record
            for record in [*parent_records, *exact_records, *nested_records]
            if record.marker_count > 0
        ]

        if metadata_records:
            valid_records = [
                record
                for record in metadata_records
                if record.test == test_name
                and record.marker_count == 1
                and record.token is not None
                and record.location is not None
            ]
            tokens = {record.token for record in valid_records}
            if (
                len(valid_records) == len(metadata_records)
                and len(tokens) == 1
                and len({(record.token, record.location) for record in valid_records}) == 1
                and not any(record.marker_count > 0 for record in nested_records)
                and scenario not in nested_failures
                and not parent_records_with_markers(parent_records)
            ):
                record = valid_records[0]
                annotation = mapped_assertion(
                    root,
                    scenario,
                    record.token or "",
                    record.location or ("", 0),
                    sha,
                )
                if annotation is not None:
                    print(annotation)
                    continue
            print(format_unavailable(scenario, sha))
            continue

        if scenario in nested_failures:
            print(format_unavailable(scenario, sha))
            continue

        if sha == "unavailable" or not indent:
            print(format_unavailable(scenario, sha))
            continue
        legacy: tuple[str, int] | None = None
        for record in exact_records:
            if record.safe_legacy_location is not None:
                legacy = legacy_location(root, *record.safe_legacy_location)
                if legacy is not None:
                    break
        if legacy is None:
            print(format_unavailable(scenario, sha))
            continue
        path, line = legacy
        print(
            f"::error file={path},line={line}::TestSmoke/{scenario} failed "
            f"(category: scenario-failure; location: {path}:{line}; "
            f"checked-out commit: {sha}; details redacted)"
        )

    if has_non_smoke_failure:
        print(
            f"::error::E2E suite failed (category: suite-failure; "
            f"checked-out commit: {sha}; details redacted)"
        )
    if parent_failed and not failed_scenarios:
        print(
            f"::error::TestSmoke failed (category: suite-failure; "
            f"checked-out commit: {sha}; details redacted)"
        )
    return 0


def parent_records_with_markers(records: list[OutputRecord]) -> bool:
    return any(record.marker_count > 0 for record in records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", type=int, required=True)
    parser.add_argument("--package", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--indent", default="")
    parser.add_argument("--scenario", action="append", required=True)
    parser.add_argument("input", nargs="?", default="-")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = os.path.realpath(args.root)
    try:
        source = sys.stdin if args.input == "-" else open(args.input, "r", encoding="utf-8")
    except OSError:
        if args.status != 0:
            print(
                f"::error::E2E suite failed (category: execution-failure; "
                f"checked-out commit: {checked_out_commit(root)}; details redacted)"
            )
        return 0
    try:
        return report_failure(
            args.status,
            args.package,
            root,
            args.indent,
            args.scenario,
            source,
        )
    finally:
        if source is not sys.stdin:
            source.close()


if __name__ == "__main__":
    raise SystemExit(main())
