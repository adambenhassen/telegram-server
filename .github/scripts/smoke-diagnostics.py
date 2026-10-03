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


GO_SOURCE_PATH = re.compile(r"test/e2e/(?:[A-Za-z0-9_]+/)*[A-Za-z0-9_]+\.go\Z")
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
class SourceFunction:
    path: str
    name: str
    declaration_line: int
    end_line: int | None
    header: str


@dataclass(frozen=True)
class SourceTree:
    paths: tuple[str, ...]
    source_lines: dict[str, list[str]]
    code_lines: dict[str, list[str]]
    functions: tuple[SourceFunction, ...]


@dataclass(frozen=True)
class ScenarioBinding:
    root_function: SourceFunction
    root_call: SourceLocation
    root_has_helper: bool
    test_path: str
    closure_start_line: int
    closure_end_line: int


@dataclass(frozen=True)
class SourceCall:
    name: str
    arguments: tuple[str, ...] | None
    start: int
    end: int | None


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


def go_code_lines(source_lines: list[str]) -> tuple[list[str], bool]:
    code_lines: list[str] = []
    in_block_comment = False
    quote = ""
    valid = True
    for line in source_lines:
        code = [" "] * len(line)
        index = 0
        while index < len(line):
            if in_block_comment:
                end = line.find("*/", index)
                if end < 0:
                    break
                in_block_comment = False
                index = end + 2
                continue
            if quote:
                if quote == "`":
                    end = line.find("`", index)
                    if end < 0:
                        break
                    quote = ""
                    index = end + 1
                    continue
                if line[index] == "\\":
                    index += 2
                    continue
                if line[index] == quote:
                    quote = ""
                index += 1
                continue
            if line.startswith("//", index):
                break
            if line.startswith("/*", index):
                in_block_comment = True
                index += 2
                continue
            if line[index] in {'"', "'", "`"}:
                quote = line[index]
                index += 1
                continue
            code[index] = line[index]
            index += 1
        if quote in {'"', "'"}:
            valid = False
            quote = ""
        code_lines.append("".join(code))
    return code_lines, valid and not in_block_comment and not quote


def source_string_literals(source_line: str) -> list[str]:
    literals: list[str] = []
    index = 0
    while index < len(source_line):
        if source_line.startswith("//", index):
            break
        if source_line.startswith("/*", index):
            end = source_line.find("*/", index + 2)
            if end < 0:
                break
            index = end + 2
            continue
        quote = source_line[index]
        if quote == "`":
            end = source_line.find("`", index + 1)
            if end < 0:
                break
            literals.append(source_line[index + 1 : end])
            index = end + 1
            continue
        if quote == '"':
            end = index + 1
            value: list[str] = []
            while end < len(source_line):
                if source_line[end] == "\\":
                    if end + 1 >= len(source_line):
                        end = len(source_line)
                        break
                    value.append(source_line[end : end + 2])
                    end += 2
                    continue
                if source_line[end] == '"':
                    literals.append("".join(value))
                    index = end + 1
                    break
                value.append(source_line[end])
                end += 1
            else:
                break
            if end >= len(source_line):
                break
            continue
        if quote == "'":
            end = index + 1
            while end < len(source_line):
                if source_line[end] == "\\":
                    end += 2
                elif source_line[end] == "'":
                    index = end + 1
                    break
                else:
                    end += 1
            else:
                break
            continue
        index += 1
    return literals


def split_go_arguments(source: str) -> tuple[str, ...] | None:
    if not source.strip():
        return ()
    code_lines, valid = go_code_lines([source])
    if not valid:
        return None
    code = code_lines[0]
    start = 0
    depths = {"(": 0, "[": 0, "{": 0}
    closing = {")": "(", "]": "[", "}": "{"}
    arguments: list[str] = []
    for index, char in enumerate(code):
        if char in depths:
            depths[char] += 1
        elif char in closing:
            opener = closing[char]
            depths[opener] -= 1
            if depths[opener] < 0:
                return None
        elif char == "," and not any(depths.values()):
            arguments.append(source[start:index].strip())
            start = index + 1
    if any(depths.values()):
        return None
    final = source[start:].strip()
    if final:
        arguments.append(final)
    elif arguments:
        return None
    return tuple(arguments)


def matching_delimiter(code: str, opening_index: int, opening: str, closing: str) -> int | None:
    if opening_index < 0 or opening_index >= len(code) or code[opening_index] != opening:
        return None
    depth = 0
    for index in range(opening_index, len(code)):
        if code[index] == opening:
            depth += 1
        elif code[index] == closing:
            depth -= 1
            if depth == 0:
                return index
            if depth < 0:
                return None
    return None


def source_tree(root: str) -> SourceTree | None:
    paths = source_paths(root)
    if paths is None or not paths:
        return None
    source_lines: dict[str, list[str]] = {}
    code_lines: dict[str, list[str]] = {}
    functions: list[SourceFunction] = []
    for path in paths:
        result = run_git(root, "show", f"HEAD:{path}")
        if result is None or result.returncode != 0:
            return None
        lines = result.stdout.splitlines()
        code, valid = go_code_lines(lines)
        if not valid:
            return None
        source_lines[path] = lines
        code_lines[path] = code
        for index, code_line in enumerate(code):
            declaration = FUNCTION_DECL.match(code_line)
            if declaration is None:
                continue
            opening = code_line.find("{", declaration.end())
            end_line: int | None = None
            header = code_line[:opening].strip() if opening >= 0 else code_line.strip()
            if opening >= 0:
                depth = 1
                for body_index in range(index, len(code)):
                    segment = code[body_index][opening + 1 :] if body_index == index else code[body_index]
                    depth += segment.count("{") - segment.count("}")
                    if depth == 0:
                        end_line = body_index + 1
                        break
                    if depth < 0:
                        break
            functions.append(
                SourceFunction(
                    path=path,
                    name=declaration.group("name"),
                    declaration_line=index + 1,
                    end_line=end_line,
                    header=header,
                )
            )
    return SourceTree(tuple(paths), source_lines, code_lines, tuple(functions))


def functions_named(tree: SourceTree, name: str) -> list[SourceFunction]:
    return [function for function in tree.functions if function.name == name]


def unique_function(tree: SourceTree, name: str) -> SourceFunction | None:
    matches = functions_named(tree, name)
    if len(matches) != 1 or matches[0].end_line is None:
        return None
    return matches[0]


def is_plain_function(function: SourceFunction) -> bool:
    return re.match(rf"^func\s+{re.escape(function.name)}\s*\(", function.header) is not None


def function_at(tree: SourceTree, path: str, line_number: int) -> SourceFunction | None:
    matches = [
        function
        for function in tree.functions
        if function.path == path
        and function.end_line is not None
        and function.declaration_line <= line_number <= function.end_line
    ]
    return matches[0] if len(matches) == 1 else None


def source_location(tree: SourceTree, path: str, line_number: int) -> SourceLocation | None:
    lines = tree.source_lines.get(path)
    if lines is None or line_number < 1 or line_number > len(lines):
        return None
    function = function_at(tree, path, line_number)
    if function is None:
        return None
    return SourceLocation(path, line_number, function.name, lines[line_number - 1])


def source_literal_count(source_line: str, value: str) -> int:
    delimited_value = re.compile(
        rf"(?<![A-Za-z0-9_.-]){re.escape(value)}(?![A-Za-z0-9_.-])"
    )
    return sum(
        len(list(delimited_value.finditer(literal)))
        for literal in source_string_literals(source_line)
    )


def resolve_literal(tree: SourceTree, literal: str) -> SourceLocation | None:
    if not re.fullmatch(r"[a-z0-9.-]+", literal):
        return None
    matches: list[SourceLocation] = []
    for path in tree.paths:
        for line_number, source_line in enumerate(tree.source_lines[path], 1):
            count = source_literal_count(source_line, literal)
            if not count:
                continue
            location = source_location(tree, path, line_number)
            if location is None:
                return None
            matches.extend([location] * count)
    return matches[0] if len(matches) == 1 else None


def source_calls_on_line(tree: SourceTree, path: str, line_number: int) -> list[SourceCall]:
    code = tree.code_lines[path][line_number - 1]
    source = tree.source_lines[path][line_number - 1]
    calls: list[SourceCall] = []
    for match in re.finditer(r"(?<![A-Za-z0-9_$.])(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(", code):
        opening = code.find("(", match.start("name") + len(match.group("name")))
        closing = matching_delimiter(code, opening, "(", ")")
        if closing is None:
            calls.append(SourceCall(match.group("name"), None, match.start(), None))
            continue
        args = split_go_arguments(source[opening + 1 : closing])
        calls.append(SourceCall(match.group("name"), args, match.start(), closing))
    return calls


def call_statement(tree: SourceTree, path: str, line_number: int) -> SourceCall | None:
    code = tree.code_lines[path][line_number - 1]
    source = tree.source_lines[path][line_number - 1]
    match = re.match(r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(", code)
    if match is None:
        return None
    opening = code.find("(", match.start("name") + len(match.group("name")))
    closing = matching_delimiter(code, opening, "(", ")")
    if closing is None or code[closing + 1 :].strip():
        return None
    args = split_go_arguments(source[opening + 1 : closing])
    if args is None:
        return None
    return SourceCall(match.group("name"), args, match.start(), closing)


def calls_named_in_function(
    tree: SourceTree, function: SourceFunction, name: str
) -> list[tuple[int, SourceCall]] | None:
    if function.end_line is None:
        return None
    matches: list[tuple[int, SourceCall]] = []
    for line_number in range(function.declaration_line + 1, function.end_line):
        for call in source_calls_on_line(tree, function.path, line_number):
            if call.name == name:
                if call.arguments is None:
                    return None
                matches.append((line_number, call))
    return matches


def calls_named_in_tree(tree: SourceTree, name: str) -> list[tuple[str, int, SourceCall]] | None:
    matches: list[tuple[str, int, SourceCall]] = []
    for path in tree.paths:
        for line_number, code in enumerate(tree.code_lines[path], 1):
            declaration = FUNCTION_DECL.match(code)
            if declaration is not None and declaration.group("name") == name:
                continue
            for call in source_calls_on_line(tree, path, line_number):
                if call.name == name:
                    if call.arguments is None:
                        return None
                    matches.append((path, line_number, call))
    return matches


def has_helper_first(tree: SourceTree, function: SourceFunction) -> bool:
    if function.end_line is None:
        return False
    for line_number in range(function.declaration_line + 1, function.end_line):
        statement = tree.code_lines[function.path][line_number - 1].strip()
        if statement:
            return statement == "t.Helper()"
    return False


def parameter_index(tree: SourceTree, function: SourceFunction, parameter: str) -> int | None:
    declaration = FUNCTION_DECL.match(function.header)
    if declaration is None or declaration.group("name") != function.name:
        return None
    opening = declaration.end() - 1
    closing = matching_delimiter(function.header, opening, "(", ")")
    if closing is None:
        return None
    arguments = split_go_arguments(function.header[opening + 1 : closing])
    if arguments is None:
        return None
    indices = [
        index
        for index, argument in enumerate(arguments)
        if re.fullmatch(rf"{re.escape(parameter)}\s+string", argument.strip())
    ]
    return indices[0] if len(indices) == 1 else None


def matching_block_end(tree: SourceTree, path: str, line_number: int, opening: int) -> int | None:
    code = tree.code_lines[path]
    depth = 1
    for index in range(line_number - 1, len(code)):
        segment = code[index][opening + 1 :] if index == line_number - 1 else code[index]
        depth += segment.count("{") - segment.count("}")
        if depth == 0:
            return index + 1
        if depth < 0:
            return None
    return None


def scenario_binding(tree: SourceTree, scenario: str) -> ScenarioBinding | None:
    test_functions = functions_named(tree, "TestSmoke")
    if len(test_functions) != 1 or test_functions[0].end_line is None:
        return None
    test_function = test_functions[0]
    if test_function.header != "func TestSmoke(t *testing.T)":
        return None
    closures: list[tuple[int, int]] = []
    scenario_runs = 0
    closure_pattern = re.compile(
        r'^\s*t\.Run\("(?P<scenario>[a-z0-9-]+)",\s*func\(t\s+\*testing\.T\)\s*\{\s*$'
    )
    for line_number in range(test_function.declaration_line + 1, test_function.end_line):
        raw_line = tree.source_lines[test_function.path][line_number - 1]
        code = tree.code_lines[test_function.path][line_number - 1]
        if re.search(r"\bt\.Run\s*\(", code) is None:
            continue
        match = closure_pattern.fullmatch(raw_line)
        if match is None:
            return None
        if match.group("scenario") != scenario:
            continue
        scenario_runs += 1
        if re.fullmatch(r"\s*t\.Run\s*\(\s*,\s*func\s*\(\s*t\s+\*testing\.T\s*\)\s*\{\s*", code) is None:
            return None
        opening = code.rfind("{")
        end_line = matching_block_end(tree, test_function.path, line_number, opening)
        if end_line is None:
            return None
        closures.append((line_number, end_line))
    if scenario_runs != 1 or len(closures) != 1:
        return None

    closure_start, closure_end = closures[0]
    depth = 1
    root_calls: list[tuple[int, SourceCall, bool]] = []
    malformed_call = False
    for line_number in range(closure_start + 1, closure_end):
        calls = source_calls_on_line(tree, test_function.path, line_number)
        for call in calls:
            if call.arguments is None:
                malformed_call = True
            elif call.arguments == ("t",):
                root_calls.append((line_number, call, depth == 1))
        depth += (
            tree.code_lines[test_function.path][line_number - 1].count("{")
            - tree.code_lines[test_function.path][line_number - 1].count("}")
        )
        if depth < 1:
            return None
    if malformed_call or len(root_calls) != 1:
        return None
    root_line, root_candidate, root_call_is_direct = root_calls[0]
    if not root_call_is_direct:
        return None
    statement = call_statement(tree, test_function.path, root_line)
    if (
        statement is None
        or statement.name != root_candidate.name
        or statement.arguments != ("t",)
        or statement.end != root_candidate.end
    ):
        return None
    root_function = unique_function(tree, statement.name)
    if root_function is None or not re.fullmatch(
        rf"func\s+{re.escape(root_function.name)}\s*\(\s*t\s+\*testing\.T\s*\)",
        root_function.header,
    ):
        return None
    location = source_location(tree, test_function.path, root_line)
    if location is None or location.function != "TestSmoke":
        return None
    all_root_calls = calls_named_in_tree(tree, root_function.name)
    if (
        all_root_calls is None
        or len(all_root_calls) != 1
        or all_root_calls[0][0] != test_function.path
        or all_root_calls[0][1] != root_line
        or all_root_calls[0][2].arguments != ("t",)
    ):
        return None
    return ScenarioBinding(
        root_function=root_function,
        root_call=location,
        root_has_helper=has_helper_first(tree, root_function),
        test_path=test_function.path,
        closure_start_line=closure_start,
        closure_end_line=closure_end,
    )


def basename_is_unique(tree: SourceTree, path: str) -> bool:
    basename = path.rsplit("/", 1)[-1]
    return sum(item.rsplit("/", 1)[-1] == basename for item in tree.paths) == 1


def predicted_frame(
    tree: SourceTree, binding: ScenarioBinding, callsite: SourceLocation
) -> tuple[str, int] | None:
    path = binding.test_path if binding.root_has_helper else callsite.path
    if not basename_is_unique(tree, path):
        return None
    line = binding.root_call.line if binding.root_has_helper else callsite.line
    return path.rsplit("/", 1)[-1], line


def go_string_value(expression: str) -> str | None:
    value = expression.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        if "\\" not in value[1:-1]:
            return value[1:-1]
        return None
    if len(value) >= 2 and value[0] == "`" and value[-1] == "`":
        return value[1:-1]
    return None


def scenario_id_literals(source_line: str) -> list[str]:
    return [
        literal
        for literal in source_string_literals(source_line)
        if re.fullmatch(r"[a-z0-9.-]+", literal)
    ]


def helper_call_for_id(
    tree: SourceTree, location: SourceLocation, identifier: str
) -> tuple[SourceCall, SourceFunction] | None:
    call = call_statement(tree, location.path, location.line)
    if call is None or call.name == "t":
        return None
    literals = scenario_id_literals(location.source_line)
    if literals != [identifier] or not any(
        go_string_value(argument) == identifier for argument in call.arguments or ()
    ):
        return None
    helper = unique_function(tree, call.name)
    if helper is None or not is_plain_function(helper) or not has_helper_first(tree, helper):
        return None
    return call, helper


def assertion_arguments(tree: SourceTree, location: SourceLocation) -> tuple[str, ...] | None:
    code = tree.code_lines[location.path][location.line - 1]
    matches = list(ASSERTION_CALL.finditer(code))
    if len(matches) != 1:
        return None
    opening = code.find("(", matches[0].start())
    closing = matching_delimiter(code, opening, "(", ")")
    if closing is None:
        return None
    arguments = split_go_arguments(
        tree.source_lines[location.path][location.line - 1][opening + 1 : closing]
    )
    return arguments


def direct_assertion_for_id(
    tree: SourceTree, location: SourceLocation, identifier: str
) -> bool:
    arguments = assertion_arguments(tree, location)
    if not arguments:
        return False
    first_literal = go_string_value(arguments[0])
    return first_literal is not None and first_literal.startswith(f"[assert:{identifier}]")


def helper_assertion_for_id(
    tree: SourceTree, location: SourceLocation, identifier: str
) -> bool:
    arguments = assertion_arguments(tree, location)
    if not arguments or len(arguments) < 2:
        return False
    first_literal = go_string_value(arguments[0])
    return (
        first_literal is not None
        and first_literal.startswith(f"[assert:%s/{identifier}]")
        and arguments[1].strip() == "callsiteID"
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


def annotation(
    scenario: str,
    sha: str,
    category: str,
    identifier: str,
    location: tuple[str, int],
    helper_call: tuple[str, int] | None = None,
) -> str:
    path, line = location
    detail = f"category: {category}; ID: {identifier}; location: {path}:{line}; "
    if helper_call is not None:
        helper_path, helper_line = helper_call
        detail += f"helper-call: {helper_path}:{helper_line}; "
    return (
        f"::error file={path},line={line}::TestSmoke/{scenario} failed "
        f"({detail}checked-out commit: {sha}; details redacted)"
    )


def mapped_assertion(
    root: str, scenario: str, token: str, reported_location: tuple[str, int], sha: str
) -> str | None:
    if sha == "unavailable" or not source_tree_is_clean(root):
        return None
    parsed = parse_assertion_id(token, scenario)
    if parsed is None:
        return None
    tree = source_tree(root)
    if tree is None:
        return None
    binding = scenario_binding(tree, scenario)
    if binding is None:
        return None
    first_id, second_id = parsed
    first = resolve_literal(tree, first_id)
    if first is None:
        return None

    if second_id is None:
        if first.function == binding.root_function.name:
            expected = predicted_frame(tree, binding, first)
            if expected is None or reported_location != expected:
                return None
            if direct_assertion_for_id(tree, first, first_id):
                return annotation(
                    scenario,
                    sha,
                    "assertion",
                    first_id,
                    (first.path, first.line),
                )
            if helper_call_for_id(tree, first, first_id) is None:
                return None
            return annotation(
                scenario,
                sha,
                "helper-call",
                first_id,
                (first.path, first.line),
            )

        if (
            first.function == "TestSmoke"
            and binding.closure_start_line < first.line < binding.closure_end_line
            and basename_is_unique(tree, first.path)
            and reported_location == (first.path.rsplit("/", 1)[-1], first.line)
        ):
            if direct_assertion_for_id(tree, first, first_id):
                return annotation(
                    scenario,
                    sha,
                    "assertion",
                    first_id,
                    (first.path, first.line),
                )
            if helper_call_for_id(tree, first, first_id) is not None:
                return annotation(
                    scenario,
                    sha,
                    "helper-call",
                    first_id,
                    (first.path, first.line),
                )
            return None

        helper = unique_function(tree, first.function)
        if (
            helper is None
            or not is_plain_function(helper)
            or not has_helper_first(tree, helper)
            or not direct_assertion_for_id(tree, first, first_id)
            or binding.root_has_helper
        ):
            return None
        callers = calls_named_in_function(tree, binding.root_function, helper.name)
        if callers is None or len(callers) != 1:
            return None
        caller_line, call = callers[0]
        caller_statement = call_statement(tree, binding.root_function.path, caller_line)
        if (
            caller_statement is None
            or caller_statement.name != call.name
            or caller_statement.arguments != call.arguments
            or caller_statement.end != call.end
        ):
            return None
        caller_location = source_location(tree, binding.root_function.path, caller_line)
        if (
            caller_location is None
            or caller_location.function != binding.root_function.name
            or not basename_is_unique(tree, caller_location.path)
            or reported_location
            != (caller_location.path.rsplit("/", 1)[-1], caller_location.line)
        ):
            return None
        return annotation(
            scenario,
            sha,
            "helper-call",
            first_id,
            (caller_location.path, caller_location.line),
        )

    second = resolve_literal(tree, second_id)
    if second is None or first.function != binding.root_function.name:
        return None
    expected = predicted_frame(tree, binding, first)
    if expected is None or reported_location != expected:
        return None
    callsite = helper_call_for_id(tree, first, first_id)
    if callsite is None:
        return None
    call, helper = callsite
    helper_parameter = parameter_index(tree, helper, "callsiteID")
    if helper_parameter is None or helper_parameter >= len(call.arguments or ()):
        return None
    if go_string_value((call.arguments or ())[helper_parameter]) != first_id:
        return None

    if second.function == helper.name:
        check_helper = helper
    else:
        check_helper = unique_function(tree, second.function)
        if check_helper is None or not is_plain_function(check_helper):
            return None
        helper_calls = calls_named_in_function(tree, helper, check_helper.name)
        check_parameter = parameter_index(tree, check_helper, "callsiteID")
        if (
            helper_calls is None
            or len(helper_calls) != 1
            or check_parameter is None
            or helper_parameter is None
            or helper_calls[0][1].arguments is None
            or check_parameter >= len(helper_calls[0][1].arguments or ())
            or (helper_calls[0][1].arguments or ())[check_parameter].strip() != "callsiteID"
        ):
            return None
    if (
        not has_helper_first(tree, helper)
        or not has_helper_first(tree, check_helper)
        or second.function != check_helper.name
        or not helper_assertion_for_id(tree, second, second_id)
    ):
        return None
    return annotation(
        scenario,
        sha,
        "assertion",
        f"{first_id}/{second_id}",
        (second.path, second.line),
        (first.path, first.line),
    )


def legacy_location(root: str, basename: str, line_number: int) -> tuple[str, int] | None:
    location = legacy_source_location(root, (basename, line_number))
    return (location.path, location.line) if location is not None else None


def source_line_at(root: str, path: str, line_number: int) -> SourceLocation | None:
    if not GO_SOURCE_PATH.fullmatch(path) or line_number < 1:
        return None
    tree = source_tree(root)
    if tree is None:
        return None
    return source_location(tree, path, line_number)


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
