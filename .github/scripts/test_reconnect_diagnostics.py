import json
import os
import select
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from reconnect_diagnostics import (
    _artifact_publication_ready,
    _cleanup_container_after_groups,
    _container_ids,
    _diagnostic_exit_code,
    _load_suite_outcome,
    _stop_and_join,
    _test_environment,
    format_scenario_summary,
    summarize_go_test_lines,
)


PACKAGE = "github.com/adambenhassen/telegram-server/test/e2e"
TARGET = "TestMessagingReconnectPushGap"


def event(action, test=TARGET, **fields):
    return json.dumps({"Package": PACKAGE, "Action": action, "Test": test, **fields})


class ReconnectDiagnosticsTest(unittest.TestCase):
    def test_diagnostic_workflow_execs_runner_for_step_timeout_signal_delivery(self):
        workflow_path = (
            Path(__file__).resolve().parents[1]
            / "workflows"
            / "reconnect-diagnostic.yml"
        )
        workflow = workflow_path.read_text(encoding="utf-8")
        diagnostic_step = workflow.split(
            "      - name: Run bounded reconnect diagnostic\n", maxsplit=1
        )[1].split("\n      - name:", maxsplit=1)[0]

        self.assertIn(
            "          exec python3 harness/.github/scripts/reconnect_diagnostics.py run \\\n",
            diagnostic_step,
        )

    def test_keeps_only_target_events_and_allowlisted_a2_error_class(self):
        secret = "auth-key-bytes=do-not-retain phone=+15551046101"
        lines = [
            event("run"),
            event(
                "output",
                Output="    messaging_reconnect_test.go:118: A2 gap send failed (cause=rpc(code=420,type=FLOOD_WAIT_3))\n",
            ),
            event("output", Output=f"    messaging_reconnect_test.go:119: {secret}\n"),
            event("pass", Elapsed=1.25),
            event("fail", test="TestUnrelated", Output=secret),
        ]

        summary = summarize_go_test_lines(lines, expected_runs=1)
        rendered = format_scenario_summary("focused", summary)

        self.assertEqual(summary["result"], "passed")
        self.assertEqual(summary["a2_error_classes"], ["rpc(code=420,type=FLOOD_WAIT_3)"])
        self.assertIn("focused_scenario_run_events=1", rendered)
        self.assertIn(
            "focused_delivery_observation=b1_and_b2_gap_pushes_asserted", rendered
        )
        self.assertIn("focused_scenario_elapsed_seconds=1.250", rendered)
        self.assertNotIn(secret, rendered)

    def test_rejects_unclassified_raw_a2_error(self):
        secret = "raw RPC body with auth-key-bytes=private"
        summary = summarize_go_test_lines(
            [
                event("run"),
                event(
                    "output",
                    Output=f"    messaging_reconnect_test.go:118: A2 gap send failed (cause={secret})\n",
                ),
                event("fail", Elapsed=2.0),
            ],
            expected_runs=1,
        )

        rendered = format_scenario_summary("focused", summary)

        self.assertEqual(summary["result"], "failed")
        self.assertEqual(summary["a2_error_classes"], [])
        self.assertNotIn(secret, rendered)

    def test_retains_b1_lifecycle_failure_phase_category_timing_and_registry(self):
        summary = summarize_go_test_lines(
            [
                event("run"),
                event(
                    "output",
                    Output=(
                        "    messaging_echo_lifecycle_helper_test.go:208: "
                        "B1 idle failed after 1.25s "
                        "(connections=2 zero-key=0 distinct-key=2 "
                        "observation-age=20ms; cause=canceled)\n"
                    ),
                ),
                event("fail", Elapsed=1.25),
            ],
            expected_runs=1,
        )

        self.assertEqual(
            summary["failure_evidence"],
            [
                {
                    "kind": "b1_lifecycle_failure",
                    "client": "B1",
                    "phase": "idle",
                    "error_category": "canceled",
                    "elapsed": "1.25s",
                    "registry": {
                        "status": "available",
                        "connections": 2,
                        "zero_key": 0,
                        "distinct_key": 2,
                        "observation_age": "20ms",
                    },
                }
            ],
        )

    def test_distinguishes_b1_delivery_assertion_from_lifecycle_failure(self):
        summary = summarize_go_test_lines(
            [
                event("run"),
                event(
                    "output",
                    Output=(
                        "    messaging_reconnect_test.go:424: "
                        "B1 gap push message metadata mismatch "
                        "(id=1046101 out=false)\n"
                    ),
                ),
                event("fail", Elapsed=1.25),
            ],
            expected_runs=1,
        )

        self.assertEqual(
            summary["failure_evidence"],
            [
                {
                    "kind": "delivery_assertion_failure",
                    "recipient": "B1",
                    "scenario": "gap_push",
                    "check": "message_metadata",
                    "message_id": 1046101,
                    "out": False,
                }
            ],
        )

    def test_b1_wait_failure_keeps_lifecycle_cause_instead_of_delivery_timeout(self):
        summary = summarize_go_test_lines(
            [
                event("run"),
                event(
                    "output",
                    Output=(
                        "    messaging_test.go:219: timed out waiting for B1 gap push message: "
                        "B1 idle failed after 1.25s "
                        "(connections=2 zero-key=0 distinct-key=2 "
                        "observation-age=20ms; cause=canceled)\n"
                    ),
                ),
                event("fail", Elapsed=1.25),
            ],
            expected_runs=1,
        )

        self.assertEqual(summary["failure_evidence"][0]["kind"], "b1_lifecycle_failure")
        self.assertEqual(summary["failure_evidence"][0]["phase"], "idle")
        self.assertEqual(summary["failure_evidence"][0]["error_category"], "canceled")

    def test_nested_b1_lifecycle_failure_keeps_inner_phase_metadata(self):
        summary = summarize_go_test_lines(
            [
                event("run"),
                event(
                    "output",
                    Output=(
                        "    smoke_test.go:1436: B1 login failed after 2s "
                        "(connections=2 zero-key=0 distinct-key=2 "
                        "observation-age=30ms; "
                        "B1 idle failed after 1.25s "
                        "(connections=2 zero-key=0 distinct-key=2 "
                        "observation-age=20ms; cause=canceled))\n"
                    ),
                ),
                event("fail", Elapsed=2.0),
            ],
            expected_runs=1,
        )

        self.assertEqual(summary["failure_evidence"][0]["phase"], "idle")
        self.assertEqual(summary["failure_evidence"][0]["elapsed"], "1.25s")

    def test_allowlisted_synthetic_delivery_labels_are_retained(self):
        outputs = (
            "messaging_reconnect_test.go:424: "
            "A2 gap result message metadata mismatch (id=1046101 out=true)",
            "messaging_reconnect_test.go:424: "
            "B2 gap push message metadata mismatch (id=1046101 out=false)",
            "messaging_reconnect_test.go:129: "
            "A2 origin push during gap received an unexpected message",
        )
        summary = summarize_go_test_lines(
            [event("run"), *(event("output", Output=f"    {line}\n") for line in outputs), event("fail")],
            expected_runs=1,
        )

        self.assertEqual(
            [entry["recipient"] for entry in summary["failure_evidence"]],
            ["A2", "B2", "A2"],
        )
        self.assertEqual(
            [entry["scenario"] for entry in summary["failure_evidence"]],
            ["gap_push", "gap_push", "origin_push_during_gap"],
        )


    def test_rejects_unknown_or_unsafe_b1_failure_output(self):
        secrets = (
            "auth-key-bytes=private",
            "phone=+15551046101",
            "salt=private",
        )
        outputs = (
            "messaging_echo_lifecycle_helper_test.go:208: "
            "B1 idle failed after 1s (connections=1 zero-key=0 "
            "distinct-key=1 observation-age=1ms; cause=canceled) "
            + secrets[0],
            "messaging_echo_lifecycle_helper_test.go:208: "
            "B1 password failed after 1s (connections=1 zero-key=0 "
            "distinct-key=1 observation-age=1ms; cause=canceled)",
            "messaging_reconnect_test.go:424: "
            "B1 gap push message metadata mismatch (id=1046101 out=false) "
            + secrets[1],
            "messaging_echo_lifecycle_helper_test.go:208: "
            "B1 idle failed after 1s (connections=1 zero-key=0 "
            "distinct-key=1 observation-age=1ms; cause=unknown) "
            + secrets[2],
        )
        summary = summarize_go_test_lines(
            [event("run"), *(event("output", Output=f"    {line}\n") for line in outputs), event("fail")],
            expected_runs=1,
        )
        rendered = format_scenario_summary("focused", summary)

        self.assertEqual(summary["failure_evidence"], [])
        self.assertEqual(summary["rejected_output_lines"], 4)
        for secret in secrets:
            self.assertNotIn(secret, rendered)
        self.assertIn("focused_rejected_output_lines=4", rendered)

    def test_skip_and_missing_selector_are_not_green(self):
        skipped = summarize_go_test_lines(
            [event("run"), event("skip", Elapsed=0.01)], expected_runs=1
        )
        missing = summarize_go_test_lines([event("pass", test="")], expected_runs=1)

        self.assertEqual(skipped["result"], "skipped")
        self.assertEqual(missing["result"], "incomplete")
        self.assertEqual(missing["delivery_observation"], "not_established")

    def test_test_process_receives_no_credentials_or_service_overrides(self):
        with patch.dict(
            os.environ,
            {
                "PATH": "/usr/bin",
                "HOME": "/home/runner",
                "GITHUB_TOKEN": "secret-token",
                "TG_POSTGRES_DSN": "deployed-dsn",
                "TG_BLOB_S3_ENDPOINT": "deployed-endpoint",
                "AWS_SECRET_ACCESS_KEY": "secret-key",
            },
            clear=True,
        ):
            env = _test_environment()

        self.assertEqual(env["GOTOOLCHAIN"], "go1.26.6")
        self.assertEqual(env["TESTCONTAINERS_RYUK_DISABLED"], "true")
        self.assertNotIn("GITHUB_TOKEN", env)
        self.assertNotIn("TG_POSTGRES_DSN", env)
        self.assertNotIn("TG_BLOB_S3_ENDPOINT", env)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)

    def test_docker_preflight_timeout_fails_closed(self):
        with patch(
            "reconnect_diagnostics.subprocess.run",
            side_effect=subprocess.TimeoutExpired("docker ps", 30),
        ):
            self.assertEqual(_container_ids(), (None, "docker_unavailable"))

    def test_cleanup_kills_child_that_survives_its_leader(self):
        child_program = (
            "import signal, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "print('child-ready', flush=True); time.sleep(60)"
        )
        leader_program = (
            "import signal, subprocess, sys, time\n"
            "signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))\n"
            f"child = subprocess.Popen([sys.executable, '-c', {child_program!r}], stdout=subprocess.PIPE, text=True)\n"
            "assert child.stdout is not None\n"
            "assert child.stdout.readline().strip() == 'child-ready'\n"
            "print('leader-ready', flush=True)\n"
            "time.sleep(60)\n"
        )
        leader = subprocess.Popen(
            [sys.executable, "-c", leader_program],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
        )
        try:
            self.assertIsNotNone(leader.stdout)
            ready, _, _ = select.select([leader.stdout], [], [], 5)
            self.assertTrue(ready, "test process group did not become ready")
            self.assertEqual(leader.stdout.readline().strip(), "leader-ready")

            cleanup = _stop_and_join({"focused": leader})

            self.assertEqual(cleanup["status"], "verified")
            self.assertEqual(cleanup["escalated_groups"], ["focused"])
            with self.assertRaises(ProcessLookupError):
                os.killpg(leader.pid, 0)
        finally:
            try:
                os.killpg(leader.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            leader.wait(timeout=5)
            if leader.stdout is not None:
                leader.stdout.close()

    def test_process_group_cleanup_escalates_and_reports_unverified_groups(self):
        class StuckProcess:
            pid = 999_999

            def poll(self):
                return None

        with (
            patch("reconnect_diagnostics._process_group_exists", return_value=True),
            patch("reconnect_diagnostics.os.killpg") as killpg,
        ):
            started = time.monotonic()
            cleanup = _stop_and_join(
                {"focused": StuckProcess()}, term_timeout=0.02, kill_timeout=0.02
            )
            elapsed = time.monotonic() - started

        self.assertLess(elapsed, 1.0)
        self.assertEqual(cleanup["status"], "failed")
        self.assertEqual(cleanup["remaining_groups"], ["focused"])
        self.assertEqual(
            [call.args[1] for call in killpg.call_args_list],
            [signal.SIGTERM, signal.SIGKILL],
        )

    def test_postgres_cleanup_waits_until_every_process_group_is_gone(self):
        with patch("reconnect_diagnostics._cleanup_container") as cleanup_container:
            status = _cleanup_container_after_groups(
                {"remaining_groups": ["focused"]}, preflight_clear=True
            )

        self.assertEqual(status, "not_attempted_process_groups_remain")
        cleanup_container.assert_not_called()

        with patch(
            "reconnect_diagnostics._cleanup_container", return_value="removed_verified"
        ) as cleanup_container:
            status = _cleanup_container_after_groups(
                {"remaining_groups": []}, preflight_clear=True
            )

        self.assertEqual(status, "removed_verified")
        cleanup_container.assert_called_once_with(True)

    def test_artifact_publication_requires_verified_process_group_disappearance(self):
        self.assertFalse(
            _artifact_publication_ready(
                {"remaining_groups": ["focused"]}, report_written=True
            )
        )
        self.assertFalse(
            _artifact_publication_ready({"remaining_groups": []}, report_written=False)
        )
        self.assertTrue(
            _artifact_publication_ready({"remaining_groups": []}, report_written=True)
        )

    def test_diagnostic_verdict_requires_completed_success_and_verified_cleanup(self):
        passed = {"result": "passed", "package_result": "pass"}
        skipped = {"result": "skipped", "package_result": "pass"}
        incomplete = {"result": "incomplete", "package_result": "not_reported"}

        self.assertEqual(
            _diagnostic_exit_code(
                {"focused": 0}, {"focused": passed}, False, False, "complete", False
            ),
            0,
        )
        for statuses, summaries, interrupted, runner_failure, cleanup_status in (
            ({"focused": 0}, {"focused": skipped}, False, False, "complete"),
            ({"focused": 0}, {"focused": incomplete}, False, False, "complete"),
            ({"focused": 1}, {"focused": passed}, False, False, "complete"),
            ({"focused": 0}, {"focused": passed}, True, False, "complete"),
            ({"focused": 0}, {"focused": passed}, False, True, "complete"),
            ({"focused": 0}, {"focused": passed}, False, False, "ownership_unverified"),
            ({"focused": 0}, {"focused": passed}, False, False, "process_groups_remain"),
        ):
            with self.subTest(cleanup_status=cleanup_status, statuses=statuses):
                self.assertEqual(
                    _diagnostic_exit_code(
                        statuses,
                        summaries,
                        interrupted,
                        runner_failure,
                        cleanup_status,
                        False,
                    ),
                    1,
                )

    def test_load_suite_failure_is_separate_and_keeps_overall_exit_nonzero(self):
        summaries = {
            "focused": {"result": "passed", "package_result": "pass"},
            "load": {"result": "passed", "package_result": "fail"},
        }

        self.assertEqual(_load_suite_outcome(True, 1, summaries["load"]), "failed")
        self.assertEqual(summaries["focused"]["result"], "passed")
        self.assertEqual(
            _diagnostic_exit_code(
                {"focused": 0, "load": 1}, summaries, False, False, "complete", True
            ),
            1,
        )


if __name__ == "__main__":
    unittest.main()
