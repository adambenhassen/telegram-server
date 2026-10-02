import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from reconnect_diagnostics import (
    _container_ids,
    _test_environment,
    format_scenario_summary,
    summarize_go_test_lines,
)


PACKAGE = "github.com/adambenhassen/telegram-server/test/e2e"
TARGET = "TestMessagingReconnectPushGap"


def event(action, test=TARGET, **fields):
    return json.dumps({"Package": PACKAGE, "Action": action, "Test": test, **fields})


class ReconnectDiagnosticsTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
