#!/usr/bin/env python3
"""Keep the manual release entry bound to the single CI build path."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


class WorkflowContractTest(unittest.TestCase):
    def read(self, name):
        return (WORKFLOWS / name).read_text(encoding="utf-8")

    def test_ci_is_reusable_and_gates_only_the_artifact_upload(self):
        ci = self.read("ci.yml")
        self.assertRegex(ci, r"(?m)^\s{2}workflow_call:\s*$")
        self.assertRegex(ci, r"(?m)^\s{6}upload:\s*$")
        self.assertRegex(ci, r"(?m)^\s{8}default:\s*\"true\"\s*$")
        self.assertIn("if: ${{ inputs.upload != 'false' }}", ci)
        self.assertIn("retention-days: 14", ci)
        self.assertIn("amd64", ci)
        self.assertIn("arm64", ci)

    def test_release_uses_the_shared_ci_and_required_locked_inputs(self):
        release = self.read("release.yml")
        for name in ("expected_version", "expected_source_sha", "prepare_action",
                     "apt_overlay_url", "dependency_set_digest"):
            self.assertIn(name, release)
        self.assertIn("uses: ./.github/workflows/ci.yml", release)
        self.assertIn("upload: ${{ inputs.prepare_action == 'release' && 'true' || 'false' }}", release)
        for marker in ("inputs.prepare_action", "compatibility-verify",
                       "inputs.apt_overlay_url", "XGC2_APT_OVERLAY_URL",
                       "inputs.dependency_set_digest"):
            self.assertIn(marker, release)
        self.assertIn("git rev-parse HEAD", release)
        self.assertIn(".xgc2/product.yml", release)
        self.assertIn('test -z "$XGC2_DEPENDENCY_SET_DIGEST"', release)
        self.assertNotIn("continue-on-error: true", release)

    def test_release_does_not_duplicate_the_build(self):
        release = self.read("release.yml")
        for marker in (".xgc2/scripts/build-ci.sh", ".xgc2/scripts/in-image.sh",
                       "docker run", "actions/upload-artifact"):
            self.assertNotIn(marker, release)

    def test_release_permissions_stay_read_only(self):
        release = self.read("release.yml")
        permissions = release.split("permissions:", 1)[1].split("defaults:", 1)[0]
        self.assertIn("contents: read", permissions)
        self.assertIn("packages: read", permissions)
        self.assertNotIn("write", permissions)


if __name__ == "__main__":
    unittest.main()
