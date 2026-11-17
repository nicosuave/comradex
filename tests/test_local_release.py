import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest


MOCK_TOOL = r'''#!/usr/bin/env python3
import io, json, os, sys, tarfile
from pathlib import Path
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['MOCK_COMMAND_LOG'], 'a') as log:
    log.write(json.dumps([name, *args]) + '\n')
if name == 'git':
    if args[:1] == ['rev-parse']:
        print('a' * 40)
    elif args[:2] == ['diff', '--quiet']:
        sys.exit(int(os.environ.get('MOCK_DIRTY', '0')))
    elif args[:1] == ['archive']:
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode='w') as archive:
            archive.add('Cargo.toml', arcname='Cargo.toml')
        sys.stdout.buffer.write(output.getvalue())
    else:
        sys.exit(96)
elif name == 'uname':
    print('Darwin' if args == ['-s'] else os.environ.get('MOCK_ARCH', 'arm64'))
elif name == 'cargo':
    manifest = Path(args[args.index('--manifest-path') + 1])
    checkout = Path(os.environ['MOCK_CHECKOUT_DIR']).resolve()
    if (checkout / '.cargo/config.toml').exists() and checkout in [Path.cwd(), *Path.cwd().parents]:
        sys.exit('working checkout Cargo configuration reached the snapshot build')
    if os.environ.get('MOCK_REJECT_STALE_SOURCE') and (manifest.parent / 'build.rs').exists():
        sys.exit('stale source survived snapshot extraction')
    with open(os.environ['MOCK_SNAPSHOT_LOG'], 'a') as log:
        log.write(json.dumps({'manifest': str(manifest), 'contents': manifest.read_text(), 'cwd': str(Path.cwd())}) + '\n')
    target = args[args.index('--target') + 1]
    binary = Path(os.environ['CARGO_TARGET_DIR']) / target / 'release/comradex'
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.with_suffix('.patched').unlink(missing_ok=True)
    binary.write_bytes(b'local-release-test-executable')
    binary.chmod(0o700)
elif name == 'xcrun':
    if args[:2] == ['otool', '-L']:
        binary = Path(args[-1])
        dependency = '/usr/lib/libiconv.2.dylib' if binary.with_suffix('.patched').exists() else os.environ.get('MOCK_DEPENDENCY', '/nix/store/fixture/lib/libiconv.2.dylib')
        print(str(binary) + ':')
        print('\t' + dependency + ' (compatibility version 7.0.0, current version 7.0.0)')
        print('\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1.0.0)')
    elif args[:2] == ['install_name_tool', '-change']:
        Path(args[-1]).with_suffix('.patched').touch()
    elif args[:2] == ['notarytool', 'history']:
        print('{}')
    elif args[:2] == ['notarytool', 'submit']:
        assert Path(args[2]).is_file()
        print(json.dumps({'status': os.environ.get('MOCK_NOTARY_STATUS', 'Accepted')}))
    else:
        sys.exit(96)
elif name == 'ditto':
    Path(args[-1]).write_bytes(b'notarization-upload-fixture')
elif name == 'codesign':
    assert Path(args[-1]).is_file()
elif name == 'plutil':
    print(json.loads(Path(args[-1]).read_text())['status'])
elif name in ['gh', 'security']:
    sys.exit(97)
else:
    sys.exit(96)
'''


class LocalReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="comradex-release-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.scratch = tempfile.TemporaryDirectory(prefix="comradex-build-scratch-")
        self.addCleanup(self.scratch.cleanup)
        scripts = self.root / "scripts"
        scripts.mkdir()
        self.script = scripts / "release_macos_local.sh"
        shutil.copyfile(Path(__file__).resolve().parents[1] / "scripts/release_macos_local.sh", self.script)
        (self.root / "Cargo.toml").write_text('[package]\nversion = "0.19.6"\n')
        tools = self.root / "bin"
        tools.mkdir()
        for name in ["git", "uname", "cargo", "xcrun", "ditto", "codesign", "plutil", "gh", "security"]:
            path = tools / name
            path.write_text(MOCK_TOOL)
            path.chmod(0o700)
        self.log = self.root / "commands.jsonl"
        self.snapshot_log = self.root / "snapshots.jsonl"
        self.environment = {
            **os.environ,
            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
            "MOCK_COMMAND_LOG": str(self.log),
            "MOCK_SNAPSHOT_LOG": str(self.snapshot_log),
            "MOCK_CHECKOUT_DIR": str(self.root),
            "TMPDIR": self.scratch.name,
            "CARGO_TARGET_DIR": str(self.root / "build"),
            "CODESIGN_IDENTITY": "Developer ID Application: Fixture",
            "NOTARY_PROFILE": "fixture-notary",
        }

    def run_release(self, **environment):
        return subprocess.run(
            ["bash", str(self.script), "--local"],
            env={**self.environment, **environment},
            capture_output=True,
            text=True,
            timeout=20,
        )

    def commands(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def test_local_release_signs_and_notarizes_only_host_architecture_without_publishing(self):
        result = self.run_release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = self.commands()
        builds = [command for command in commands if command[0] == "cargo"]
        self.assertEqual(len(builds), 1)
        self.assertIn("aarch64-apple-darwin", builds[0])
        manifest = Path(builds[0][builds[0].index("--manifest-path") + 1])
        self.assertNotEqual(manifest.resolve(), (self.root / "Cargo.toml").resolve())
        snapshot = json.loads(self.snapshot_log.read_text().splitlines()[0])
        self.assertEqual(snapshot["manifest"], str(manifest))
        self.assertEqual(snapshot["contents"], (self.root / "Cargo.toml").read_text())
        self.assertFalse(any(command[0] == "gh" for command in commands))
        patches = [i for i, command in enumerate(commands) if command[:2] == ["xcrun", "install_name_tool"]]
        signatures = [i for i, command in enumerate(commands) if command[:2] == ["codesign", "--force"]]
        submits = [i for i, command in enumerate(commands) if command[:3] == ["xcrun", "notarytool", "submit"]]
        self.assertEqual(len(patches), 1)
        self.assertEqual(len(signatures), 1)
        self.assertEqual(len(submits), 1)
        self.assertLess(patches[0], signatures[0])
        self.assertLess(signatures[0], submits[0])
        archives = list((self.root / "target/local-release").rglob("*.tar.gz"))
        self.assertEqual(len(archives), 1)
        self.assertIn("local-", str(archives[0].parent))
        with tarfile.open(archives[0]) as archive:
            self.assertEqual(archive.getnames(), ["comradex"])
            self.assertEqual(archive.extractfile("comradex").read(), b"local-release-test-executable")
        self.assertTrue(archives[0].with_suffix(".gz.sha256").is_file())

    def test_dirty_source_is_rejected_before_signing_or_notarization(self):
        result = self.run_release(MOCK_DIRTY="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("uncommitted", result.stderr)
        self.assertFalse(any(command[0] in ["cargo", "codesign", "xcrun", "gh"] for command in self.commands()))

    def test_unknown_build_cache_dependency_is_rejected_before_signing_or_upload(self):
        result = self.run_release(MOCK_DEPENDENCY="/nix/store/fixture/lib/libunexpected.dylib")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime dependency", result.stderr)
        self.assertFalse(any(command[0] == "codesign" or command[:3] == ["xcrun", "notarytool", "submit"] for command in self.commands()))

    def test_rejected_notarization_does_not_produce_an_installable_archive(self):
        result = self.run_release(MOCK_NOTARY_STATUS="Invalid")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("notarization", result.stderr.lower())
        self.assertEqual(list(self.root.rglob("*.tar.gz")), [])

    def test_retry_does_not_reuse_a_contaminated_source_snapshot(self):
        first = self.run_release()
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        snapshot = json.loads(self.snapshot_log.read_text().splitlines()[0])
        source = Path(snapshot["manifest"]).parent
        source.mkdir(parents=True, exist_ok=True)
        (source / "build.rs").write_text("untracked stale build script")
        second = self.run_release(MOCK_REJECT_STALE_SOURCE="1")
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        snapshots = [json.loads(line) for line in self.snapshot_log.read_text().splitlines()]
        self.assertNotEqual(snapshots[0]["manifest"], snapshots[1]["manifest"])

    def test_failed_retry_removes_the_previous_installable_archive(self):
        first = self.run_release()
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        second = self.run_release(MOCK_NOTARY_STATUS="Invalid")
        self.assertNotEqual(second.returncode, 0)
        self.assertEqual(list(self.root.rglob("*.tar.gz")), [])
        self.assertEqual(list(self.root.rglob("*.tar.gz.sha256")), [])

    def test_snapshot_build_does_not_load_untracked_checkout_cargo_configuration(self):
        configuration = self.root / ".cargo/config.toml"
        configuration.parent.mkdir()
        configuration.write_text("untracked working-copy compiler overrides")
        result = self.run_release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
