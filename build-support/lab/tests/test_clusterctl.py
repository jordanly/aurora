# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and limitations under the License.
"""Daemon-free cluster ownership regressions; not physical Docker evidence."""
import contextlib
import copy
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, Mock

SOURCE = Path(__file__).resolve().parents[1] / "clusterctl"
LOADER = importlib.machinery.SourceFileLoader("clusterctl_test", str(SOURCE))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
cluster = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(cluster)
CID = "a" * 64


def response(stdout="", code=0):
    return subprocess.CompletedProcess(["docker"], code, stdout, "")


class ClusterCtlTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.lab = cluster.Lab(self.root, fresh=True)
        self.lab.data = {"schema": 1, "root": str(self.root), "uid": os.getuid(), "gid": os.getgid(),
                         "run": "0123456789abcdef", "imageId": "sha256:" + "c" * 64,
                         "containers": {}, "networks": {}, "status": "prepared"}
        for index, role in enumerate(("control", "worker-a", "worker-b", "restore"), 1):
            self.lab.data["networks"][role] = {"id": str(index) * 64, "name": self.lab.name(role)}
        (self.root / "evidence").mkdir()
        self.lab.persist()
        self.addCleanup(self.lab.release)

    def container(self, role="agent-a", partial=False):
        networks = cluster.TOPOLOGY[role]
        directory = self.root / role / "state"
        directory.mkdir(parents=True, exist_ok=True)
        record = {"id": None if partial else CID, "name": self.lab.name(role), "args": ["owned-binary"],
                  "networks": networks.copy(), "mounts": [[str(directory), "/state", True]],
                  "publish": role == "scheduler" or role in cluster.RESTORE_ROLES,
                  "apiMode": "private" if role == "scheduler" or role in cluster.RESTORE_ROLES else None}
        self.lab.data["containers"][role] = record
        endpoints = {self.lab.name(n): {"NetworkID": self.lab.data["networks"][n]["id"],
                     "IPAddress": "172.22.0.2", "Aliases": [record["name"], *cluster.network_aliases(role, n)]} for n in networks}
        bindings = {}
        ports = {"8443/tcp": None} if record["publish"] else {}
        return {"Id": CID, "Name": "/" + record["name"], "Image": self.lab.data["imageId"],
                "Config": {"Labels": self.lab.labels(role), "User": f"{os.getuid()}:{os.getgid()}", "Cmd": record["args"]},
                "HostConfig": {"ReadonlyRootfs": True, "Init": True, "Privileged": False,
                               "PidMode": "", "CapDrop": ["ALL"], "CapAdd": None,
                               "SecurityOpt": ["no-new-privileges:true"], "PortBindings": bindings},
                "Mounts": [{"Type": "bind", "Source": str(directory), "Destination": "/state", "RW": True}],
                "NetworkSettings": {"Networks": endpoints, "Ports": ports},
                "State": {"Running": True, "Status": "running"}}

    def inspect(self, role, item, **kwargs):
        with patch.object(cluster, "docker", return_value=response(json.dumps([item]))), \
                patch.object(self.lab, "inspect_network", return_value={}):
            return self.lab.inspect_container(role, **kwargs)

    def test_identity_label_mount_mismatches_preserve_owned_record(self):
        for mutation in ("id", "label", "mount", "added-capability"):
            with self.subTest(mutation=mutation):
                item = self.container()
                if mutation == "id": item["Id"] = "b" * 64
                elif mutation == "label": item["Config"]["Labels"]["com.aurora.lab.run"] = "unrelated"
                elif mutation == "mount": item["Mounts"][0]["Source"] = "/unrelated"
                else: item["HostConfig"]["CapAdd"] = ["SYS_ADMIN"]
                with self.assertRaises(cluster.native.SmokeError): self.inspect("agent-a", item)
                self.assertEqual(CID, self.lab.data["containers"]["agent-a"]["id"])

    def test_alias_and_network_identity_are_verified(self):
        item = self.container("proxy-a")
        self.inspect("proxy-a", item)
        for field, value in (("Aliases", ["agent-b"]), ("NetworkID", "f" * 64)):
            altered = copy.deepcopy(item)
            altered["NetworkSettings"]["Networks"][self.lab.name("control")][field] = value
            with self.assertRaises(cluster.native.SmokeError): self.inspect("proxy-a", altered)

    def test_partial_creation_recovers_only_cidfile_identity(self):
        item = self.container("proxy-a", partial=True)
        (self.root / "proxy-a.cid").write_text(CID)
        del item["NetworkSettings"]["Networks"][self.lab.name("control")]
        item["State"] = {"Running": False, "Status": "created"}
        item["NetworkSettings"]["Networks"][self.lab.name("worker-a")]["NetworkID"] = ""
        with self.assertRaises(cluster.native.SmokeError): self.inspect("proxy-a", item)
        self.assertEqual(CID, self.inspect("proxy-a", item, allow_partial=True)["Id"])
        (self.root / "proxy-a.cid").unlink()
        with patch.object(cluster, "docker") as docker:
            with self.assertRaises(cluster.native.SmokeError): self.lab.inspect_container("proxy-a", allow_partial=True)
        docker.assert_not_called()

    def test_private_endpoint_rejects_all_publications_and_address_changes(self):
        item = self.container("scheduler")
        self.inspect("scheduler", item)
        for section in ("HostConfig", "NetworkSettings"):
            altered = copy.deepcopy(item)
            key = "PortBindings" if section == "HostConfig" else "Ports"
            altered[section][key] = {"8443/tcp": [{"HostIp": "127.0.0.1", "HostPort": "35443"}]}
            with self.assertRaises(cluster.native.SmokeError): self.inspect("scheduler", altered)
        self.lab.data["containers"]["scheduler"]["address"] = "172.22.0.3"
        with self.assertRaises(cluster.native.SmokeError): self.inspect("scheduler", item)

    def test_discarded_legacy_publication_is_cleanup_only(self):
        item = self.container("scheduler")
        del self.lab.data["containers"]["scheduler"]["apiMode"]
        item["HostConfig"]["PortBindings"] = {"8443/tcp": [{"HostIp": "127.0.0.1", "HostPort": ""}]}
        with self.assertRaises(cluster.native.SmokeError): self.inspect("scheduler", item)
        self.inspect("scheduler", item, allow_partial=True)
        item["NetworkSettings"]["Ports"]["8443/tcp"] = [{"HostIp": "127.0.0.1", "HostPort": "35443"}]
        with self.assertRaises(cluster.native.SmokeError): self.inspect("scheduler", item, allow_partial=True)

    def test_private_tls_connect_preserves_scheduler_hostname(self):
        context = Mock()
        raw = Mock()
        with patch.object(cluster.socket, "create_connection", return_value=raw) as connect:
            connection = cluster.LocalTLS("172.22.0.2", context)
            connection.connect()
        connect.assert_called_once_with(("172.22.0.2", 8443), 5)
        context.wrap_socket.assert_called_once_with(raw, server_hostname="scheduler")
        for address in ("127.0.0.1", "0.0.0.0", "8.8.8.8", "169.254.1.2"):
            with self.assertRaises(cluster.native.SmokeError): cluster.LocalTLS(address, context)

    def test_proxy_backend_uses_unique_worker_name_not_control_alias(self):
        with patch.object(self.lab, "create_network"), \
                patch.object(self.lab, "create_container") as create, \
                patch.object(self.lab, "await_state"):
            self.lab.start()
        proxies = {call.args[0]: call.args[1] for call in create.call_args_list
                   if call.args[0].startswith("proxy-")}
        for node in cluster.NODES:
            args = proxies["proxy-" + node[-1]]
            target = args[args.index("--target") + 1]
            self.assertEqual(self.lab.name(node) + ":8443", target)
            self.assertNotEqual(node + ":8443", target)

    def test_snapshot_restore_cannot_join_live_network(self):
        for role in cluster.RESTORE_ROLES:
            item = self.container(role)
            self.inspect(role, item)
            with patch.object(cluster, "docker") as docker:
                with self.assertRaises(cluster.native.SmokeError):
                    self.lab.create_container(role, [], [], ["control"], publish=True)
            docker.assert_not_called()

    def test_failed_network_creation_keeps_pending_identity(self):
        self.lab.data["networks"].pop("restore")
        def docker(*args, **kwargs):
            return response() if args[:2] == ("network", "ls") else response(code=1)
        with patch.object(cluster, "docker", side_effect=docker):
            with self.assertRaises(cluster.native.SmokeError): self.lab.create_network("restore")
        self.assertIsNone(self.lab.data["networks"]["restore"]["id"])
        persisted = json.loads(self.lab.manifest.read_text())
        self.assertIn("restore", persisted["networks"])

    def test_lost_creation_reply_keeps_cidfile_for_recovery(self):
        self.lab.data["containers"].clear()
        def docker(*args, **kwargs):
            if args[0] == "ps": return response()
            if args[0] == "create":
                (self.root / "agent-a.cid").write_text(CID)
                raise cluster.native.SmokeError("lost create reply")
            raise AssertionError(args)
        with patch.object(self.lab, "inspect_network", return_value={}), \
                patch.object(cluster, "docker", side_effect=docker):
            with self.assertRaises(cluster.native.SmokeError):
                self.lab.create_container("agent-a", ["owned"], [], ["worker-a"])
        self.assertEqual(CID, cluster.creation_id(self.root / "agent-a.cid"))
        self.assertIsNone(self.lab.data["containers"]["agent-a"]["id"])

    def test_confirmed_absence_removes_only_stale_records(self):
        self.container()
        with patch.object(cluster, "docker", return_value=response()) as docker:
            self.lab.down()
        self.assertEqual({}, self.lab.data["containers"])
        self.assertEqual({}, self.lab.data["networks"])
        self.assertFalse(any(c.args[0] in ("stop", "rm") or c.args[:2] == ("network", "rm")
                             for c in docker.call_args_list))

    def test_unknown_creation_does_not_claim_cleanup_complete(self):
        self.container(partial=True)
        self.lab.data["networks"] = {}
        with patch.object(cluster, "docker") as docker:
            with self.assertRaises(cluster.native.SmokeError): self.lab.down()
        self.assertEqual("cleanup-incomplete", self.lab.data["status"])
        self.assertIn("agent-a", self.lab.data["containers"])
        docker.assert_not_called()

    def test_fault_parser_rejects_invalid_target_action_before_io(self):
        for args in (("--target", "agent-a", "--action", "block"),
                     ("--target", "proxy-a", "--action", "crash"),
                     ("--target", "unrelated", "--action", "crash")):
            with patch.object(cluster.sys, "argv", [str(SOURCE), "fault", *args]), \
                    patch.object(cluster, "Lab") as lab, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error: cluster.main()
                self.assertEqual(2, error.exception.code)
                lab.assert_not_called()

    def test_fault_symlink_preserves_unrelated_target(self):
        control = self.root / "proxy-a/control"
        control.mkdir(parents=True)
        target = self.root / "unrelated"
        target.write_text("preserved")
        (control / "blocked").symlink_to(target)
        with patch.object(self.lab, "inspect_container", return_value={}):
            with self.assertRaises(cluster.native.SmokeError): self.lab.fault("proxy-a", "block")
        self.assertEqual("preserved", target.read_text())

    def test_primary_gid_change_preserves_uid_ownership_and_creation_gid(self):
        item = self.container("scheduler")
        creation_gid = self.lab.data["gid"]
        self.lab.persist()
        with patch.object(cluster.os, "getgid", return_value=creation_gid + 100):
            reopened = cluster.Lab(self.root)
            self.assertEqual(creation_gid, reopened.data["gid"])
            with patch.object(cluster, "docker", return_value=response(json.dumps([item]))), \
                    patch.object(reopened, "inspect_network", return_value={}):
                self.assertEqual(CID, reopened.inspect_container("scheduler")["Id"])

    def test_operation_lock_refreshes_manifest(self):
        stale = cluster.Lab(self.root)
        self.addCleanup(stale.release)
        self.lab.data["status"] = "newer-state"
        self.lab.persist()
        stale.acquire()
        self.assertEqual("newer-state", stale.data["status"])
        competing = cluster.Lab(self.root)
        with self.assertRaises(BlockingIOError): competing.acquire()
        self.assertIsNone(competing.lock)


if __name__ == "__main__":
    unittest.main()
