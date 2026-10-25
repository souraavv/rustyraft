#!/usr/bin/env python3
"""Run local RustyRaft fault-injection tests with 3, 5, 7, or 15 nodes.

Run from the repository root:
    python3 tests/real-simulation/rustyraft.py --nodes 3
    python3 tests/real-simulation/rustyraft.py --nodes 15

Each run builds the binary, starts an isolated local cluster, submits a
command to the elected leader, waits for commitment, verifies command status
on every node, and checks that a follower rejects a write.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SUPPORTED_NODE_COUNTS = (3, 5, 7, 15)
DEFAULT_NODE_COUNT = 3
RAFT_PORT_BASE = 7000
HTTP_PORT_BASE = 3000
LEADER_TIMEOUT_SECONDS = 30.0
COMMIT_TIMEOUT_SECONDS = 20.0
HTTP_TIMEOUT_SECONDS = 1.5


class E2EFailure(RuntimeError):
    pass


def say(message: str) -> None:
    print(message, flush=True)


def find_repo_root(explicit: Optional[str]) -> Path:
    candidates = []
    if explicit:
        candidates.append(Path(explicit).expanduser().resolve())
    candidates.append(Path.cwd().resolve())
    candidates.extend(Path(__file__).resolve().parents)

    for candidate in candidates:
        if (
            (candidate / "Cargo.toml").is_file()
            and (candidate / "src" / "main.rs").is_file()
        ):
            try:
                manifest = (candidate / "Cargo.toml").read_text(
                    encoding="utf-8"
                )
            except OSError:
                continue
            if 'name = "rustyraft"' in manifest:
                return candidate
    raise E2EFailure(
        "Could not locate the rustyraft repository. Run this script from "
        "the repository root or pass --repo /path/to/rustyraft."
    )


def request_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Any, str]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(
            request, timeout=HTTP_TIMEOUT_SECONDS
        ) as response:
            raw = response.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                parsed = None
            return response.status, parsed, raw
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            parsed = None
        return error.code, parsed, raw
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return 0, None, str(error)


def find_value(value: Any, key: str) -> Any:
    """Find a field recursively, tolerating direct and wrapped JSON payloads."""
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for nested in value.values():
            found = find_value(nested, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = find_value(nested, key)
            if found is not None:
                return found
    return None


def port_is_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def check_ports_available(node_count: int) -> None:
    ports = [
        RAFT_PORT_BASE + i for i in range(1, node_count + 1)
    ]
    ports += [
        HTTP_PORT_BASE + i for i in range(1, node_count + 1)
    ]
    busy = [port for port in ports if not port_is_available(port)]
    if busy:
        raise E2EFailure(
            "These fixed RustyRaft test ports are already in use: "
            + ", ".join(str(port) for port in busy)
            + ". Stop the existing node processes and run the test again."
        )


def get_leader(base_url: str) -> Tuple[int, Optional[int], Any, str]:
    code, body, raw = request_json("GET", base_url + "/leader")
    leader_id = find_value(body, "leader_id") if body is not None else None
    known = find_value(body, "known") if body is not None else None
    if code == 200 and known is True and isinstance(leader_id, int):
        return code, leader_id, body, raw
    return code, None, body, raw


def status_snapshot(base_url: str, request_id: str) -> Tuple[int, Any, str]:
    encoded = urllib.parse.quote(request_id, safe="")
    return request_json("GET", base_url + "/commands/" + encoded)


def normalized_status(body: Any) -> Optional[str]:
    value = find_value(body, "status") if body is not None else None
    if isinstance(value, str):
        value = value.lower().replace("-", "_")
        # Accept the historical enum spelling while validating its meaning.
        return "committed" if value == "commited" else value
    return None


def normalized_log_index(body: Any) -> Optional[int]:
    value = find_value(body, "log_index") if body is not None else None
    if isinstance(value, int):
        return value
    return None


def tail(path: Path, line_count: int = 35) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-line_count:]) or "(no log output)"
    except OSError as error:
        return "(could not read log: " + str(error) + ")"


def stop_processes(
    processes: Dict[int, subprocess.Popen],
    logs: Dict[int, Any],
) -> None:
    for process in processes.values():
        if process.poll() is None:
            process.terminate()
    deadline = time.monotonic() + 4.0
    for process in processes.values():
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
    for stream in logs.values():
        try:
            stream.close()
        except Exception:
            pass


def wait_for_node_ports(
    node_id: int,
    timeout_seconds: float = 5.0,
) -> None:
    raft_port = RAFT_PORT_BASE + node_id
    http_port = HTTP_PORT_BASE + node_id
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if port_is_available(raft_port) and port_is_available(http_port):
            return
        time.sleep(0.1)

    raise E2EFailure(
        "Node {} ports did not become available: Raft {}, HTTP {}".format(
            node_id, raft_port, http_port
        )
    )


def stop_node(
    node_id: int,
    processes: Dict[int, subprocess.Popen],
    log_streams: Dict[int, Any],
) -> None:
    process = processes[node_id]
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=4.0)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass

    stream = log_streams.pop(node_id, None)
    if stream is not None:
        stream.close()


def start_node(
    binary: Path,
    node_id: int,
    node_count: int,
    data_root: Path,
    processes: Dict[int, subprocess.Popen],
    log_streams: Dict[int, Any],
    log_paths: Dict[int, Path],
) -> None:
    wait_for_node_ports(node_id)
    log_path = log_paths[node_id]
    stream = log_path.open("a", encoding="utf-8")
    process = subprocess.Popen(
        [str(binary), str(node_id), str(node_count)],
        cwd=str(data_root),
        stdout=stream,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    processes[node_id] = process
    log_streams[node_id] = stream
    say(
        "      Started node {} (HTTP {}, Raft {})".format(
            node_id, HTTP_PORT_BASE + node_id, RAFT_PORT_BASE + node_id
        )
    )


def wait_for_stable_leader(
    node_count: int,
    processes: Dict[int, subprocess.Popen],
    context: str,
) -> int:
    deadline = time.monotonic() + LEADER_TIMEOUT_SECONDS
    last_views: Dict[int, Any] = {}

    while time.monotonic() < deadline:
        exited = [
            node_id
            for node_id, process in processes.items()
            if process.poll() is not None
        ]
        if exited:
            raise E2EFailure(
                "{}: node process(es) exited: {}".format(
                    context, ", ".join(map(str, exited))
                )
            )

        views: Dict[int, int] = {}
        last_views = {}
        for node_id in range(1, node_count + 1):
            url = "http://127.0.0.1:{}".format(
                HTTP_PORT_BASE + node_id
            )
            code, reported, body, raw = get_leader(url)
            last_views[node_id] = {
                "http_status": code,
                "body": body if body is not None else raw,
            }
            if reported in range(1, node_count + 1):
                views[node_id] = reported

        values = list(views.values())
        if len(views) == node_count and len(set(values)) == 1:
            leader_id = values[0]
            say(
                "      PASS: cluster elected/confirmed leader node {}".format(
                    leader_id
                )
            )
            return leader_id

        time.sleep(0.25)

    raise E2EFailure(
        "{}: no stable leader within {} seconds. Last views: {}".format(
            context,
            int(LEADER_TIMEOUT_SECONDS),
            json.dumps(last_views, sort_keys=True),
        )
    )


def command_status_on_all_nodes(
    node_count: int,
    request_id: str,
    expected_index: int,
) -> Tuple[bool, Dict[str, Any]]:
    results: Dict[str, Any] = {}
    passed = True

    for node_id in range(1, node_count + 1):
        url = "http://127.0.0.1:{}".format(
            HTTP_PORT_BASE + node_id
        )
        code, body, raw = status_snapshot(url, request_id)
        status = normalized_status(body)
        index = normalized_log_index(body)
        results[str(node_id)] = {
            "http_status": code,
            "status": status,
            "log_index": index,
            "body": body if body is not None else raw,
        }
        if not (
            code == 200
            and status == "committed"
            and index == expected_index
        ):
            passed = False

    return passed, results


def wait_for_commands_on_all_nodes(
    node_count: int,
    commands: List[Dict[str, Any]],
    processes: Dict[int, subprocess.Popen],
    timeout_seconds: float,
) -> Tuple[bool, Dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    latest: Dict[str, Any] = {}

    while time.monotonic() < deadline:
        all_passed = True
        latest = {}
        for command in commands:
            request_id = command["request_id"]
            expected_index = command["log_index"]
            passed, node_results = command_status_on_all_nodes(
                node_count, request_id, expected_index
            )
            latest[request_id] = {
                "expected_log_index": expected_index,
                "nodes": node_results,
                "passed": passed,
            }
            if not passed:
                all_passed = False

        exited = [
            node_id
            for node_id, process in processes.items()
            if process.poll() is not None
        ]
        if exited:
            raise E2EFailure(
                "Node process(es) exited during command verification: "
                + ", ".join(map(str, exited))
            )

        if all_passed:
            return True, latest
        time.sleep(0.25)

    return False, latest


def submit_and_verify_command(
    node_count: int,
    leader_id: int,
    request_id: str,
    command: str,
    processes: Dict[int, subprocess.Popen],
) -> Dict[str, Any]:
    leader_url = "http://127.0.0.1:{}".format(
        HTTP_PORT_BASE + leader_id
    )
    code, body, raw = request_json(
        "POST",
        leader_url + "/commands",
        {"request_id": request_id, "command": command},
    )
    if code not in (200, 202):
        raise E2EFailure(
            "Leader rejected command: HTTP {} body={!r}".format(
                code, body if body is not None else raw
            )
        )

    returned_id = find_value(body, "request_id") if body is not None else None
    if returned_id is not None and returned_id != request_id:
        raise E2EFailure(
            "Unexpected request ID in response: {!r}".format(returned_id)
        )

    say("      PASS: POST /commands returned HTTP {}".format(code))
    deadline = time.monotonic() + COMMIT_TIMEOUT_SECONDS
    last_status = None

    while time.monotonic() < deadline:
        if processes[leader_id].poll() is not None:
            raise E2EFailure(
                "Leader process exited while waiting for command commit."
            )
        status_code, status_body, status_raw = status_snapshot(
            leader_url, request_id
        )
        last_status = {
            "http_status": status_code,
            "body": status_body if status_body is not None else status_raw,
        }
        if (
            status_code == 200
            and normalized_status(status_body) == "committed"
        ):
            break
        time.sleep(0.25)
    else:
        raise E2EFailure(
            "Command {} did not commit on leader: {}".format(
                request_id, json.dumps(last_status, sort_keys=True)
            )
        )

    log_index = normalized_log_index(status_body)
    if log_index is None:
        raise E2EFailure(
            "Committed response has no integer log_index: {}".format(
                json.dumps(status_body, sort_keys=True)
            )
        )

    record = {"request_id": request_id, "log_index": log_index}
    passed, details = wait_for_commands_on_all_nodes(
        node_count, [record], processes, COMMIT_TIMEOUT_SECONDS
    )
    if not passed:
        raise E2EFailure(
            "Command was not committed on all nodes: "
            + json.dumps(details, sort_keys=True)
        )

    say(
        "      PASS: request {} committed at log index {}".format(
            request_id, log_index
        )
    )
    say(
        "      PASS: all {} nodes report committed at log index {}".format(
            node_count, log_index
        )
    )
    return record



def wait_for_stable_leader_subset(
    node_count: int,
    node_ids: List[int],
    processes: Dict[int, subprocess.Popen],
    context: str,
) -> int:
    """Wait for an agreed leader among the live quorum/subset."""
    expected = sorted(node_ids)
    deadline = time.monotonic() + LEADER_TIMEOUT_SECONDS
    stable_samples = 0
    last_views: Dict[int, Any] = {}

    while time.monotonic() < deadline:
        exited = [
            node_id for node_id in expected
            if processes[node_id].poll() is not None
        ]
        if exited:
            raise E2EFailure(
                "{}: expected node(s) exited: {}".format(context, exited)
            )

        views = {}
        for node_id in expected:
            code, reported, body, raw = get_leader(
                "http://127.0.0.1:{}".format(HTTP_PORT_BASE + node_id)
            )
            views[node_id] = {
                "http_status": code,
                "leader_id": reported,
                "body": body if body is not None else raw,
            }
        last_views = views
        leaders = [view["leader_id"] for view in views.values()]
        if (
            len(leaders) == len(expected)
            and None not in leaders
            and len(set(leaders)) == 1
            and leaders[0] in expected
        ):
            stable_samples += 1
            if stable_samples >= 3:
                say(
                    "      PASS: {}; leader {} is stable on nodes {}".format(
                        context, leaders[0], expected
                    )
                )
                return leaders[0]
        else:
            stable_samples = 0
        time.sleep(0.25)

    raise E2EFailure(
        "{}: no stable leader for nodes {}. Last views: {}".format(
            context, expected, json.dumps(last_views, sort_keys=True)
        )
    )


def hard_kill_nodes(
    node_ids: List[int],
    processes: Dict[int, subprocess.Popen],
    log_streams: Dict[int, Any],
) -> None:
    """Kill selected nodes before waiting, to minimize fault delay."""
    for node_id in node_ids:
        process = processes[node_id]
        if process.poll() is None:
            process.kill()

    for node_id in node_ids:
        process = processes[node_id]
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass
        stream = log_streams.pop(node_id, None)
        if stream is not None:
            stream.close()

    for node_id in node_ids:
        wait_for_node_ports(node_id)
        say("      Hard-crashed node {}".format(node_id))


def command_status_on_nodes(
    node_ids: List[int],
    request_id: str,
) -> Dict[int, Dict[str, Any]]:
    """Query nodes concurrently so 15-node runs avoid serialized HTTP."""
    def query(node_id: int) -> Tuple[int, Dict[str, Any]]:
        url = "http://127.0.0.1:{}".format(HTTP_PORT_BASE + node_id)
        code, body, raw = status_snapshot(url, request_id)
        return node_id, {
            "http_status": code,
            "status": normalized_status(body),
            "log_index": normalized_log_index(body),
            "body": body if body is not None else raw,
        }

    results = {}
    with ThreadPoolExecutor(max_workers=min(32, len(node_ids))) as executor:
        for node_id, result in executor.map(query, node_ids):
            results[node_id] = result
    return results


def wait_for_commands_on_nodes(
    commands: List[Dict[str, Any]],
    node_ids: List[int],
    processes: Dict[int, subprocess.Popen],
    timeout_seconds: float = COMMIT_TIMEOUT_SECONDS,
) -> Tuple[bool, Dict[str, Any]]:
    """Wait for committed IDs to converge on the requested node subset."""
    deadline = time.monotonic() + timeout_seconds
    latest: Dict[str, Any] = {}

    while time.monotonic() < deadline:
        exited = [
            node_id for node_id in node_ids
            if processes[node_id].poll() is not None
        ]
        if exited:
            raise E2EFailure(
                "Node(s) exited during verification: {}".format(exited)
            )

        latest = {}
        all_passed = True
        for command in commands:
            request_id = command["request_id"]
            expected_index = command["log_index"]
            observed = command_status_on_nodes(node_ids, request_id)
            passed = all(
                result["http_status"] == 200
                and result["status"] == "committed"
                and result["log_index"] == expected_index
                for result in observed.values()
            )
            latest[request_id] = {
                "expected_log_index": expected_index,
                "passed": passed,
                "nodes": observed,
            }
            if not passed:
                all_passed = False

        if all_passed:
            return True, latest
        time.sleep(0.25)

    return False, latest


def submit_and_verify_on_nodes(
    leader_id: int,
    request_id: str,
    command: str,
    node_ids: List[int],
    processes: Dict[int, subprocess.Popen],
) -> Dict[str, Any]:
    """Submit and verify one command while some cluster nodes are offline."""
    leader_url = "http://127.0.0.1:{}".format(HTTP_PORT_BASE + leader_id)
    code, body, raw = request_json(
        "POST",
        leader_url + "/commands",
        {"request_id": request_id, "command": command},
    )
    if code not in (200, 202):
        raise E2EFailure(
            "Leader rejected request {}: HTTP {} body={!r}".format(
                request_id, code, body if body is not None else raw
            )
        )

    index = normalized_log_index(body)
    if index is None:
        raise E2EFailure(
            "Request {} response lacks log_index: {!r}".format(
                request_id, body if body is not None else raw
            )
        )

    record = {"request_id": request_id, "log_index": index}
    passed, details = wait_for_commands_on_nodes(
        [record], node_ids, processes
    )
    if not passed:
        raise E2EFailure(
            "Request {} failed to commit on nodes {}: {}".format(
                request_id, node_ids, json.dumps(details, sort_keys=True)
            )
        )
    say(
        "      PASS: {} committed at index {} on nodes {}".format(
            request_id, index, node_ids
        )
    )
    return record


def verify_api_contract(leader_url: str) -> None:
    say("[API] Checking validation and missing request status...")
    code, body, raw = request_json(
        "POST",
        leader_url + "/commands",
        {"request_id": "", "command": "INVALID_EMPTY_REQUEST_ID"},
    )
    if code != 400:
        raise E2EFailure(
            "Empty request_id should return HTTP 400; got {} body={!r}"
            .format(code, body if body is not None else raw)
        )

    missing_id = "missing-{}-{}".format(int(time.time()), os.getpid())
    code, body, raw = status_snapshot(leader_url, missing_id)
    if code != 404:
        raise E2EFailure(
            "Unknown request status should return HTTP 404; got {} "
            "body={!r}".format(code, body if body is not None else raw)
        )
    say("      PASS: empty request ID returns 400")
    say("      PASS: unknown request status returns 404")


def verify_idempotent_retry(
    leader_url: str,
    record: Dict[str, Any],
) -> None:
    """Same request ID and payload must return its existing log entry."""
    code, body, raw = request_json(
        "POST",
        leader_url + "/commands",
        {
            "request_id": record["request_id"],
            "command": record.get("command", ""),
        },
    )
    if code not in (200, 202):
        raise E2EFailure(
            "Retry for {} failed: HTTP {} body={!r}".format(
                record["request_id"],
                code,
                body if body is not None else raw,
            )
        )
    retry_index = normalized_log_index(body)
    if retry_index != record["log_index"]:
        raise E2EFailure(
            "Retry for {} changed log index: expected {}, got {!r}".format(
                record["request_id"], record["log_index"], body
            )
        )
    say("      PASS: duplicate retry kept log index {}".format(retry_index))


def submit_concurrent_burst(
    node_count: int,
    leader_id: int,
    processes: Dict[int, subprocess.Popen],
    count: int,
) -> List[Dict[str, Any]]:
    """Submit many unique client requests concurrently to the leader."""
    say("[STRESS] Submitting {} concurrent client writes...".format(count))
    leader_url = "http://127.0.0.1:{}".format(HTTP_PORT_BASE + leader_id)
    suffix = "{}-{}".format(int(time.time()), os.getpid())
    requests = [
        (
            "burst-{}-{}".format(suffix, sequence),
            "BURST_COMMAND_{}".format(sequence),
        )
        for sequence in range(count)
    ]

    def submit_one(item: Tuple[str, str]) -> Tuple[str, str, int, Any, str]:
        request_id, command = item
        code, body, raw = request_json(
            "POST",
            leader_url + "/commands",
            {"request_id": request_id, "command": command},
        )
        return request_id, command, code, body, raw

    records = []
    with ThreadPoolExecutor(max_workers=min(32, count)) as executor:
        results = list(executor.map(submit_one, requests))

    for request_id, command, code, body, raw in results:
        if code not in (200, 202):
            raise E2EFailure(
                "Concurrent request {} failed: HTTP {} body={!r}".format(
                    request_id, code, body if body is not None else raw
                )
            )
        returned_id = find_value(body, "request_id")
        if returned_id not in (None, request_id):
            raise E2EFailure(
                "Unexpected request ID: expected {}, got {!r}".format(
                    request_id, body
                )
            )
        index = normalized_log_index(body)
        if index is None:
            raise E2EFailure(
                "Concurrent response for {} lacks log_index: {!r}".format(
                    request_id, body
                )
            )
        records.append({
            "request_id": request_id,
            "command": command,
            "log_index": index,
        })

    indexes = [record["log_index"] for record in records]
    if len(indexes) != len(set(indexes)):
        raise E2EFailure(
            "Unique concurrent requests received duplicate log indexes: {}"
            .format(indexes)
        )

    passed, details = wait_for_commands_on_nodes(
        records,
        list(range(1, node_count + 1)),
        processes,
    )
    if not passed:
        raise E2EFailure(
            "Concurrent writes failed on every node: {}".format(
                json.dumps(details, sort_keys=True)
            )
        )
    say(
        "      PASS: {} concurrent writes have unique indexes and committed "
        "on all nodes".format(count)
    )
    return records


def stress_follower_crash(
    node_count: int,
    leader_id: int,
    command_records: List[Dict[str, Any]],
    processes: Dict[int, subprocess.Popen],
    log_streams: Dict[int, Any],
    log_paths: Dict[int, Path],
) -> int:
    """Kill one follower, continue with quorum, and recover the follower."""
    follower_id = next(
        node_id for node_id in range(1, node_count + 1)
        if node_id != leader_id
    )
    say("[FAULT] Hard-crashing follower {}...".format(follower_id))
    hard_kill_nodes([follower_id], processes, log_streams)
    live = [
        node_id for node_id in range(1, node_count + 1)
        if processes[node_id].poll() is None
    ]
    leader_id = wait_for_stable_leader_subset(
        node_count, live, processes, "majority after follower crash"
    )
    request_id = "follower-down-{}-{}".format(int(time.time()), os.getpid())
    record = submit_and_verify_on_nodes(
        leader_id,
        request_id,
        "COMMIT_WHILE_FOLLOWER_DOWN",
        live,
        processes,
    )
    command_records.append(record)

    say("[RECOVERY] Restarting follower {}...".format(follower_id))
    data_root = log_paths[leader_id].parent
    binary = Path(processes[leader_id].args[0])
    start_node(
        binary, follower_id, node_count, data_root,
        processes, log_streams, log_paths,
    )
    leader_id = wait_for_stable_leader(
        node_count, processes, "follower rejoin"
    )
    passed, details = wait_for_commands_on_nodes(
        command_records, list(range(1, node_count + 1)), processes
    )
    if not passed:
        raise E2EFailure(
            "Committed prefix failed after follower rejoin: {}".format(
                json.dumps(details, sort_keys=True)
            )
        )
    say("      PASS: follower recovered the committed log prefix")
    return leader_id


def stress_leader_crash(
    node_count: int,
    leader_id: int,
    command_records: List[Dict[str, Any]],
    processes: Dict[int, subprocess.Popen],
    log_streams: Dict[int, Any],
    log_paths: Dict[int, Path],
    round_number: int,
) -> int:
    """Crash the leader, commit with a majority, then rejoin the old leader."""
    old_leader = leader_id
    say("[FAULT] Hard-crashing leader {}...".format(old_leader))
    hard_kill_nodes([old_leader], processes, log_streams)
    live = [
        node_id for node_id in range(1, node_count + 1)
        if processes[node_id].poll() is None
    ]
    new_leader = wait_for_stable_leader_subset(
        node_count,
        live,
        processes,
        "leader failover round {}".format(round_number),
    )

    passed, details = wait_for_commands_on_nodes(
        command_records, live, processes
    )
    if not passed:
        raise E2EFailure(
            "Committed commands were lost after leader crash: {}".format(
                json.dumps(details, sort_keys=True)
            )
        )

    request_id = "leader-down-{}-{}-{}".format(
        round_number, int(time.time()), os.getpid()
    )
    record = submit_and_verify_on_nodes(
        new_leader,
        request_id,
        "COMMIT_AFTER_LEADER_CRASH_{}".format(round_number),
        live,
        processes,
    )
    command_records.append(record)

    say("[RECOVERY] Restarting former leader {}...".format(old_leader))
    data_root = log_paths[new_leader].parent
    binary = Path(processes[new_leader].args[0])
    start_node(
        binary, old_leader, node_count, data_root,
        processes, log_streams, log_paths,
    )
    leader_id = wait_for_stable_leader(
        node_count, processes, "former leader rejoin"
    )
    passed, details = wait_for_commands_on_nodes(
        command_records, list(range(1, node_count + 1)), processes
    )
    if not passed:
        raise E2EFailure(
            "Committed prefix failed after leader rejoin: {}".format(
                json.dumps(details, sort_keys=True)
            )
        )
    say("      PASS: old and new leaders agree on committed history")
    return leader_id


def verify_no_quorum_safety(
    node_count: int,
    leader_id: int,
    command_records: List[Dict[str, Any]],
    processes: Dict[int, subprocess.Popen],
    log_streams: Dict[int, Any],
    log_paths: Dict[int, Path],
) -> int:
    """Keep a minority alive and ensure writes remain uncommitted."""
    minority_size = node_count // 2
    others = [
        node_id for node_id in range(1, node_count + 1)
        if node_id != leader_id
    ]
    survivors = [leader_id] + others[:minority_size - 1]
    removed = [
        node_id for node_id in range(1, node_count + 1)
        if node_id not in survivors
    ]
    say(
        "[QUORUM] Leaving minority {} alive; crash majority {}...".format(
            survivors, removed
        )
    )
    hard_kill_nodes(removed, processes, log_streams)

    live_minority = [
        node_id for node_id in survivors
        if processes[node_id].poll() is None
    ]
    if len(live_minority) >= node_count // 2 + 1:
        raise E2EFailure("No-quorum fault did not leave a strict minority.")

    request_id = "no-quorum-{}-{}".format(int(time.time()), os.getpid())
    code, body, raw = request_json(
        "POST",
        "http://127.0.0.1:{}/commands".format(HTTP_PORT_BASE + leader_id),
        {"request_id": request_id, "command": "MUST_NOT_COMMIT_NO_QUORUM"},
    )
    if code not in (200, 202):
        raise E2EFailure(
            "Could not submit no-quorum probe before leader stepped down: "
            "HTTP {} body={!r}".format(
                code, body if body is not None else raw
            )
        )
    if normalized_status(body) == "committed":
        raise E2EFailure(
            "No-quorum request was immediately reported committed: {!r}"
            .format(body)
        )

    say("      Observing no-quorum request for 3 seconds...")
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        observed = command_status_on_nodes(survivors, request_id)
        committed = {
            node_id: result for node_id, result in observed.items()
            if result["status"] == "committed"
        }
        if committed:
            raise E2EFailure(
                "Raft committed a command without quorum: {}".format(
                    json.dumps(committed, sort_keys=True)
                )
            )
        time.sleep(0.25)
    say("      PASS: no command committed without a majority")

    say("[QUORUM] Restoring the stopped majority...")
    data_root = log_paths[survivors[0]].parent
    binary = Path(processes[survivors[0]].args[0])
    for node_id in removed:
        start_node(
            binary, node_id, node_count, data_root,
            processes, log_streams, log_paths,
        )
    leader_id = wait_for_stable_leader(
        node_count, processes, "quorum restoration"
    )
    passed, details = wait_for_commands_on_nodes(
        command_records, list(range(1, node_count + 1)), processes
    )
    if not passed:
        raise E2EFailure(
            "Committed history failed after quorum restoration: {}".format(
                json.dumps(details, sort_keys=True)
            )
        )
    return leader_id


def audit_election_safety(log_paths: Dict[int, Path]) -> None:
    """Fail if different node IDs announce leadership in the same term."""
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    pattern = re.compile(
        r"Raft node became leader.*?server_id=(\d+).*?term=(\d+)"
    )
    leaders_by_term: Dict[int, Set[int]] = {}

    for node_id, path in sorted(log_paths.items()):
        lines = path.read_text(
            encoding="utf-8",
            errors="replace",
        ).splitlines()
        for line in lines:
            clean_line = ansi.sub("", line)
            match = pattern.search(clean_line)
            if match is None:
                continue
            server_id = int(match.group(1))
            term = int(match.group(2))
            leaders_by_term.setdefault(term, set()).add(server_id)

    if not leaders_by_term:
        raise E2EFailure(
            "Cannot audit election safety: no leader events found in logs."
        )
    violations = {
        term: sorted(server_ids)
        for term, server_ids in leaders_by_term.items()
        if len(server_ids) > 1
    }
    if violations:
        raise E2EFailure(
            "Election safety violation: multiple leaders in one term: "
            + json.dumps(violations, sort_keys=True)
        )
    say(
        "      PASS: election logs show no term with multiple leaders "
        "({} terms observed)".format(len(leaders_by_term))
    )

def run_test(repo: Path, node_count: int) -> int:
    binary = repo / "target" / "debug" / "rustyraft"
    say("[1/12] Building rustyraft...")
    build = subprocess.run(
        ["cargo", "build", "--bin", "rustyraft"],
        cwd=str(repo),
        text=True,
    )
    if build.returncode != 0:
        raise E2EFailure("cargo build failed; fix the build errors above.")
    if not binary.is_file():
        raise E2EFailure(
            "Build succeeded but target/debug/rustyraft was not found."
        )
    say("      PASS: binary built")

    check_ports_available(node_count)
    processes: Dict[int, subprocess.Popen] = {}
    log_streams: Dict[int, Any] = {}
    log_paths: Dict[int, Path] = {}
    temp_root = Path(tempfile.mkdtemp(prefix="rustyraft-e2e-"))

    try:
        say(
            "[2/12] Starting {} isolated node processes...".format(
                node_count
            )
        )
        for node_id in range(1, node_count + 1):
            log_path = temp_root / ("node{}.log".format(node_id))
            stream = log_path.open("w", encoding="utf-8")
            log_streams[node_id] = stream
            log_paths[node_id] = log_path
            process = subprocess.Popen(
                [str(binary), str(node_id), str(node_count)],
                cwd=str(temp_root),
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            processes[node_id] = process
            say("      Started node {} (HTTP {}, Raft {})".format(
                node_id, HTTP_PORT_BASE + node_id, RAFT_PORT_BASE + node_id
            ))

        say("[3/12] Waiting for all nodes to agree on a leader...")
        deadline = time.monotonic() + LEADER_TIMEOUT_SECONDS
        leader_id = None
        last_leader_views: Dict[int, Any] = {}
        while time.monotonic() < deadline:
            exited = [
                node_id for node_id, process in processes.items()
                if process.poll() is not None
            ]
            if exited:
                raise E2EFailure(
                    "Node process(es) exited unexpectedly: "
                    + ", ".join(map(str, exited))
                )

            views: Dict[int, int] = {}
            last_leader_views = {}
            for node_id in range(1, node_count + 1):
                code, reported, body, raw = get_leader(
                    "http://127.0.0.1:{}".format(HTTP_PORT_BASE + node_id)
                )
                last_leader_views[node_id] = {
                    "http_status": code,
                    "body": body if body is not None else raw,
                }
                if reported in range(1, node_count + 1):
                    views[node_id] = reported

            values = list(views.values())
            if len(views) == node_count and len(set(values)) == 1:
                leader_id = values[0]
                break
            time.sleep(0.25)

        if leader_id is None:
            raise E2EFailure(
                (
                    "No stable leader was reported by all {} nodes within "
                    "{} seconds. Last views: {}"
                ).format(
                    node_count,
                    int(LEADER_TIMEOUT_SECONDS),
                    json.dumps(last_leader_views, sort_keys=True),
                )
            )
        say("      PASS: node {} is leader; all nodes agree".format(leader_id))

        say("[4/12] Submitting the initial command...")
        request_id = "e2e-{}-{}".format(int(time.time()), os.getpid())
        command = "E2E_COMMAND_{}".format(request_id)
        leader_url = "http://127.0.0.1:{}".format(HTTP_PORT_BASE + leader_id)
        code, body, raw = request_json(
            "POST",
            leader_url + "/commands",
            {"request_id": request_id, "command": command},
        )
        if code not in (200, 202):
            raise E2EFailure(
                "Leader rejected command submission: HTTP {} body={!r}".format(
                    code, body if body is not None else raw
                )
            )
        returned_id = (
            find_value(body, "request_id") if body is not None else None
        )
        if returned_id is not None and returned_id != request_id:
            raise E2EFailure(
                "Submission response had unexpected request_id: {!r}".format(
                    returned_id
                )
            )
        say("      PASS: POST /commands returned HTTP {}".format(code))

        say(
            "[5/12] Waiting for command commit on the leader..."
        )
        commit_deadline = time.monotonic() + COMMIT_TIMEOUT_SECONDS
        leader_body = None
        leader_index = None
        last_status_response = None
        while time.monotonic() < commit_deadline:
            if processes[leader_id].poll() is not None:
                raise E2EFailure(
                    "Leader process exited while waiting for commit."
                )
            status_code, status_body, status_raw = status_snapshot(
                leader_url, request_id
            )
            last_status_response = {
                "http_status": status_code,
                "body": status_body if status_body is not None else status_raw,
            }
            if (
                status_code == 200
                and normalized_status(status_body) == "committed"
            ):
                leader_body = status_body
                leader_index = normalized_log_index(status_body)
                break
            time.sleep(0.25)

        if leader_body is None:
            raise E2EFailure(
                "Command did not reach committed status within {} seconds. "
                "Last response: {}".format(
                    int(COMMIT_TIMEOUT_SECONDS),
                    json.dumps(last_status_response, sort_keys=True),
                )
            )
        if leader_index is None:
            raise E2EFailure(
                (
                    "Leader reported committed but response had no "
                    "integer log_index: "
                )
                + json.dumps(leader_body, sort_keys=True)
            )
        say("      PASS: request {} committed at log index {}".format(
            request_id, leader_index
        ))

        say("[5/12] Verifying replicated command status on every node...")
        replication_deadline = time.monotonic() + COMMIT_TIMEOUT_SECONDS
        node_results: Dict[int, Any] = {}
        while time.monotonic() < replication_deadline:
            all_committed = True
            node_results = {}
            for node_id in range(1, node_count + 1):
                url = "http://127.0.0.1:{}".format(HTTP_PORT_BASE + node_id)
                status_code, status_body, status_raw = status_snapshot(
                    url, request_id
                )
                state = normalized_status(status_body)
                index = normalized_log_index(status_body)
                node_results[node_id] = {
                    "http_status": status_code,
                    "status": state,
                    "log_index": index,
                    "body": (
                        status_body
                        if status_body is not None
                        else status_raw
                    ),
                }
                if not (
                    status_code == 200
                    and state == "committed"
                    and index == leader_index
                ):
                    all_committed = False
            if all_committed:
                break
            time.sleep(0.25)

        failed_nodes = {
            node_id: result for node_id, result in node_results.items()
            if not (
                result["http_status"] == 200
                and result["status"] == "committed"
                and result["log_index"] == leader_index
            )
        }
        if failed_nodes:
            raise E2EFailure(
                "Command was not reported committed at log index {} "
                "on all nodes. Node results: {}".format(
                    leader_index, json.dumps(failed_nodes, sort_keys=True)
                )
            )
        say(
            (
                "      PASS: all {} nodes report committed at "
                "log index {}"
            ).format(node_count, leader_index)
        )
        command_records = [
            {
                "request_id": request_id,
                "log_index": leader_index,
                "command": command,
            }
        ]

        verify_api_contract(leader_url)
        verify_idempotent_retry(leader_url, command_records[0])

        say("[6/12] Verifying that a follower rejects writes...")
        follower_id = next(node_id for node_id in range(1, node_count + 1)
                           if node_id != leader_id)
        follower_url = "http://127.0.0.1:{}".format(
            HTTP_PORT_BASE + follower_id
        )
        reject_id = request_id + "-follower-write"
        reject_code, reject_body, reject_raw = request_json(
            "POST",
            follower_url + "/commands",
            {"request_id": reject_id, "command": "MUST_NOT_BE_ACCEPTED"},
        )
        if reject_code != 409:
            raise E2EFailure(
                "Expected follower node {} to reject POST with HTTP 409; got "
                "HTTP {} body={!r}".format(
                    follower_id,
                    reject_code,
                    reject_body if reject_body is not None else reject_raw,
                )
            )
        say(
            "      PASS: follower node {} returned HTTP 409".format(
                follower_id
            )
        )

        say(
            "[7/12] Restarting follower {} while other nodes stay up...".format(
                follower_id
            )
        )
        stop_node(follower_id, processes, log_streams)
        say("      Stopped follower {}".format(follower_id))
        start_node(
            binary, follower_id, node_count, temp_root,
            processes, log_streams, log_paths,
        )
        say("      Waiting for the restarted node to rejoin...")
        leader_id = wait_for_stable_leader(
            node_count, processes, "Single-node restart"
        )

        say(
            "[8/12] Checking old commands and submitting a fresh command..."
        )
        recovered, single_details = wait_for_commands_on_all_nodes(
            node_count,
            command_records,
            processes,
            COMMIT_TIMEOUT_SECONDS,
        )
        if not recovered:
            raise E2EFailure(
                "A committed command did not recover after single-node "
                "restart: " + json.dumps(single_details, sort_keys=True)
            )
        say("      PASS: old command remains committed on all nodes")

        second_id = "e2e-after-node-restart-{}-{}".format(
            int(time.time()), os.getpid()
        )
        second = submit_and_verify_command(
            node_count,
            leader_id,
            second_id,
            "E2E_AFTER_NODE_RESTART_{}".format(second_id),
            processes,
        )
        command_records.append(second)

        burst_count = min(30, max(12, node_count * 2))
        command_records.extend(
            submit_concurrent_burst(
                node_count,
                leader_id,
                processes,
                burst_count,
            )
        )

        leader_id = stress_follower_crash(
            node_count,
            leader_id,
            command_records,
            processes,
            log_streams,
            log_paths,
        )
        leader_id = stress_leader_crash(
            node_count,
            leader_id,
            command_records,
            processes,
            log_streams,
            log_paths,
            round_number=1,
        )

        leader_id = verify_no_quorum_safety(
            node_count,
            leader_id,
            command_records,
            processes,
            log_streams,
            log_paths,
        )

        failover_rounds = 2 if node_count < 15 else 3
        for round_number in range(2, failover_rounds + 2):
            leader_id = stress_leader_crash(
                node_count,
                leader_id,
                command_records,
                processes,
                log_streams,
                log_paths,
                round_number=round_number,
            )

        say("[9/12] Hard-crashing all {} nodes...".format(node_count))
        hard_kill_nodes(
            list(range(1, node_count + 1)),
            processes,
            log_streams,
        )
        for node_id in range(1, node_count + 1):
            wait_for_node_ports(node_id)
        say("      PASS: all node processes stopped")

        say(
            "[10/12] Restarting all nodes with the same persistent data..."
        )
        for node_id in range(1, node_count + 1):
            start_node(
                binary, node_id, node_count, temp_root,
                processes, log_streams, log_paths,
            )
        leader_id = wait_for_stable_leader(
            node_count, processes, "Full-cluster restart"
        )

        say(
            "[11/12] Checking old commands before any new client write..."
        )
        recovery_ok, recovery_details = wait_for_commands_on_all_nodes(
            node_count,
            command_records,
            processes,
            COMMIT_TIMEOUT_SECONDS,
        )
        if recovery_ok:
            say(
                "      PASS: all earlier commands recovered as committed"
            )
        else:
            say(
                "      FAIL: old commands remain pending after full "
                "cluster restart"
            )
            say(
                "      Recovery details: {}".format(
                    json.dumps(recovery_details, sort_keys=True)
                )
            )

        say("[12/12] Submitting a fresh command after full restart...")
        third_id = "e2e-after-full-restart-{}-{}".format(
            int(time.time()), os.getpid()
        )
        third = submit_and_verify_command(
            node_count,
            leader_id,
            third_id,
            "E2E_AFTER_FULL_RESTART_{}".format(third_id),
            processes,
        )
        command_records.append(third)
        audit_election_safety(log_paths)

        say("")
        say("========================================")
        say("E2E RESULT: {}".format("PASS" if recovery_ok else "FAIL"))
        say("Cluster size:             {}".format(node_count))
        say("Last leader node:          {}".format(leader_id))
        say("Commands submitted:        {}".format(len(command_records)))
        for record in command_records:
            say(
                "  {} -> log index {}".format(
                    record["request_id"], record["log_index"]
                )
            )
        say("Replicas verified:         {}/{}".format(
            node_count, node_count
        ))
        say("Follower rejection:        PASS")
        say("Concurrent write stress:   PASS")
        say("Follower hard-crash test:  PASS")
        say("Leader failover tests:     PASS")
        say("No-quorum commit safety:   PASS")
        say("Election-safety log audit: PASS")
        say("Single-node restart check: PASS")
        say(
            "Full-cluster recovery:     {}".format(
                "PASS" if recovery_ok else "FAIL"
            )
        )
        say("Fresh post-restart write:  PASS")
        say("Node logs/data:             {}".format(temp_root))
        say("========================================")

        if not recovery_ok:
            failed = [
                request_id
                for request_id, details in recovery_details.items()
                if not details["passed"]
            ]
            raise E2EFailure(
                "Commands {} did not recover before the new client write."
                .format(", ".join(failed))
            )
        return 0

    except Exception:
        say("")
        say("E2E RESULT: FAIL")
        say("Logs are available in: {}".format(temp_root))
        for node_id, log_path in log_paths.items():
            say("\n--- node {} log (last lines) ---".format(node_id))
            say(tail(log_path))
        raise
    finally:
        stop_processes(processes, log_streams)
        # Preserve logs for inspection; only isolated node data is temporary.
        # The temporary directory is intentionally not removed.


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run RustyRaft fault-injection tests for 3, 5, 7, or 15 nodes."
        )
    )
    parser.add_argument(
        "--repo",
        help=(
            "Path to the rustyraft repository (defaults to current directory)."
        ),
    )
    parser.add_argument(
        "--nodes",
        type=int,
        choices=SUPPORTED_NODE_COUNTS,
        default=DEFAULT_NODE_COUNT,
        help="Cluster size (3, 5, 7, or 15 nodes).",
    )
    args = parser.parse_args()

    try:
        repo = find_repo_root(args.repo)
        node_count = args.nodes
        say("RustyRaft real-simulation test")
        say("Repository: {}".format(repo))
        say("Cluster size: {} nodes".format(node_count))
        say(
            "Ports: Raft {}-{}, HTTP {}-{}".format(
                RAFT_PORT_BASE + 1,
                RAFT_PORT_BASE + node_count,
                HTTP_PORT_BASE + 1,
                HTTP_PORT_BASE + node_count,
            )
        )
        return run_test(repo, node_count)
    except KeyboardInterrupt:
        say("\nInterrupted; terminating test.")
        return 130
    except E2EFailure as error:
        say("\nFAIL: {}".format(error))
        return 1
    except Exception as error:
        say("\nFAIL: unexpected error: {}".format(error))
        return 1


if __name__ == "__main__":
    sys.exit(main())