"""Phase 4 bufferbloat experiments through the user-space router."""

import argparse
import csv
import hashlib
import ipaddress
import json
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"
PLOT_SCRIPT = REPO_ROOT / "scripts" / "plot_bufferbloat.py"
DEFAULT_RESULTS_ROOT = REPO_ROOT / "results" / "phase4"

PYTHON = sys.executable

MININET_PLAN = {
    "router_sender_face": ("10.0.0.2", 5001),
    "router_receiver_face": ("10.0.1.2", 5001),
    "receiver_bind": ("0.0.0.0", 5001),
    "receiver_target": ("10.0.1.1", 5001),
    "sender_target": ("10.0.0.2", 5001),
}

LOOPBACK_PLAN = {
    "router_sender_face": ("127.0.0.1", 5001),
    "router_receiver_face": ("127.0.0.1", 5003),
    "receiver_bind": ("127.0.0.1", 5002),
    "receiver_target": ("127.0.0.1", 5002),
    "sender_target": ("127.0.0.1", 5001),
}

RELAY_PLAN = {
    "relay1_listen": ("127.0.0.1", 5001),
    "relay1_forward": ("127.0.0.1", 5011),
    "router_sender_face": ("127.0.0.1", 5011),
    "router_receiver_face": ("127.0.0.1", 5013),
    "relay2_listen": ("127.0.0.1", 5021),
    "relay2_forward": ("127.0.0.1", 5002),
    "receiver_bind": ("127.0.0.1", 5002),
    "receiver_target": ("127.0.0.1", 5021),
    "sender_target": ("127.0.0.1", 5001),
}

PLANS = {
    "mininet": MININET_PLAN,
    "loopback": LOOPBACK_PLAN,
    "relay": RELAY_PLAN,
}

BASE_FILE_BYTES = 5 * 1000


def format_address(address):
    return f"{address[0]}:{address[1]}"


def prepare_run_dir(results_root, run_id):
    """Create results/<run_id>/ or refuse to reuse an existing run."""
    run_dir = Path(results_root) / run_id
    if run_dir.exists():
        raise SystemExit(
            f"error: run directory already exists: {run_dir} "
            f"(choose another --run-id)"
        )
    run_dir.mkdir(parents=True)
    return run_dir


def make_input(path, size_bytes):
    with open(path, "wb") as file:
        remaining = size_bytes
        while remaining > 0:
            chunk = min(remaining, 1 << 20)
            file.write(os.urandom(chunk))
            remaining -= chunk


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while True:
            chunk = file.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def parse_details(details):
    values = {}
    for token in str(details).split():
        key, _, value = token.partition("=")
        try:
            values[key] = float(value)
        except ValueError:
            values[key] = value
    return values


def parse_sender_log(path):
    """Extract RTT samples (ms), retransmissions and completion."""
    result = {
        "rtt_samples_ms": [],
        "retransmissions": 0,
        "completed": False,
        "rows": 0,
    }

    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            result["rows"] += 1
            event = row["event"]

            if event == "RTT_SAMPLE":
                rtt = parse_details(row["details"]).get("rtt")
                if rtt is not None:
                    result["rtt_samples_ms"].append(rtt * 1000.0)
            elif event == "RETRANSMISSION":
                result["retransmissions"] += 1
            elif event == "TRANSFER_COMPLETED":
                result["completed"] = True

    return result


def parse_receiver_log(path):
    result = {"completed": False, "bytes": None, "rows": 0}

    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            result["rows"] += 1
            if row["event"] == "TRANSFER_COMPLETED":
                result["completed"] = True
                result["bytes"] = int(row["bytes"])

    return result


def parse_telemetry(path):
    """Summarise router telemetry: occupancy, sojourn, drops, marks."""
    stats = {
        "rows": 0,
        "enqueue": 0,
        "forward": 0,
        "drop": 0,
        "error": 0,
        "max_queue": 0,
        "mean_queue": None,
        "mean_sojourn_ms": None,
        "max_sojourn_ms": None,
        "marks": 0,
        "forwarded_bytes": 0,
    }

    queue_after_arrival = []
    sojourns = []

    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            stats["rows"] += 1
            event = row["event"]

            if event not in stats:
                raise ValueError(f"unknown telemetry event: {event!r}")
            stats[event] += 1

            queue_length = int(row["queue_length_packets"])
            stats["max_queue"] = max(stats["max_queue"], queue_length)
            if event == "enqueue":
                queue_after_arrival.append(queue_length)

            if event == "forward":
                sojourns.append(float(row["sojourn_ms"]))
                stats["forwarded_bytes"] = max(
                    stats["forwarded_bytes"], int(row["forwarded_bytes"])
                )

            stats["marks"] = max(stats["marks"], int(row["marks"]))

    if queue_after_arrival:
        stats["mean_queue"] = statistics.fmean(queue_after_arrival)
    if sojourns:
        stats["mean_sojourn_ms"] = statistics.fmean(sojourns)
        stats["max_sojourn_ms"] = max(sojourns)

    return stats


def percentile(values, pct):
    """Linear-interpolated percentile of a non-empty sequence."""
    if not values:
        raise ValueError("percentile of an empty sequence")

    ordered = sorted(values)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)

    if low == high:
        return ordered[low]

    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def rtt_stats(samples_ms):
    if not samples_ms:
        return None

    return {
        "n": len(samples_ms),
        "min_ms": min(samples_ms),
        "mean_ms": statistics.fmean(samples_ms),
        "p50_ms": percentile(samples_ms, 50),
        "p95_ms": percentile(samples_ms, 95),
        "max_ms": max(samples_ms),
    }


def wait_for(condition, process, timeout, poll_seconds=0.05):
    """Poll a readiness condition while the process stays alive."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if condition():
            return True
        if process is not None and process.poll() is not None:
            return False
        time.sleep(poll_seconds)

    return False


class MininetContext:
    """h1 -- router -- h2 topology; launches roles in node namespaces."""

    name = "mininet"

    def __init__(self, python, delay_ms):
        from mininet.net import Mininet
        from mininet.link import TCLink
        from mininet.log import setLogLevel

        setLogLevel("warning")

        net = Mininet(controller=None, link=TCLink)
        h1 = net.addHost("h1", ip="10.0.0.1/24")
        router = net.addHost("r", ip="10.0.0.2/24")
        h2 = net.addHost("h2", ip="10.0.1.1/24")

        delay = f"{delay_ms}ms"
        net.addLink(h1, router, cls=TCLink, delay=delay)
        net.addLink(router, h2, cls=TCLink, delay=delay)

        net.start()
        router.setIP("10.0.1.2/24", intf="r-eth1")

        self.net = net
        self.nodes = {"sender": h1, "router": router, "receiver": h2}
        self.python = python

    def launch(self, role, argv, stdout_path):
        return self.nodes[role].popen(
            argv,
            cwd=str(REPO_ROOT),
            stdout=open(stdout_path, "w"),
            stderr=subprocess.STDOUT,
        )

    def teardown(self):
        self.net.stop()


class LoopbackContext:
    """Sandbox topology: all roles as local processes on 127.0.0.1."""

    name = "loopback"

    def __init__(self, python):
        self.python = python

    def launch(self, role, argv, stdout_path):
        return subprocess.Popen(
            argv,
            cwd=str(REPO_ROOT),
            stdout=open(stdout_path, "w"),
            stderr=subprocess.STDOUT,
        )

    def teardown(self):
        pass


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Phase 4 bufferbloat experiments (large vs small "
        "DropTail buffer through the user-space router)."
    )
    parser.add_argument(
        "--topology",
        choices=sorted(PLANS),
        default="mininet",
        help="mininet (root) or loopback (sandbox, no link delay)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="output directory name under results/phase4 (must be new; "
        "default: timestamp, prefixed manual_ in interactive mode)",
    )
    parser.add_argument(
        "--results-root",
        default=str(DEFAULT_RESULTS_ROOT),
        help="root directory for experiment outputs",
    )
    parser.add_argument(
        "--rate-mbps",
        type=float,
        default=5.0,
        help="router forward-path rate in Mbps (guide range 2-10)",
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=20.0,
        help="per-access-link propagation delay (mininet topology only)",
    )
    parser.add_argument(
        "--large-capacity",
        type=int,
        default=500,
        help="large DropTail capacity in packets",
    )
    parser.add_argument(
        "--small-capacity",
        type=int,
        default=50,
        help="small DropTail capacity in packets",
    )
    parser.add_argument(
        "--file-bytes",
        type=int,
        default=30 * 1000 * 1000,
        help="bulk transfer input size in bytes",
    )
    parser.add_argument(
        "--sender-window",
        type=int,
        default=400,
        help="experiment-only sender max in-flight packets",
    )
    parser.add_argument(
        "--initial-ssthresh",
        type=float,
        default=20.0,
        help="sender initial ssthresh for the buffer runs; 20 makes the "
        "slow-start-to-CA transition land at the 20-packet small-queue "
        "boundary so loss arrives one packet per window instead of as "
        "an un-paced burst",
    )
    parser.add_argument(
        "--rwnd-bytes",
        type=int,
        default=800_000,
        help="receiver advertised-window buffer in bytes",
    )
    parser.add_argument(
        "--python",
        default=PYTHON,
        help="python interpreter used for transport and router roles",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="manual-validation mode: build the Mininet topology, start "
        "the router, print operator commands, then enter the Mininet CLI",
    )
    parser.add_argument(
        "--queue-capacity",
        type=int,
        default=None,
        help="interactive-mode router DropTail capacity in packets "
        "(default: --large-capacity)",
    )
    parser.add_argument(
        "--runs",
        default="base,large,small",
        help="comma-separated subset of base,large,small to execute",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=300.0,
        help="per-transfer wall-clock timeout in seconds",
    )
    return parser


def validate_args(args):
    if args.rate_mbps <= 0:
        raise SystemExit("error: --rate-mbps must be positive")
    if not 2.0 <= args.rate_mbps <= 10.0:
        print(
            f"warning: --rate-mbps {args.rate_mbps} is outside the "
            f"guide's 2-10 Mbps range"
        )
    if args.large_capacity < 1 or args.small_capacity < 1:
        raise SystemExit("error: queue capacities must be at least 1")
    if args.file_bytes < 1000:
        raise SystemExit("error: --file-bytes must be at least 1000")
    if args.sender_window < 1:
        raise SystemExit("error: --sender-window must be at least 1")
    if args.rwnd_bytes < 1:
        raise SystemExit("error: --rwnd-bytes must be at least 1")
    if args.timeout_s < 10:
        raise SystemExit("error: --timeout-s must be at least 10")
    if args.queue_capacity is not None and args.queue_capacity < 1:
        raise SystemExit("error: --queue-capacity must be at least 1")
    if args.interactive and args.topology != "mininet":
        raise SystemExit(
            "error: --interactive requires the Mininet topology "
            "(--topology mininet)"
        )
    selected = parse_run_selection(args.runs)
    if not selected:
        raise SystemExit("error: --runs selects no experiments")
    if args.topology == "loopback" and args.delay_ms:
        print(
            "warning: --delay-ms is ignored in loopback mode "
            "(no link emulation)"
        )
    return selected


def parse_run_selection(text):
    valid = ("base", "large", "small")
    selected = []
    for name in text.split(","):
        name = name.strip().lower()
        if not name:
            continue
        if name not in valid:
            raise SystemExit(
                f"error: unknown run {name!r}; choose from {valid}"
            )
        if name not in selected:
            selected.append(name)
    return selected


def sender_options(args, bulk):
    """Sender CLI options; bulk runs raise the in-flight cap."""
    if not bulk:
        return []

    return [
        "--window-size", str(args.sender_window),
        "--initial-ssthresh", str(args.initial_ssthresh),
    ]


def receiver_options(args):
    if args.rwnd_bytes is None:
        return []
    return ["--rwnd-bytes", str(args.rwnd_bytes)]


def build_router_command(python, plan, args, run_dir, queue_capacity,
                         max_seconds=None):
    """Exact router invocation shared by automated and interactive runs."""
    return [
        python, "-u", "-m", "router.forwarder",
        "--sender-face", format_address(plan["router_sender_face"]),
        "--receiver-face", format_address(plan["router_receiver_face"]),
        "--receiver", format_address(plan["receiver_target"]),
        "--rate-mbps", str(args.rate_mbps),
        "--queue-capacity", str(queue_capacity),
        "--telemetry", str(Path(run_dir) / "router_telemetry.csv"),
        "--ready-file", str(Path(run_dir) / "router.ready"),
        "--max-seconds", str(
            args.timeout_s + 60.0 if max_seconds is None else max_seconds
        ),
    ]


def stop_process(process, timeout=15.0):
    if process.poll() is None:
        process.terminate()
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            return process.wait(timeout=timeout)
    return process.returncode


def run_transfer(context, plan, args, run_dir, input_path, input_sha256,
                 queue_capacity, bulk):
    """Run one transfer and collect its evidence; never raises for transfer failures."""
    run_dir.mkdir(parents=True, exist_ok=True)
    output_path = run_dir / "out.bin"
    receiver_log = run_dir / "receiver_log.csv"
    sender_log = run_dir / "sender_log.csv"
    router_ready = run_dir / "router.ready"

    router_cmd = build_router_command(
        context.python, plan, args, run_dir, queue_capacity
    )
    receiver_cmd = [
        context.python, "-u", str(NODE), "receiver",
        str(output_path),
        plan["receiver_bind"][0],
        str(plan["receiver_bind"][1]),
        str(receiver_log),
        *receiver_options(args),
    ]
    sender_cmd = [
        context.python, "-u", str(NODE), "sender",
        str(input_path),
        plan["sender_target"][0],
        str(plan["sender_target"][1]),
        str(sender_log),
        *sender_options(args, bulk),
    ]

    commands = {
        "router": " ".join(router_cmd),
        "receiver": " ".join(receiver_cmd),
        "sender": " ".join(sender_cmd),
    }

    relay_specs = []
    if "relay1_listen" in plan:
        for name, listen_key, forward_key in (
            ("relay1", "relay1_listen", "relay1_forward"),
            ("relay2", "relay2_listen", "relay2_forward"),
        ):
            relay_cmd = [
                context.python, "-u", str(NODE), "relay",
                format_address(plan[listen_key]),
                format_address(plan[forward_key]),
                str(run_dir / f"{name}_stats.txt"),
                "--delay-ms", str(args.delay_ms),
                "--ack-delay-ms", str(args.delay_ms),
            ]
            relay_specs.append((name, relay_cmd))
            commands[name] = " ".join(relay_cmd)

    result = {
        "queue_capacity_packets": queue_capacity,
        "bulk": bulk,
        "commands": commands,
        "sender_exit": None,
        "receiver_exit": None,
        "router_exit": None,
        "relay_exits": {},
        "router_ready": False,
        "receiver_ready": False,
        "duration_s": None,
        "sha256_match": None,
        "sender": None,
        "receiver": None,
        "telemetry": None,
        "success": False,
        "failure_reasons": [],
    }

    router = context.launch("router", router_cmd, run_dir / "router.out")
    relays = []
    try:
        result["router_ready"] = wait_for(
            router_ready.exists, router, timeout=20.0
        )
        if not result["router_ready"]:
            result["failure_reasons"].append("router never became ready")
            return result

        for name, relay_cmd in relay_specs:
            relays.append(
                (name, context.launch(name, relay_cmd, run_dir / f"{name}.out"))
            )

        receiver = context.launch(
            "receiver", receiver_cmd, run_dir / "receiver.out"
        )
        try:
            result["receiver_ready"] = wait_for(
                lambda: "Receiver listening"
                in (run_dir / "receiver.out").read_text(errors="replace"),
                receiver,
                timeout=15.0,
            )
            if not result["receiver_ready"]:
                result["failure_reasons"].append(
                    "receiver never reported listening"
                )

            started = time.monotonic()
            sender = context.launch(
                "sender", sender_cmd, run_dir / "sender.out"
            )
            try:
                sender.wait(timeout=args.timeout_s)
            except subprocess.TimeoutExpired:
                result["failure_reasons"].append("sender timed out")
                sender.kill()
                sender.wait(timeout=10.0)
            result["sender_exit"] = sender.returncode
            result["duration_s"] = time.monotonic() - started

            try:
                receiver.wait(timeout=30.0)
            except subprocess.TimeoutExpired:
                result["failure_reasons"].append("receiver timed out")
                receiver.kill()
                receiver.wait(timeout=10.0)
            result["receiver_exit"] = receiver.returncode
        finally:
            if receiver.poll() is None:
                receiver.kill()

        result["router_exit"] = stop_process(router)
        for name, relay_process in relays:
            result["relay_exits"][name] = stop_process(relay_process)
    finally:
        if router.poll() is None:
            router.kill()
            router.wait()
        for _name, relay_process in relays:
            if relay_process.poll() is None:
                relay_process.kill()
                relay_process.wait()

    if result["failure_reasons"]:
        return result

    if result["sender_exit"] != 0:
        result["failure_reasons"].append(
            f"sender exited {result['sender_exit']}"
        )
    if result["receiver_exit"] != 0:
        result["failure_reasons"].append(
            f"receiver exited {result['receiver_exit']}"
        )
    if result["router_exit"] != 0:
        result["failure_reasons"].append(
            f"router exited {result['router_exit']}"
        )
    for name, relay_exit in result["relay_exits"].items():
        if relay_exit != 0:
            result["failure_reasons"].append(f"{name} exited {relay_exit}")

    result["sender"] = parse_sender_log(sender_log)
    result["receiver"] = parse_receiver_log(receiver_log)
    telemetry_path = run_dir / "router_telemetry.csv"
    result["telemetry"] = parse_telemetry(telemetry_path)

    if not result["sender"]["completed"]:
        result["failure_reasons"].append("sender log lacks TRANSFER_COMPLETED")
    if not result["receiver"]["completed"]:
        result["failure_reasons"].append(
            "receiver log lacks TRANSFER_COMPLETED"
        )

    if output_path.exists():
        result["sha256_match"] = (
            sha256_file(output_path) == input_sha256
        )
        if not result["sha256_match"]:
            result["failure_reasons"].append("SHA-256 mismatch")
    else:
        result["failure_reasons"].append("output file missing")

    if result["telemetry"]["forward"] < 1:
        result["failure_reasons"].append("router forwarded no datagrams")

    result["success"] = not result["failure_reasons"]
    return result


def write_transfer_metadata(run_dir, result):
    lines = [
        f"success: {result['success']}",
        f"queue_capacity_packets: {result['queue_capacity_packets']}",
        f"sender_exit: {result['sender_exit']}",
        f"receiver_exit: {result['receiver_exit']}",
        f"router_exit: {result['router_exit']}",
        f"router_ready: {result['router_ready']}",
        f"receiver_ready: {result['receiver_ready']}",
        f"duration_s: "
        f"{result['duration_s']:.3f}" if result["duration_s"] else
        "duration_s: n/a",
        f"sha256_match: {result['sha256_match']}",
        f"failure_reasons: "
        f"{'; '.join(result['failure_reasons']) or 'none'}",
        "",
        "Exact commands used:",
    ]
    for role in ("router", "receiver", "sender"):
        lines.append(f"  {role}: {result['commands'][role]}")

    (run_dir / "run_metadata.txt").write_text("\n".join(lines) + "\n")


def goodput_mbps(file_bytes, duration_s):
    if not duration_s:
        return None
    return file_bytes * 8 / duration_s / 1_000_000


def build_summary(config, plan, results, input_bytes):
    base = results.get("base_rtt")

    base_rtt = (
        rtt_stats(base["sender"]["rtt_samples_ms"])
        if base and base["sender"]
        else None
    )

    summary = {
        "topology": config.topology,
        "address_plan": {
            name: format_address(address)
            for name, address in plan.items()
        },
        "configuration": {
            "rate_mbps": config.rate_mbps,
            "access_link_delay_ms": config.delay_ms,
            "file_bytes": input_bytes,
            "sender_window_packets": config.sender_window,
            "initial_ssthresh": config.initial_ssthresh,
            "rwnd_bytes": config.rwnd_bytes,
            "large_capacity_packets": config.large_capacity,
            "small_capacity_packets": config.small_capacity,
        },
        "base_rtt": base_rtt,
        "runs": {},
    }

    for name in ("large_buffer", "small_buffer"):
        run = results.get(name)
        if run is None:
            continue
        telemetry = run["telemetry"]
        entry = {
            "queue_capacity_packets": run["queue_capacity_packets"],
            "success": run["success"],
            "failure_reasons": run["failure_reasons"],
            "duration_s": run["duration_s"],
            "goodput_mbps": goodput_mbps(input_bytes, run["duration_s"]),
            "sha256_match": run["sha256_match"],
            "rtt": rtt_stats(run["sender"]["rtt_samples_ms"])
            if run["sender"] else None,
            "retransmissions": run["sender"]["retransmissions"]
            if run["sender"] else None,
            "telemetry": telemetry,
        }

        if entry["goodput_mbps"] is not None:
            entry["link_utilisation"] = (
                entry["goodput_mbps"] / config.rate_mbps
            )

        summary["runs"][name] = entry

    large = summary["runs"].get("large_buffer")
    small = summary["runs"].get("small_buffer")
    large_rtt = large["rtt"] if large else None
    small_rtt = small["rtt"] if small else None

    if (
        large_rtt and small_rtt
        and large["telemetry"] and small["telemetry"]
    ):
        summary["comparison"] = {
            "large_mean_rtt_ms": large_rtt["mean_ms"],
            "small_mean_rtt_ms": small_rtt["mean_ms"],
            "large_p95_rtt_ms": large_rtt["p95_ms"],
            "small_p95_rtt_ms": small_rtt["p95_ms"],
            "large_max_queue": large["telemetry"]["max_queue"],
            "small_max_queue": small["telemetry"]["max_queue"],
            "large_drops": large["telemetry"]["drop"],
            "small_drops": small["telemetry"]["drop"],
            "delay_inflation_large_vs_base": (
                large_rtt["mean_ms"] / base_rtt["mean_ms"]
                if base_rtt else None
            ),
            "delay_inflation_small_vs_base": (
                small_rtt["mean_ms"] / base_rtt["mean_ms"]
                if base_rtt else None
            ),
        }

    summary["overall_success"] = all(
        run["success"] for run in results.values()
    )
    return summary


def write_summary_files(run_dir, summary):
    comparison = run_dir / "comparison"
    comparison.mkdir(exist_ok=True)

    with open(comparison / "summary.json", "w") as file:
        json.dump(summary, file, indent=2)
        file.write("\n")

    lines = [
        "Phase 4 bufferbloat summary",
        f"topology: {summary['topology']}",
        f"overall_success: {summary['overall_success']}",
        "",
        f"configuration: {json.dumps(summary['configuration'])}",
        "",
    ]

    if summary["base_rtt"]:
        stats = summary["base_rtt"]
        lines.append(
            f"base RTT (unloaded, real transport): "
            f"n={stats['n']} mean={stats['mean_ms']:.2f}ms "
            f"p50={stats['p50_ms']:.2f}ms min={stats['min_ms']:.2f}ms"
        )
        lines.append("")

    for name in ("large_buffer", "small_buffer"):
        run = summary["runs"].get(name)
        if run is None:
            continue
        lines.append(
            f"{name}: capacity={run['queue_capacity_packets']}p "
            f"success={run['success']} "
            f"goodput={run['goodput_mbps'] and round(run['goodput_mbps'], 3)}Mbps "
            f"utilisation={run.get('link_utilisation') and round(run['link_utilisation'], 3)}"
        )

        if run["rtt"]:
            stats = run["rtt"]
            lines.append(
                f"  loaded RTT: n={stats['n']} mean={stats['mean_ms']:.2f}ms "
                f"p50={stats['p50_ms']:.2f}ms p95={stats['p95_ms']:.2f}ms "
                f"max={stats['max_ms']:.2f}ms"
            )

        telemetry = run["telemetry"]
        if telemetry:
            lines.append(
                f"  queue: max={telemetry['max_queue']}p "
                f"mean={telemetry['mean_queue'] is not None and round(telemetry['mean_queue'], 2)} "
                f"drops={telemetry['drop']} marks={telemetry['marks']}"
            )
            if telemetry["mean_sojourn_ms"] is not None:
                lines.append(
                    f"  sojourn (router queueing delay): "
                    f"mean={telemetry['mean_sojourn_ms']:.2f}ms "
                    f"max={telemetry['max_sojourn_ms']:.2f}ms"
                )
        else:
            lines.append("  queue telemetry: not collected")
        lines.append(
            f"  retransmissions={run['retransmissions']} "
            f"sha256_match={run['sha256_match']}"
        )
        if run["failure_reasons"]:
            lines.append(f"  failures: {run['failure_reasons']}")
        lines.append("")

    if "comparison" in summary:
        contrast = summary["comparison"]
        lines.append("large vs small buffer comparison:")
        lines.append(
            f"  mean RTT: large={contrast['large_mean_rtt_ms']:.2f}ms "
            f"small={contrast['small_mean_rtt_ms']:.2f}ms"
        )
        lines.append(
            f"  max queue: large={contrast['large_max_queue']}p "
            f"small={contrast['small_max_queue']}p"
        )
        lines.append(
            f"  drops: large={contrast['large_drops']} "
            f"small={contrast['small_drops']}"
        )
        if contrast["delay_inflation_large_vs_base"]:
            lines.append(
                f"  RTT inflation vs base: "
                f"large={contrast['delay_inflation_large_vs_base']:.1f}x "
                f"small={contrast['delay_inflation_small_vs_base']:.1f}x"
            )
        lines.append("")
        lines.append(
            "Note: router sojourn time is the per-datagram queueing "
            "delay inside the bottleneck queue; end-to-end RTT adds the "
            "propagation and receiver turnaround on top of it."
        )

    (comparison / "summary.txt").write_text("\n".join(lines) + "\n")


def run_experiments(args):
    results_root = Path(args.results_root)
    run_dir = prepare_run_dir(results_root, args.run_id)
    plan = PLANS[args.topology]

    print(f"run directory: {run_dir}")
    print(f"topology: {args.topology}")

    context = (
        MininetContext(args.python, args.delay_ms)
        if args.topology == "mininet"
        else LoopbackContext(args.python)
    )

    try:
        bulk_input = run_dir / "input.bin"
        make_input(bulk_input, args.file_bytes)
        bulk_sha = sha256_file(bulk_input)

        base_input = run_dir / "input_base.bin"
        make_input(base_input, BASE_FILE_BYTES)
        base_sha = sha256_file(base_input)

        selected = parse_run_selection(args.runs)
        results = {}
        run_specs = {
            "base": (
                "base_rtt", base_input, base_sha, args.large_capacity, False,
                "[base] base RTT transfer (unloaded)",
            ),
            "large": (
                "large_buffer", bulk_input, bulk_sha, args.large_capacity,
                True, f"[large] large buffer ({args.large_capacity} packets)",
            ),
            "small": (
                "small_buffer", bulk_input, bulk_sha, args.small_capacity,
                True, f"[small] small buffer ({args.small_capacity} packets)",
            ),
        }

        for index, name in enumerate(selected, start=1):
            dir_name, input_path, sha, capacity, bulk, label = run_specs[name]
            print(f"[{index}/{len(selected)}] {label}...")
            results[dir_name] = run_transfer(
                context, plan, args, run_dir / dir_name, input_path,
                sha, queue_capacity=capacity, bulk=bulk,
            )
            print(
                f"      success={results[dir_name]['success']} "
                f"reasons={results[dir_name]['failure_reasons']}"
            )

        for name, result in results.items():
            write_transfer_metadata(run_dir / name, result)

        summary = build_summary(args, plan, results, args.file_bytes)
        write_summary_files(run_dir, summary)

        if all((run_dir / d).is_dir() for d in
               ("base_rtt", "large_buffer", "small_buffer")):
            plot = subprocess.run(
                [args.python, str(PLOT_SCRIPT), str(run_dir)],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
            (run_dir / "comparison" / "plot.out").write_text(
                plot.stdout + plot.stderr
            )
            if plot.returncode != 0:
                print(
                    "warning: plot generation failed "
                    f"(exit {plot.returncode}); see comparison/plot.out"
                )

        print(
            "overall_success: "
            f"{summary['overall_success']}"
        )
        return 0 if summary["overall_success"] else 1
    finally:
        context.teardown()


def make_deterministic_input(path, size_bytes, seed=20261113):
    """Write a seeded input so manual runs are byte-for-byte reproducible."""
    import random

    rng = random.Random(seed)
    with open(path, "wb") as file:
        remaining = size_bytes
        while remaining > 0:
            chunk = min(remaining, 1 << 20)
            file.write(rng.randbytes(chunk))
            remaining -= chunk


def topology_lines(nodes):
    lines = ["topology: h1 -- router -- h2"]

    for role in ("sender", "router", "receiver"):
        node = nodes[role]
        lines.append(f"  {role}: node {node.name}")

        for intf in node.intfList():
            ip = node.IP(intf) or "-"
            lines.append(f"    {intf.name}: {ip}")

    return lines


def build_manual_endpoint_commands(args, plan, run_dir, interfaces):
    """Operator commands for the Mininet CLI and the shell, with exact
    PID files for every launched process and validated stop commands."""
    for key in ("sender", "router_sender", "router_receiver", "receiver"):
        if not interfaces.get(key):
            raise ValueError(
                f"capture interface for {key} is missing; refusing to "
                f"generate manual commands"
            )

    cli = [
        f"mininet> h1 tcpdump -i {interfaces['sender']} -w "
        f"{run_dir}/h1_capture.pcap & echo $! > {run_dir}/h1_capture.pid",
        f"mininet> r tcpdump -i {interfaces['router_sender']} -w "
        f"{run_dir}/router_sender_capture.pcap & "
        f"echo $! > {run_dir}/router_sender_capture.pid",
        f"mininet> r tcpdump -i {interfaces['router_receiver']} -w "
        f"{run_dir}/router_receiver_capture.pcap & "
        f"echo $! > {run_dir}/router_receiver_capture.pid",
        f"mininet> h2 tcpdump -i {interfaces['receiver']} -w "
        f"{run_dir}/h2_capture.pcap & echo $! > {run_dir}/h2_capture.pid",
        f"mininet> h2 python3 -u {NODE} receiver {run_dir}/out.bin "
        f"{plan['receiver_bind'][0]} {plan['receiver_bind'][1]} "
        f"{run_dir}/manual_receiver_log.csv --rwnd-bytes {args.rwnd_bytes} "
        f"</dev/null > {run_dir}/manual_receiver_stdout.log 2>&1 "
        f"& echo $! > {run_dir}/receiver.pid",
        f"mininet> h1 python3 -u {NODE} sender {run_dir}/input.bin "
        f"{plan['sender_target'][0]} {plan['sender_target'][1]} "
        f"{run_dir}/manual_sender_log.csv "
        f"--window-size {args.sender_window} "
        f"--initial-ssthresh {args.initial_ssthresh} "
        f"</dev/null > {run_dir}/manual_sender_stdout.log 2>&1 "
        f"& echo $! > {run_dir}/sender.pid",
        f"mininet> sh tail -f {run_dir}/router_telemetry.csv",
    ]

    def stop_command(pid_file, fragment, label):
        return (
            f"p=$(cat {run_dir}/{pid_file} 2>/dev/null) && "
            f"grep -aq {fragment} /proc/$p/cmdline && kill $p || "
            f"echo \"{label}: pid $p is not the expected process; "
            f"not signalled\""
        )

    stops = [
        stop_command("h1_capture.pid", "tcpdump", "h1 capture"),
        stop_command("router_sender_capture.pid", "tcpdump",
                     "router sender-face capture"),
        stop_command("router_receiver_capture.pid", "tcpdump",
                     "router receiver-face capture"),
        stop_command("h2_capture.pid", "tcpdump", "h2 capture"),
        stop_command("sender.pid", "phase2_node.py", "sender"),
        stop_command("receiver.pid", "phase2_node.py", "receiver"),
    ]

    shell = stops + [
        f"sha256sum {run_dir}/out.bin   # compare with the expected SHA-256",
        f"tcpdump -r {run_dir}/h1_capture.pcap 2>/dev/null | head -20",
        f"tcpdump -r {run_dir}/router_sender_capture.pcap 2>/dev/null "
        f"| head -20",
        f"tcpdump -r {run_dir}/router_receiver_capture.pcap 2>/dev/null "
        f"| head -20",
        f"tcpdump -r {run_dir}/h2_capture.pcap 2>/dev/null | head -20",
        f"head -30 {run_dir}/router_telemetry.csv",
        f"column -s, -t < {run_dir}/manual_receiver_log.csv | head -30",
    ]

    return {"cli": cli, "shell": shell}


PIDFILE_WAIT_SECONDS = 10.0
PIDFILE_POLL_SECONDS = 0.05


def read_pid_file(path):
    try:
        text = Path(path).read_text().strip()
    except OSError:
        return None

    return int(text) if text.isdigit() else None


def process_cmdline(pid):
    """Command line of pid with NULs as spaces; None if it is gone."""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None

    return raw.decode(errors="replace").replace("\x00", " ").strip()


def pid_matches(pid, expected_fragment):
    """True when pid still runs the expected program (guards PID reuse)."""
    cmdline = process_cmdline(pid)
    return cmdline is not None and expected_fragment in cmdline


def pid_alive(pid):
    """True while pid runs; a zombie counts as terminated."""
    return bool(process_cmdline(pid))


def terminate_pid_file(path, expected_fragment,
                       timeout=PIDFILE_WAIT_SECONDS):
    """Validate, SIGTERM and wait for one exact PID from a pid file,
    escalating to SIGKILL; stale or reused PIDs are never signalled."""
    pid = read_pid_file(path)
    if pid is None or not pid_matches(pid, expected_fragment):
        Path(path).unlink(missing_ok=True)
        return False

    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        Path(path).unlink(missing_ok=True)
        return False

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and pid_alive(pid):
        time.sleep(PIDFILE_POLL_SECONDS)

    if pid_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and pid_alive(pid):
            time.sleep(PIDFILE_POLL_SECONDS)

    Path(path).unlink(missing_ok=True)
    return True


def shutdown_interactive(context, router, run_dir=None):
    """Terminate only this session's processes (validated PID files
    and the router Popen), then tear the network down."""
    if run_dir is not None:
        for name, fragment in (
            ("sender.pid", "phase2_node.py"),
            ("receiver.pid", "phase2_node.py"),
            ("h1_capture.pid", "tcpdump"),
            ("router_sender_capture.pid", "tcpdump"),
            ("router_receiver_capture.pid", "tcpdump"),
            ("h2_capture.pid", "tcpdump"),
        ):
            terminate_pid_file(Path(run_dir) / name, fragment)

    if router is not None:
        stop_process(router)

    context.teardown()


def run_interactive(args):
    """Manual-validation mode: topology plus router, then the Mininet CLI."""
    run_dir = prepare_run_dir(Path(args.results_root), args.run_id).resolve()
    capacity = (
        args.queue_capacity if args.queue_capacity is not None
        else args.large_capacity
    )
    print(f"manual-validation directory: {run_dir}")
    print(
        f"router: rate {args.rate_mbps} Mbps, queue {capacity} packets, "
        f"access-link delay {args.delay_ms} ms"
    )

    input_path = run_dir / "input.bin"
    make_deterministic_input(input_path, args.file_bytes)
    input_sha = sha256_file(input_path)
    print(f"input file: {input_path} ({args.file_bytes} bytes)")
    print(f"expected SHA-256: {input_sha}")

    context = MininetContext(args.python, args.delay_ms)
    router = None
    try:
        for line in topology_lines(context.nodes):
            print(line)

        router_cmd = build_router_command(
            context.python, MININET_PLAN, args, run_dir, capacity,
            max_seconds=86400.0,
        )
        router = context.launch("router", router_cmd, run_dir / "router.out")
        if not wait_for(
            (run_dir / "router.ready").exists, router, timeout=20.0
        ):
            print("error: router never became ready; see router.out")
            return 1

        interfaces = discover_capture_interfaces(
            context.nodes, MININET_PLAN
        )
        commands = build_manual_endpoint_commands(
            args, MININET_PLAN, run_dir, interfaces
        )

        print("\nPaste into the mininet> CLI:")
        for line in commands["cli"]:
            print(f"  {line}")
        print("\nAfter the transfer (from the shell or inside the CLI):")
        for line in commands["shell"]:
            print(f"  {line}")
        (run_dir / "manual_commands.txt").write_text(
            "\n".join(commands["cli"] + [""] + commands["shell"]) + "\n"
        )
        print(f"\ncommands saved to {run_dir / 'manual_commands.txt'}")
        print("Type 'exit' in the CLI to shut the router and network down.")

        from mininet.cli import CLI

        CLI(context.net)
    finally:
        shutdown_interactive(context, router, run_dir)

    print("manual-validation session finished cleanly")
    return 0


def _interface_for_remote(node, remote_ip):
    """Name of the node's interface on the same /24 as remote_ip, or
    None; every host and router face owns one access-link /24."""
    network = ipaddress.ip_network(f"{remote_ip}/24", strict=False)

    for intf in node.intfList():
        ip = node.IP(intf)
        if not ip:
            continue
        try:
            if ipaddress.ip_address(ip) in network:
                return intf.name
        except ValueError:
            continue

    return None


def discover_capture_interfaces(nodes, plan):
    """Capture interface per point from the live topology; aborts
    with a clear error when any lookup fails."""
    lookups = (
        ("sender", "sender", plan["sender_target"][0]),
        ("router_sender", "router", plan["router_sender_face"][0]),
        ("router_receiver", "router", plan["router_receiver_face"][0]),
        ("receiver", "receiver", plan["receiver_target"][0]),
    )

    interfaces = {}
    for key, role, remote_ip in lookups:
        node = nodes[role]
        name = _interface_for_remote(node, remote_ip)

        if name is None:
            raise SystemExit(
                f"error: no {node.name} interface is on the same /24 as "
                f"{remote_ip} ({key} capture); aborting before "
                f"generating manual commands"
            )

        interfaces[key] = name

    return interfaces


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    validate_args(args)
    if args.run_id is None:
        args.run_id = (
            f"manual_{time.strftime('%Y%m%d_%H%M%S')}"
            if args.interactive
            else time.strftime("%Y%m%d_%H%M%S")
        )
    if args.interactive:
        return run_interactive(args)
    return run_experiments(args)


if __name__ == "__main__":
    sys.exit(main())
