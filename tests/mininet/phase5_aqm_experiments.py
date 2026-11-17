"""Phase 5 AQM experiments: DropTail vs RED vs CoDel through the router."""

import argparse
import csv
import importlib.util
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from router.queues.red import DEFAULT_TYPICAL_PACKET_BYTES

NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"
PHASE4_HARNESS = REPO_ROOT / "tests" / "mininet" / "phase4_experiments.py"
PLOT_SCRIPT = REPO_ROOT / "scripts" / "plot_aqm_comparison.py"
DEFAULT_RESULTS_ROOT = REPO_ROOT / "results" / "phase5"

PYTHON = sys.executable

AQMS = ("droptail", "red", "codel")


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


phase4 = load_module("phase4_experiments_support", PHASE4_HARNESS)


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as file:
        json.dump(payload, file, indent=2)
        file.write("\n")


def merge_run_metadata(run_dir, extra):
    path = Path(run_dir) / "run_metadata.json"
    payload = json.loads(path.read_text()) if path.exists() else {}
    payload.update(extra)
    write_json(path, payload)


def router_args_for(args, aqm):
    from router.forwarder import build_parser

    return build_parser().parse_args(
        [
            "--sender-face", "127.0.0.1:0",
            "--receiver-face", "127.0.0.1:0",
            "--receiver", "127.0.0.1:0",
            "--rate-mbps", str(args.rate_mbps),
            "--queue-capacity", str(args.queue_capacity),
            "--telemetry", "t.csv",
            "--aqm", aqm,
            "--aqm-mode", args.aqm_mode,
            *aqm_parameter_flags(args, aqm),
        ]
    )


def build_queue_parameters(args, aqm):
    from router.forwarder import build_queue

    return build_queue(router_args_for(args, aqm)).parameters


def aqm_parameter_flags(args, aqm):
    if aqm == "red":
        flags = [
            "--red-w-q", str(args.red_w_q),
            "--red-min-th", str(args.red_min_th),
            "--red-max-th", str(args.red_max_th),
            "--red-max-p", str(args.red_max_p),
            "--red-typical-packet-bytes",
            str(args.red_typical_packet_bytes),
        ]
        if args.red_gentle:
            flags.append("--red-gentle")
        if args.red_seed is not None:
            flags += ["--red-seed", str(args.red_seed)]
        return flags

    if aqm == "codel":
        return [
            "--codel-target-ms", str(args.codel_target_ms),
            "--codel-interval-ms", str(args.codel_interval_ms),
        ]

    return []


def build_router_command(python, plan, args, run_dir, aqm,
                         max_seconds=None):
    """Exact Phase 5 router invocation shared by automated and manual runs."""
    return [
        python, "-u", "-m", "router.forwarder",
        "--sender-face",
        phase4.format_address(plan["router_sender_face"]),
        "--receiver-face",
        phase4.format_address(plan["router_receiver_face"]),
        "--receiver", phase4.format_address(plan["receiver_target"]),
        "--rate-mbps", str(args.rate_mbps),
        "--aqm", aqm,
        "--aqm-mode", args.aqm_mode,
        *aqm_parameter_flags(args, aqm),
        "--queue-capacity", str(args.queue_capacity),
        "--telemetry", str(Path(run_dir) / "router_telemetry.csv"),
        "--ready-file", str(Path(run_dir) / "router.ready"),
        "--max-seconds", str(
            args.timeout_s + 60.0 if max_seconds is None else max_seconds
        ),
    ]


def router_command_builder_for(args, aqm):
    def builder(python, plan, run_args, run_dir, queue_capacity):
        return build_router_command(python, plan, run_args, run_dir, aqm)

    return builder


def parse_telemetry(path):
    """Summarise telemetry rows: events, reasons, occupancy and sojourn."""
    stats = {
        "rows": 0,
        "enqueue": 0,
        "forward": 0,
        "drop": 0,
        "mark": 0,
        "error": 0,
        "drops_full": 0,
        "drops_red": 0,
        "drops_codel": 0,
        "marks_red": 0,
        "marks_codel": 0,
        "max_queue": 0,
        "mean_queue": None,
        "mean_sojourn_ms": None,
        "median_sojourn_ms": None,
        "p95_sojourn_ms": None,
        "p99_sojourn_ms": None,
        "max_sojourn_ms": None,
        "sojourn_samples": 0,
        "avg_queue_mean": None,
        "avg_queue_max": None,
        "avg_queue_samples": 0,
        "forwarded_bytes": 0,
        "elapsed_s": 0.0,
    }

    queue_after_arrival = []
    sojourns = []
    averages = []

    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            stats["rows"] += 1
            event = row["event"]
            if event not in stats:
                raise ValueError(f"unknown telemetry event: {event!r}")
            stats[event] += 1
            stats["elapsed_s"] = float(row["elapsed_s"])

            queue_length = int(row["queue_length_packets"])
            stats["max_queue"] = max(stats["max_queue"], queue_length)
            if event == "enqueue":
                queue_after_arrival.append(queue_length)

            reason = row.get("reason") or ""
            if event == "drop":
                if reason == "full":
                    stats["drops_full"] += 1
                elif reason == "red":
                    stats["drops_red"] += 1
                elif reason == "codel":
                    stats["drops_codel"] += 1
            elif event == "mark":
                if reason == "red":
                    stats["marks_red"] += 1
                elif reason == "codel":
                    stats["marks_codel"] += 1

            if row.get("avg_queue_packets"):
                averages.append(float(row["avg_queue_packets"]))

            if event == "forward":
                sojourns.append(float(row["sojourn_ms"]))
                stats["forwarded_bytes"] = max(
                    stats["forwarded_bytes"], int(row["forwarded_bytes"])
                )

    if queue_after_arrival:
        stats["mean_queue"] = statistics.fmean(queue_after_arrival)
    if sojourns:
        stats["sojourn_samples"] = len(sojourns)
        stats["mean_sojourn_ms"] = statistics.fmean(sojourns)
        stats["median_sojourn_ms"] = statistics.median(sojourns)
        stats["p95_sojourn_ms"] = phase4.percentile(sojourns, 95)
        stats["p99_sojourn_ms"] = phase4.percentile(sojourns, 99)
        stats["max_sojourn_ms"] = max(sojourns)
    if averages:
        stats["avg_queue_samples"] = len(averages)
        stats["avg_queue_mean"] = statistics.fmean(averages)
        stats["avg_queue_max"] = max(averages)

    return stats


def parse_sender_timeouts(path):
    if not Path(path).exists():
        return 0

    timeouts = 0
    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            if row["event"] != "CWND_UPDATE":
                continue
            if "reason=timeout" in row["details"].split():
                timeouts += 1
    return timeouts


def build_run_metadata(args, plan, run_dir, aqm, input_sha256, parameters):
    return {
        "run_id": Path(run_dir).name,
        "algorithm": aqm,
        "mode": args.aqm_mode,
        "queue": {
            "capacity": args.queue_capacity,
            "parameters": parameters,
        },
        "network": {
            "topology": args.topology,
            "rate_mbps": args.rate_mbps,
            "access_link_delay_ms": args.delay_ms,
            "file_bytes": args.file_bytes,
            "input_sha256": input_sha256,
        },
        "sender": {
            "window_size": args.sender_window,
            "initial_ssthresh": args.initial_ssthresh,
        },
        "receiver": {"rwnd_bytes": args.rwnd_bytes},
        "address_plan": {
            name: phase4.format_address(address)
            for name, address in plan.items()
        },
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def summarize_result(result):
    return {
        "success": result["success"],
        "failure_reasons": result["failure_reasons"],
        "duration_s": result["duration_s"],
        "sha256_match": result["sha256_match"],
        "sender_exit": result["sender_exit"],
        "receiver_exit": result["receiver_exit"],
        "router_exit": result["router_exit"],
        "commands": result["commands"],
    }


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Phase 5 AQM comparison: the same bulk transfer "
        "through DropTail, RED and CoDel queues in the user-space router."
    )
    parser.add_argument(
        "--topology",
        choices=sorted(phase4.PLANS),
        default="mininet",
        help="mininet (root) or loopback/relay (sandbox, no link delay)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="output directory name under results/phase5 (must be new; "
        "default: timestamp, prefixed manual_<aqm>_ in interactive mode)",
    )
    parser.add_argument(
        "--results-root",
        default=str(DEFAULT_RESULTS_ROOT),
        help="root directory for experiment outputs",
    )
    parser.add_argument(
        "--runs",
        default="droptail,red,codel",
        help="comma-separated subset of droptail,red,codel to execute",
    )
    parser.add_argument(
        "--aqm",
        choices=AQMS,
        default="droptail",
        help="algorithm selected by --interactive mode",
    )
    parser.add_argument(
        "--aqm-mode",
        choices=("drop", "mark"),
        default="drop",
        help="congestion signalling mode for red and codel",
    )
    parser.add_argument(
        "--queue-capacity",
        type=int,
        default=200,
        help="queue capacity in packets for every algorithm",
    )
    parser.add_argument(
        "--rate-mbps",
        type=float,
        default=5.0,
        help="router forward-path rate in Mbps",
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=20.0,
        help="per-access-link propagation delay (mininet topology only)",
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
        help="sender initial ssthresh for the bulk runs",
    )
    parser.add_argument(
        "--rwnd-bytes",
        type=int,
        default=800_000,
        help="receiver advertised-window buffer in bytes",
    )
    parser.add_argument(
        "--red-w-q",
        type=float,
        default=0.002,
        help="RED EWMA weight on the instantaneous queue length",
    )
    parser.add_argument(
        "--red-min-th",
        type=float,
        default=20.0,
        help="RED minimum average queue threshold in packets",
    )
    parser.add_argument(
        "--red-max-th",
        type=float,
        default=80.0,
        help="RED maximum average queue threshold in packets",
    )
    parser.add_argument(
        "--red-max-p",
        type=float,
        default=0.1,
        help="RED maximum signalling probability",
    )
    parser.add_argument(
        "--red-gentle",
        action="store_true",
        help="ramp RED signalling to certainty at twice max_th",
    )
    parser.add_argument(
        "--red-seed",
        type=int,
        default=42,
        help="seed for reproducible RED signalling decisions",
    )
    parser.add_argument(
        "--red-typical-packet-bytes",
        type=float,
        default=DEFAULT_TYPICAL_PACKET_BYTES,
        help="typical RED packet size on the bottleneck in bytes",
    )
    parser.add_argument(
        "--codel-target-ms",
        type=float,
        default=5.0,
        help="CoDel target sojourn in milliseconds",
    )
    parser.add_argument(
        "--codel-interval-ms",
        type=float,
        default=100.0,
        help="CoDel above-target window in milliseconds",
    )
    parser.add_argument(
        "--python",
        default=PYTHON,
        help="python interpreter used for transport and router roles",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="manual-validation mode: build the topology, start the "
        "selected AQM router, print exact operator commands, then enter "
        "the Mininet CLI",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=300.0,
        help="per-transfer wall-clock timeout in seconds",
    )
    return parser


def parse_run_selection(text):
    selected = []
    for name in text.split(","):
        name = name.strip().lower()
        if not name:
            continue
        if name not in AQMS:
            raise SystemExit(
                f"error: unknown run {name!r}; choose from {AQMS}"
            )
        if name not in selected:
            selected.append(name)
    return selected


def validate_args(args):
    if args.rate_mbps <= 0:
        raise SystemExit("error: --rate-mbps must be positive")
    if args.queue_capacity < 1:
        raise SystemExit("error: --queue-capacity must be at least 1")
    if args.file_bytes < 1000:
        raise SystemExit("error: --file-bytes must be at least 1000")
    if args.sender_window < 1:
        raise SystemExit("error: --sender-window must be at least 1")
    if args.rwnd_bytes < 1:
        raise SystemExit("error: --rwnd-bytes must be at least 1")
    if args.timeout_s < 10:
        raise SystemExit("error: --timeout-s must be at least 10")
    if args.interactive and args.topology != "mininet":
        raise SystemExit(
            "error: --interactive requires the Mininet topology "
            "(--topology mininet)"
        )

    selected = parse_run_selection(args.runs)
    if not selected:
        raise SystemExit("error: --runs selects no experiments")

    droptail_selected = "droptail" in selected or (
        args.interactive and args.aqm == "droptail"
    )
    if args.aqm_mode == "mark" and droptail_selected:
        raise SystemExit(
            "error: droptail supports only drop mode; remove droptail "
            "from --runs (or from --aqm) or use --aqm-mode drop"
        )

    if args.topology == "loopback" and args.delay_ms:
        print(
            "warning: --delay-ms is ignored in loopback mode "
            "(no link emulation)"
        )

    targets = list(selected)
    if args.interactive and args.aqm not in targets:
        targets.append(args.aqm)
    for aqm in targets:
        try:
            build_queue_parameters(args, aqm)
        except ValueError as error:
            raise SystemExit(
                f"error: invalid {aqm} configuration: {error}"
            ) from None

    return selected


def build_manual_endpoint_commands(args, plan, run_dir, interfaces):
    """Operator commands with resolved paths, interfaces and exact PID files."""
    for key in ("sender", "router_sender", "router_receiver", "receiver"):
        if not interfaces.get(key):
            raise ValueError(
                f"capture interface for {key} is missing; refusing to "
                f"generate manual commands"
            )

    captures = [
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
    ]
    receiver = [
        f"mininet> h2 python3 -u {NODE} receiver {run_dir}/out.bin "
        f"{plan['receiver_bind'][0]} {plan['receiver_bind'][1]} "
        f"{run_dir}/manual_receiver_log.csv "
        f"--rwnd-bytes {args.rwnd_bytes} "
        f"</dev/null > {run_dir}/manual_receiver_stdout.log 2>&1 "
        f"& echo $! > {run_dir}/receiver.pid",
    ]
    sender = [
        f"mininet> h1 python3 -u {NODE} sender {run_dir}/input.bin "
        f"{plan['sender_target'][0]} {plan['sender_target'][1]} "
        f"{run_dir}/manual_sender_log.csv "
        f"--window-size {args.sender_window} "
        f"--initial-ssthresh {args.initial_ssthresh} "
        f"</dev/null > {run_dir}/manual_sender_stdout.log 2>&1 "
        f"& echo $! > {run_dir}/sender.pid",
    ]
    progress = [
        f"mininet> sh tail -n 5 {run_dir}/router_telemetry.csv",
        f"mininet> sh tail -n 3 {run_dir}/manual_sender_log.csv",
        f"mininet> sh tail -n 3 {run_dir}/manual_receiver_log.csv",
        f"mininet> sh ls -l {run_dir}/out.bin",
        f"mininet> sh awk -F, 'NR>1 {{c[$2]++}} END "
        f"{{for (e in c) print e, c[e]}}' {run_dir}/router_telemetry.csv",
    ]

    def stop_command(pid_file, fragment, label):
        return (
            f"p=$(cat {run_dir}/{pid_file} 2>/dev/null) && "
            f"grep -aq {fragment} /proc/$p/cmdline && kill $p || "
            f"echo \"{label}: pid $p is not the expected process; "
            f"not signalled\""
        )

    cleanup = [
        stop_command("h1_capture.pid", "tcpdump", "h1 capture"),
        stop_command("router_sender_capture.pid", "tcpdump",
                     "router sender-face capture"),
        stop_command("router_receiver_capture.pid", "tcpdump",
                     "router receiver-face capture"),
        stop_command("h2_capture.pid", "tcpdump", "h2 capture"),
        stop_command("sender.pid", "phase2_node.py", "sender"),
        stop_command("receiver.pid", "phase2_node.py", "receiver"),
        f"sha256sum {run_dir}/out.bin",
        f"cmp {run_dir}/input.bin {run_dir}/out.bin "
        f"&& echo \"cmp: byte-identical\"",
        f"stat -c '%s bytes  %n' {run_dir}/input.bin {run_dir}/out.bin "
        f"{run_dir}/h1_capture.pcap {run_dir}/router_sender_capture.pcap "
        f"{run_dir}/router_receiver_capture.pcap "
        f"{run_dir}/h2_capture.pcap",
        f"tcpdump -r {run_dir}/h1_capture.pcap 2>/dev/null | head -5",
        f"tcpdump -r {run_dir}/router_sender_capture.pcap 2>/dev/null "
        f"| head -5",
        f"tcpdump -r {run_dir}/router_receiver_capture.pcap 2>/dev/null "
        f"| head -5",
        f"tcpdump -r {run_dir}/h2_capture.pcap 2>/dev/null | head -5",
        f"head -30 {run_dir}/router_telemetry.csv",
        f"tail -n 5 {run_dir}/router_telemetry.csv",
        f"awk -F, 'NR>1 {{c[$2]++}} END "
        f"{{for (e in c) print e, c[e]}}' {run_dir}/router_telemetry.csv",
        f"grep -c TRANSFER_COMPLETED {run_dir}/manual_sender_log.csv",
        f"grep -c TRANSFER_COMPLETED {run_dir}/manual_receiver_log.csv",
        f"column -s, -t < {run_dir}/manual_receiver_log.csv | head -20",
        f"column -s, -t < {run_dir}/manual_sender_log.csv | tail -5",
    ]

    return {
        "captures": captures,
        "receiver": receiver,
        "sender": sender,
        "progress": progress,
        "cleanup": cleanup,
    }


def run_interactive(args):
    """Manual-validation mode: topology plus the selected AQM router."""
    run_dir = phase4.prepare_run_dir(
        Path(args.results_root), args.run_id
    ).resolve()
    print(f"manual-validation directory: {run_dir}")
    print(
        f"algorithm: {args.aqm} ({args.aqm_mode} mode), queue "
        f"{args.queue_capacity} packets, rate {args.rate_mbps} Mbps, "
        f"access-link delay {args.delay_ms} ms"
    )

    input_path = run_dir / "input.bin"
    phase4.make_deterministic_input(input_path, args.file_bytes)
    input_sha = phase4.sha256_file(input_path)
    print(f"input file: {input_path} ({args.file_bytes} bytes)")
    print(f"expected SHA-256: {input_sha}")

    plan = phase4.PLANS["mininet"]
    parameters = build_queue_parameters(args, args.aqm)
    write_json(
        run_dir / "run_metadata.json",
        build_run_metadata(args, plan, run_dir, args.aqm, input_sha,
                           parameters),
    )

    context = phase4.MininetContext(args.python, args.delay_ms)
    router = None
    try:
        for line in phase4.topology_lines(context.nodes):
            print(line)

        router_cmd = build_router_command(
            context.python, plan, args, run_dir, args.aqm,
            max_seconds=86400.0,
        )
        router = context.launch(
            "router", router_cmd, run_dir / "router.out"
        )
        if not phase4.wait_for(
            (run_dir / "router.ready").exists, router, timeout=20.0
        ):
            print("error: router never became ready; see router.out")
            return 1

        interfaces = phase4.discover_capture_interfaces(
            context.nodes, plan
        )
        commands = build_manual_endpoint_commands(
            args, plan, run_dir, interfaces
        )
        merge_run_metadata(
            run_dir,
            {"commands": {"router": " ".join(router_cmd)}},
        )

        sections = (
            ("1) Start the four packet captures (paste into mininet>)",
             commands["captures"]),
            ("2) Start the receiver (paste into mininet>)",
             commands["receiver"]),
            ("3) Start the sender (paste into mininet>)",
             commands["sender"]),
            ("4) Inspect progress while the transfer runs (mininet>)",
             commands["progress"]),
            ("5) After the transfer: cleanup, then verification "
             "(shell or mininet>)", commands["cleanup"]),
        )

        print("\nPaste these commands in this order:")
        for title, lines in sections:
            print(f"\n{title}")
            for line in lines:
                print(f"  {line}")

        manual_lines = []
        for title, lines in sections:
            manual_lines.append(f"# {title}")
            manual_lines.extend(lines)
            manual_lines.append("")
        (run_dir / "manual_commands.txt").write_text(
            "\n".join(manual_lines) + "\n"
        )
        print(f"\ncommands saved to {run_dir / 'manual_commands.txt'}")
        print(
            "Type 'exit' in the CLI when finished; the router and the "
            "network are shut down automatically."
        )

        from mininet.cli import CLI

        CLI(context.net)
    finally:
        phase4.shutdown_interactive(context, router, run_dir)

    print("manual-validation session finished cleanly")
    return 0


def run_experiments(args):
    run_dir = phase4.prepare_run_dir(Path(args.results_root), args.run_id)
    plan = phase4.PLANS[args.topology]
    selected = parse_run_selection(args.runs)

    print(f"run directory: {run_dir}")
    print(f"topology: {args.topology}")
    print(f"aqm comparison: {', '.join(selected)} ({args.aqm_mode} mode)")

    context = (
        phase4.MininetContext(args.python, args.delay_ms)
        if args.topology == "mininet"
        else phase4.LoopbackContext(args.python)
    )

    try:
        input_path = run_dir / "input.bin"
        phase4.make_deterministic_input(input_path, args.file_bytes)
        input_sha = phase4.sha256_file(input_path)

        results = {}
        for index, aqm in enumerate(selected, start=1):
            print(
                f"[{index}/{len(selected)}] {aqm} "
                f"({args.queue_capacity} packet queue)..."
            )
            run_subdir = run_dir / aqm
            parameters = build_queue_parameters(args, aqm)
            write_json(
                run_subdir / "run_metadata.json",
                build_run_metadata(args, plan, run_subdir, aqm,
                                   input_sha, parameters),
            )

            result = phase4.run_transfer(
                context, plan, args, run_subdir, input_path, input_sha,
                queue_capacity=args.queue_capacity,
                bulk=True,
                router_command_builder=router_command_builder_for(
                    args, aqm
                ),
            )
            result["algorithm"] = aqm
            result["queue_parameters"] = parameters
            result["telemetry"] = parse_telemetry(
                run_subdir / "router_telemetry.csv"
            )
            result["timeouts"] = parse_sender_timeouts(
                run_subdir / "sender_log.csv"
            )
            merge_run_metadata(
                run_subdir,
                {
                    "commands": result["commands"],
                    "result": summarize_result(result),
                },
            )
            results[aqm] = result
            print(
                f"      success={result['success']} "
                f"reasons={result['failure_reasons']}"
            )

        summary = build_summary(args, plan, results, args.file_bytes,
                                input_sha)
        write_summary_files(run_dir, summary)

        if all((run_dir / aqm).is_dir() for aqm in AQMS):
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

        print(f"overall_success: {summary['overall_success']}")
        return 0 if summary["overall_success"] else 1
    finally:
        context.teardown()


def build_summary(args, plan, results, input_bytes, input_sha):
    summary = {
        "topology": args.topology,
        "aqm_mode": args.aqm_mode,
        "configuration": {
            "rate_mbps": args.rate_mbps,
            "access_link_delay_ms": args.delay_ms,
            "file_bytes": input_bytes,
            "input_sha256": input_sha,
            "queue_capacity_packets": args.queue_capacity,
            "sender_window_packets": args.sender_window,
            "initial_ssthresh": args.initial_ssthresh,
            "rwnd_bytes": args.rwnd_bytes,
            "red": {
                "w_q": args.red_w_q,
                "min_th": args.red_min_th,
                "max_th": args.red_max_th,
                "max_p": args.red_max_p,
                "gentle": args.red_gentle,
                "seed": args.red_seed,
            },
            "codel": {
                "target_ms": args.codel_target_ms,
                "interval_ms": args.codel_interval_ms,
            },
        },
        "runs": {},
        "criteria": {},
        "overall_success": False,
    }

    for aqm in AQMS:
        run = results.get(aqm)
        if run is None:
            continue

        entry = {
            "queue_parameters": run["queue_parameters"],
            "success": run["success"],
            "failure_reasons": run["failure_reasons"],
            "duration_s": run["duration_s"],
            "goodput_mbps": (
                input_bytes * 8 / run["duration_s"] / 1_000_000
                if run["duration_s"]
                else None
            ),
            "sha256_match": run["sha256_match"],
            "retransmissions": (
                run["sender"]["retransmissions"] if run["sender"] else None
            ),
            "timeouts": run["timeouts"],
            "rtt": (
                phase4.rtt_stats(run["sender"]["rtt_samples_ms"])
                if run["sender"]
                else None
            ),
            "telemetry": run["telemetry"],
        }
        if entry["goodput_mbps"] is not None:
            entry["link_utilisation"] = (
                entry["goodput_mbps"] / args.rate_mbps
            )
        summary["runs"][aqm] = entry

    codel_run = summary["runs"].get("codel")
    red_run = summary["runs"].get("red")
    droptail_run = summary["runs"].get("droptail")

    if codel_run and codel_run["telemetry"]["median_sojourn_ms"]:
        median = codel_run["telemetry"]["median_sojourn_ms"]
        summary["criteria"].update(
            {
                "codel_median_sojourn_ms": median,
                "codel_target_ms": args.codel_target_ms,
                "codel_median_within_4x_target": (
                    median <= 4.0 * args.codel_target_ms
                ),
            }
        )

    if red_run and red_run["telemetry"]["avg_queue_mean"] is not None:
        mean_average = red_run["telemetry"]["avg_queue_mean"]
        summary["criteria"].update(
            {
                "red_mean_avg_queue_packets": mean_average,
                "red_min_th": args.red_min_th,
                "red_max_th": args.red_max_th,
                "red_average_between_half_min_and_double_max": (
                    0.5 * args.red_min_th
                    <= mean_average
                    <= 2.0 * args.red_max_th
                ),
            }
        )

    if droptail_run and red_run and codel_run:
        queues = {
            name: summary["runs"][name]["telemetry"]["mean_queue"]
            for name in AQMS
        }
        if all(value is not None for value in queues.values()):
            summary["criteria"].update(
                {
                    "droptail_mean_queue_packets": queues["droptail"],
                    "red_mean_queue_packets": queues["red"],
                    "codel_mean_queue_packets": queues["codel"],
                    "droptail_queue_largest": (
                        queues["droptail"] > queues["red"]
                        and queues["droptail"] > queues["codel"]
                    ),
                }
            )

    summary["overall_success"] = all(
        run["success"] for run in results.values()
    )
    return summary


def write_summary_files(run_dir, summary):
    comparison = run_dir / "comparison"
    comparison.mkdir(parents=True, exist_ok=True)
    write_json(comparison / "summary.json", summary)

    lines = [
        "Phase 5 AQM comparison summary",
        f"topology: {summary['topology']}",
        f"aqm mode: {summary['aqm_mode']}",
        f"overall_success: {summary['overall_success']}",
        "",
        f"configuration: {json.dumps(summary['configuration'])}",
        "",
    ]

    for aqm in AQMS:
        run = summary["runs"].get(aqm)
        if run is None:
            continue
        telemetry = run["telemetry"]
        goodput = (
            round(run["goodput_mbps"], 3)
            if run["goodput_mbps"] is not None
            else "n/a"
        )
        lines.append(
            f"{aqm}: success={run['success']} goodput={goodput} Mbps "
            f"sha256_match={run['sha256_match']}"
        )
        if run["rtt"]:
            lines.append(
                f"  sender RTT: mean={run['rtt']['mean_ms']:.2f}ms "
                f"p95={run['rtt']['p95_ms']:.2f}ms"
            )
        mean_queue = (
            round(telemetry["mean_queue"], 2)
            if telemetry["mean_queue"] is not None
            else "n/a"
        )
        lines.append(
            f"  queue: max={telemetry['max_queue']}p "
            f"mean(enqueue-sampled)={mean_queue}p "
            f"drops={telemetry['drop']} "
            f"(full={telemetry['drops_full']} red={telemetry['drops_red']} "
            f"codel={telemetry['drops_codel']}) "
            f"marks={telemetry['mark']} "
            f"(red={telemetry['marks_red']} codel={telemetry['marks_codel']})"
        )
        if telemetry["median_sojourn_ms"] is not None:
            lines.append(
                f"  router sojourn (at dequeue): "
                f"mean={telemetry['mean_sojourn_ms']:.2f}ms "
                f"median={telemetry['median_sojourn_ms']:.2f}ms "
                f"p95={telemetry['p95_sojourn_ms']:.2f}ms "
                f"p99={telemetry['p99_sojourn_ms']:.2f}ms"
            )
        if telemetry["avg_queue_mean"] is not None:
            lines.append(
                f"  RED average queue: "
                f"mean={telemetry['avg_queue_mean']:.2f}p "
                f"max={telemetry['avg_queue_max']:.2f}p"
            )
        lines.append(
            f"  retransmissions={run['retransmissions']} "
            f"timeouts={run['timeouts']}"
        )
        if run["failure_reasons"]:
            lines.append(f"  failures: {run['failure_reasons']}")
        lines.append("")

    criteria = summary["criteria"]
    if criteria:
        lines.append("phase 5 acceptance criteria (measured):")
        if "codel_median_sojourn_ms" in criteria:
            verdict = (
                "PASS"
                if criteria["codel_median_within_4x_target"]
                else "FAIL"
            )
            lines.append(
                f"  CoDel median sojourn "
                f"{criteria['codel_median_sojourn_ms']:.2f}ms vs target "
                f"{criteria['codel_target_ms']}ms: {verdict} "
                f"(near = within 4x target)"
            )
        if "red_mean_avg_queue_packets" in criteria:
            verdict = (
                "PASS"
                if criteria["red_average_between_half_min_and_double_max"]
                else "FAIL"
            )
            lines.append(
                f"  RED mean average queue "
                f"{criteria['red_mean_avg_queue_packets']:.2f}p vs "
                f"[{criteria['red_min_th']}, {criteria['red_max_th']}]: "
                f"{verdict} (near = 0.5x min .. 2x max)"
            )
        if "droptail_queue_largest" in criteria:
            verdict = "PASS" if criteria["droptail_queue_largest"] else "FAIL"
            lines.append(
                f"  DropTail mean queue "
                f"{criteria['droptail_mean_queue_packets']:.2f}p vs RED "
                f"{criteria['red_mean_queue_packets']:.2f}p vs CoDel "
                f"{criteria['codel_mean_queue_packets']:.2f}p: {verdict} "
                f"(DropTail expected largest)"
            )
        lines.append("")
        lines.append(
            "Note: queue occupancy is sampled at enqueue events, not "
            "time-weighted; router sojourn is measured at dequeue and "
            "excludes token-bucket wait after dequeue."
        )

    (comparison / "summary.txt").write_text("\n".join(lines) + "\n")


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    validate_args(args)
    if args.run_id is None:
        stamp = time.strftime("%Y%m%d_%H%M%S")
        args.run_id = (
            f"manual_{args.aqm}_{stamp}" if args.interactive else stamp
        )
    if args.interactive:
        return run_interactive(args)
    return run_experiments(args)


if __name__ == "__main__":
    sys.exit(main())
