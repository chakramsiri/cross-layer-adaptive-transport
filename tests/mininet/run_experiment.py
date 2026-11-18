"""Phase 8 matrix runner: one Mininet bottleneck run per cell (needs root)."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MININET_DIR = REPO_ROOT / "tests" / "mininet"
NODE = MININET_DIR / "phase2_node.py"
CROSS_NODE = MININET_DIR / "cross_traffic.py"
DEFAULT_RESULTS_ROOT = REPO_ROOT / "results" / "phase8"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(MININET_DIR) not in sys.path:
    sys.path.insert(0, str(MININET_DIR))

import phase4_experiments as phase4

PYTHON = sys.executable

RATE_MBPS = 5.0
LINK_DELAY_MS = 10.0
BASE_RTT_MS = 4 * LINK_DELAY_MS
FILE_BYTES = 5_000_000
SENDER_WINDOW = 400
INITIAL_SSTHRESH = 20.0
RWND_BYTES = 800_000
FIN_LINGER_S = 2.0
TRANSFER_TIMEOUT_S = 300.0
ROUTER_MAX_SECONDS = TRANSFER_TIMEOUT_S + 60.0
ROUTER_READY_S = 20.0
RECEIVER_READY_S = 15.0

RED_W_Q = 0.002
RED_MAX_P = 0.1
CODEL_TARGET_MS = 5.0
CODEL_INTERVAL_MS = 100.0

DCTCP_G = 0.25
DELAY_TARGET_MS = 5.0
DELAY_INCREASE_STEP = 1.0

CROSS_RATE_MBPS = 2.5
CROSS_ON_MS = 200.0
CROSS_PERIOD_MS = 1000.0

SEEDS = (1, 2, 3)
CONTROLLERS = ("reno", "dctcp", "delay")

QUEUE_VARIANTS = (
    ("droptail", "drop", CONTROLLERS),
    ("red", "mark", CONTROLLERS),
    ("codel", "mark", CONTROLLERS),
    ("red", "drop", ("reno",)),
    ("codel", "drop", ("reno",)),
)

ROUTER_SENDER_FACE = ("10.0.0.2", 5001)
ROUTER_RECEIVER_FACE = ("10.0.1.2", 5001)
RECEIVER_TARGET = ("10.0.1.1", 5001)
RECEIVER_BIND = ("0.0.0.0", 5001)
SENDER_TARGET = ("10.0.0.2", 5001)


@dataclass(frozen=True)
class Scenario:
    buffers: tuple
    cross: bool
    loss_percent: float


SCENARIOS = {
    "S1": Scenario(buffers=(200,), cross=False, loss_percent=0.0),
    "S2": Scenario(buffers=(20, 50, 100, 500), cross=False,
                   loss_percent=0.0),
    "S3": Scenario(buffers=(200,), cross=True, loss_percent=0.0),
    "S4": Scenario(buffers=(200,), cross=False, loss_percent=1.0),
}


def controller_combinations():
    return [
        (aqm, mode, controller)
        for aqm, mode, controllers in QUEUE_VARIANTS
        for controller in controllers
    ]


@dataclass(frozen=True)
class Cell:
    scenario: str
    buffer: int
    aqm: str
    aqm_mode: str
    controller: str
    seed: int

    def __post_init__(self):
        if self.scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {self.scenario!r}")
        if self.buffer not in SCENARIOS[self.scenario].buffers:
            raise ValueError(
                f"buffer {self.buffer} is not in scenario {self.scenario}"
            )
        if (self.aqm, self.aqm_mode, self.controller) not in (
            controller_combinations()
        ):
            raise ValueError(
                f"unsupported combination {self.aqm}/{self.aqm_mode}/"
                f"{self.controller}"
            )
        if self.seed < 1:
            raise ValueError("seed must be a positive integer")

    @classmethod
    def parse(cls, text):
        parts = text.split("/")
        if len(parts) != 6:
            raise ValueError(
                "expected scenario/buffer/aqm/mode/controller/seed, "
                f"got {text!r}"
            )
        scenario, buffer, aqm, aqm_mode, controller, seed = parts
        try:
            return cls(scenario, int(buffer), aqm, aqm_mode, controller,
                       int(seed))
        except ValueError as error:
            raise ValueError(f"invalid cell {text!r}: {error}") from None

    def __str__(self):
        return (
            f"{self.scenario}/{self.buffer}/{self.aqm}/{self.aqm_mode}/"
            f"{self.controller}/{self.seed}"
        )

    def key(self):
        return (self.scenario, self.buffer, self.aqm, self.aqm_mode,
                self.controller, self.seed)

    def relative_dir(self):
        return (
            Path(self.scenario) / f"buf{self.buffer}"
            / f"{self.aqm}_{self.aqm_mode}_{self.controller}"
            / f"rep{self.seed}"
        )


def enumerate_cells(scenarios=None, seeds=SEEDS):
    cells = []
    for name in sorted(SCENARIOS):
        if scenarios is not None and name not in scenarios:
            continue
        for buffer in SCENARIOS[name].buffers:
            for aqm, mode, controller in controller_combinations():
                for seed in seeds:
                    cells.append(
                        Cell(name, buffer, aqm, mode, controller, seed)
                    )
    return cells


def red_thresholds(buffer):
    return buffer / 10.0, buffer * 2 / 5


def aqm_flags(cell):
    if cell.aqm == "red":
        low, high = red_thresholds(cell.buffer)
        return [
            "--red-w-q", str(RED_W_Q),
            "--red-min-th", str(low),
            "--red-max-th", str(high),
            "--red-max-p", str(RED_MAX_P),
            "--red-seed", str(cell.seed),
        ]
    if cell.aqm == "codel":
        return [
            "--codel-target-ms", str(CODEL_TARGET_MS),
            "--codel-interval-ms", str(CODEL_INTERVAL_MS),
        ]
    return []


def controller_flags(cell):
    if cell.controller == "dctcp":
        return ["--cc-mode", "dctcp", "--dctcp-g", str(DCTCP_G)]
    if cell.controller == "delay":
        return [
            "--cc-mode", "delay",
            "--delay-target-ms", str(DELAY_TARGET_MS),
            "--delay-increase-step", str(DELAY_INCREASE_STEP),
        ]
    return ["--cc-mode", "reno"]


def controller_parameters(cell):
    if cell.controller == "dctcp":
        return {"mode": "dctcp", "parameters": {"g": DCTCP_G}}
    if cell.controller == "delay":
        return {
            "mode": "delay",
            "parameters": {
                "target_ms": DELAY_TARGET_MS,
                "increase_step": DELAY_INCREASE_STEP,
            },
        }
    return {"mode": "reno", "parameters": {}}


def format_address(address):
    return f"{address[0]}:{address[1]}"


def router_command(python, cell, run_dir, max_seconds=ROUTER_MAX_SECONDS):
    return [
        python, "-u", "-m", "router.forwarder",
        "--sender-face", format_address(ROUTER_SENDER_FACE),
        "--receiver-face", format_address(ROUTER_RECEIVER_FACE),
        "--receiver", format_address(RECEIVER_TARGET),
        "--rate-mbps", str(RATE_MBPS),
        "--queue-capacity", str(cell.buffer),
        "--aqm", cell.aqm,
        "--aqm-mode", cell.aqm_mode,
        *aqm_flags(cell),
        "--telemetry", str(Path(run_dir) / "router_telemetry.csv"),
        "--ready-file", str(Path(run_dir) / "router.ready"),
        "--max-seconds", str(max_seconds),
    ]


def sender_command(python, cell, input_path, log_path):
    return [
        python, "-u", str(NODE), "sender",
        str(input_path),
        SENDER_TARGET[0], str(SENDER_TARGET[1]),
        str(log_path),
        "--window-size", str(SENDER_WINDOW),
        "--initial-ssthresh", str(INITIAL_SSTHRESH),
        *controller_flags(cell),
    ]


def receiver_command(python, output_path, log_path):
    return [
        python, "-u", str(NODE), "receiver",
        str(output_path), RECEIVER_BIND[0], str(RECEIVER_BIND[1]),
        str(log_path),
        "--rwnd-bytes", str(RWND_BYTES),
        "--fin-linger", str(FIN_LINGER_S),
    ]


def cross_command(python, cell, log_path, duration_s):
    return [
        python, "-u", str(CROSS_NODE),
        format_address(ROUTER_SENDER_FACE),
        "--rate-mbps", str(CROSS_RATE_MBPS),
        "--on-ms", str(CROSS_ON_MS),
        "--period-ms", str(CROSS_PERIOD_MS),
        "--duration-s", str(duration_s),
        "--log", str(log_path),
    ]


def evaluate_transfer(sender_exit, receiver_exit, router_exit, sha256_match,
                      sender_completed):
    reasons = []
    if sender_exit is None:
        reasons.append("sender did not exit")
    elif sender_exit != 0:
        reasons.append(f"sender exited with {sender_exit}")
    if receiver_exit != 0:
        reasons.append("receiver did not exit cleanly")
    if router_exit != 0:
        reasons.append("router did not exit cleanly")
    if not sha256_match:
        reasons.append("output hash mismatch")
    if not sender_completed:
        reasons.append("sender log has no TRANSFER_COMPLETED")
    return reasons


def is_complete(run_dir):
    path = Path(run_dir) / "run_metadata.json"
    if not path.exists():
        return False
    metadata = json.loads(path.read_text())
    return metadata.get("result", {}).get("success") is not None


def hand_back_ownership(path, environ=None):
    environ = os.environ if environ is None else environ
    uid = environ.get("SUDO_UID")
    gid = environ.get("SUDO_GID")
    if uid is None or gid is None:
        return False
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            os.chown(os.path.join(root, name), int(uid), int(gid))
    os.chown(path, int(uid), int(gid))
    return True


def require_root(euid):
    if euid != 0:
        raise SystemExit(
            "run_experiment: Mininet needs root; re-run the matrix with sudo"
        )


class Phase8Context:
    """h1 -- router -- h2, plus h3 for CROSS traffic when requested."""

    name = "mininet"

    def __init__(self, python, loss_percent=0.0, with_cross=False):
        from mininet.link import TCLink
        from mininet.log import setLogLevel
        from mininet.net import Mininet

        setLogLevel("warning")

        net = Mininet(controller=None, link=TCLink)
        h1 = net.addHost("h1", ip="10.0.0.1/24")
        router = net.addHost("r", ip="10.0.0.2/24")
        h2 = net.addHost("h2", ip="10.0.1.1/24")

        delay = f"{LINK_DELAY_MS}ms"
        access = {"cls": TCLink, "delay": delay}
        if loss_percent > 0:
            access["loss"] = loss_percent
        net.addLink(h1, router, **access)
        net.addLink(router, h2, cls=TCLink, delay=delay)

        nodes = {"sender": h1, "router": router, "receiver": h2}
        h3 = None
        if with_cross:
            h3 = net.addHost("h3", ip="10.0.2.1/24")
            net.addLink(h3, router, cls=TCLink, delay=delay)
            nodes["cross"] = h3

        net.start()
        router.setIP("10.0.1.2/24", intf="r-eth1")
        if h3 is not None:
            router.setIP("10.0.2.2/24", intf="r-eth2")
            h3.cmd("ip route add 10.0.0.2/32 via 10.0.2.2")

        self.net = net
        self.nodes = nodes
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


def ensure_input(results_root):
    path = Path(results_root) / "input.bin"
    if not path.exists():
        phase4.make_deterministic_input(path, FILE_BYTES)
    return path, phase4.sha256_file(path)


def build_metadata(cell, run_dir, input_sha, commands, started_at):
    scenario = SCENARIOS[cell.scenario]
    return {
        "cell": {
            "scenario": cell.scenario,
            "buffer": cell.buffer,
            "aqm": cell.aqm,
            "aqm_mode": cell.aqm_mode,
            "controller": cell.controller,
            "seed": cell.seed,
        },
        "run_dir": str(run_dir),
        "started_at": started_at,
        "network": {
            "topology": "mininet",
            "rate_mbps": RATE_MBPS,
            "link_delay_ms": LINK_DELAY_MS,
            "base_rtt_ms": BASE_RTT_MS,
            "loss_percent": scenario.loss_percent,
            "cross": None if not scenario.cross else {
                "rate_mbps": CROSS_RATE_MBPS,
                "on_ms": CROSS_ON_MS,
                "period_ms": CROSS_PERIOD_MS,
            },
            "file_bytes": FILE_BYTES,
            "input_sha256": input_sha,
        },
        "queue": {
            "algorithm": cell.aqm,
            "mode": cell.aqm_mode,
            "capacity": cell.buffer,
            "flags": aqm_flags(cell),
        },
        "congestion_control": controller_parameters(cell),
        "sender": {
            "window_size": SENDER_WINDOW,
            "initial_ssthresh": INITIAL_SSTHRESH,
        },
        "receiver": {"rwnd_bytes": RWND_BYTES,
                     "fin_linger_s": FIN_LINGER_S},
        "commands": commands,
    }


def run_cell(cell, results_root, python=PYTHON):
    results_root = Path(results_root).resolve()
    run_dir = results_root / cell.relative_dir()
    if is_complete(run_dir):
        metadata = json.loads((run_dir / "run_metadata.json").read_text())
        return metadata["result"]
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    input_path, input_sha = ensure_input(results_root)
    scenario = SCENARIOS[cell.scenario]
    output_path = run_dir / "out.bin"
    sender_log = run_dir / "sender_log.csv"
    receiver_log = run_dir / "receiver_log.csv"
    cross_log = run_dir / "cross_log.csv"

    commands = {
        "router": " ".join(router_command(python, cell, run_dir)),
        "receiver": " ".join(
            receiver_command(python, output_path, receiver_log)
        ),
        "sender": " ".join(
            sender_command(python, cell, input_path, sender_log)
        ),
    }
    if scenario.cross:
        commands["cross"] = " ".join(
            cross_command(python, cell, cross_log, TRANSFER_TIMEOUT_S)
        )

    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    metadata = build_metadata(cell, run_dir, input_sha, commands, started_at)
    (run_dir / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n"
    )

    result = execute_transfer(cell, run_dir, input_path, input_sha, python)

    metadata["result"] = result
    (run_dir / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n"
    )
    return result


def execute_transfer(cell, run_dir, input_path, input_sha, python):
    scenario = SCENARIOS[cell.scenario]
    result = {
        "success": False,
        "failure_reasons": [],
        "duration_s": None,
        "goodput_mbps": None,
        "sha256_match": None,
        "sender_exit": None,
        "receiver_exit": None,
        "router_exit": None,
    }
    processes = {"router": None, "receiver": None, "cross": None,
                 "sender": None}

    context = Phase8Context(python, loss_percent=scenario.loss_percent,
                            with_cross=scenario.cross)
    try:
        run_roles(cell, run_dir, input_path, python, context, processes,
                  result)
    finally:
        finish(result, run_dir, input_sha, processes)
        context.teardown()
    return result


def run_roles(cell, run_dir, input_path, python, context, processes, result):
    scenario = SCENARIOS[cell.scenario]

    processes["router"] = context.launch(
        "router", router_command(python, cell, run_dir),
        run_dir / "router.out",
    )
    if not phase4.wait_for((run_dir / "router.ready").exists,
                           processes["router"], ROUTER_READY_S):
        result["failure_reasons"].append("router never became ready")
        return

    processes["receiver"] = context.launch(
        "receiver",
        receiver_command(python, run_dir / "out.bin",
                         run_dir / "receiver_log.csv"),
        run_dir / "receiver.out",
    )
    if not phase4.wait_for(
        lambda: "Receiver listening"
        in (run_dir / "receiver.out").read_text(errors="replace"),
        processes["receiver"], RECEIVER_READY_S,
    ):
        result["failure_reasons"].append("receiver never listened")
        return

    if scenario.cross:
        processes["cross"] = context.launch(
            "cross",
            cross_command(python, cell, run_dir / "cross_log.csv",
                          TRANSFER_TIMEOUT_S),
            run_dir / "cross.out",
        )

    started = time.monotonic()
    processes["sender"] = context.launch(
        "sender",
        sender_command(python, cell, input_path,
                       run_dir / "sender_log.csv"),
        run_dir / "sender.out",
    )
    try:
        processes["sender"].wait(timeout=TRANSFER_TIMEOUT_S)
        result["duration_s"] = time.monotonic() - started
    except subprocess.TimeoutExpired:
        processes["sender"].kill()
        processes["sender"].wait()
        result["failure_reasons"].append("transfer timed out")


def finish(result, run_dir, input_sha, processes):
    if processes["cross"] is not None:
        phase4.stop_process(processes["cross"])
    receiver = processes["receiver"]
    if receiver is not None:
        try:
            receiver.wait(timeout=FIN_LINGER_S + 30.0)
        except subprocess.TimeoutExpired:
            receiver.kill()
            receiver.wait()
    if processes["router"] is not None:
        result["router_exit"] = phase4.stop_process(processes["router"])

    sender = processes["sender"]
    result["sender_exit"] = sender.returncode if sender is not None else None
    result["receiver_exit"] = (
        receiver.returncode if receiver is not None else None
    )

    output = run_dir / "out.bin"
    match = False
    if output.exists():
        match = phase4.sha256_file(output) == input_sha
        if match:
            output.unlink()
    result["sha256_match"] = match

    sender_log = run_dir / "sender_log.csv"
    completed = sender_log.exists() and "TRANSFER_COMPLETED" in (
        sender_log.read_text(errors="replace")
    )

    result["failure_reasons"] += evaluate_transfer(
        sender_exit=result["sender_exit"],
        receiver_exit=result["receiver_exit"],
        router_exit=result["router_exit"],
        sha256_match=match,
        sender_completed=completed,
    )
    result["success"] = not result["failure_reasons"]
    if result["success"] and result["duration_s"]:
        result["goodput_mbps"] = (
            FILE_BYTES * 8 / result["duration_s"] / 1_000_000
        )
    return result


def build_parser():
    parser = argparse.ArgumentParser(
        description="Phase 8 Mininet experiment matrix (requires root)."
    )
    parser.add_argument("--cell", default=None,
                        help="run one cell: scenario/buffer/aqm/mode/cc/seed")
    parser.add_argument("--scenarios", default=None,
                        help="comma-separated subset of S1,S2,S3,S4")
    parser.add_argument("--seeds", default=None,
                        help="comma-separated repetition seeds")
    parser.add_argument("--results-root", default=str(DEFAULT_RESULTS_ROOT))
    parser.add_argument("--python", default=PYTHON)
    parser.add_argument("--list", action="store_true",
                        help="print the selected cells and exit")
    return parser


def select_cells(args):
    if args.cell:
        return [Cell.parse(args.cell)]
    scenarios = None
    if args.scenarios:
        scenarios = [name.strip() for name in args.scenarios.split(",")]
        unknown = set(scenarios) - set(SCENARIOS)
        if unknown:
            raise SystemExit(f"unknown scenarios: {sorted(unknown)}")
    seeds = SEEDS
    if args.seeds:
        seeds = tuple(int(value) for value in args.seeds.split(","))
    return enumerate_cells(scenarios=scenarios, seeds=seeds)


def main(argv=None):
    args = build_parser().parse_args(argv)
    cells = select_cells(args)

    if args.list:
        for cell in cells:
            print(cell)
        print(f"{len(cells)} cells")
        return 0

    require_root(os.geteuid())
    results_root = Path(args.results_root).resolve()
    results_root.mkdir(parents=True, exist_ok=True)

    failures = 0
    try:
        failures = run_matrix(cells, results_root, args.python)
    finally:
        hand_back_ownership(results_root)

    print(f"matrix finished: {len(cells)} cells, {failures} failed runs",
          flush=True)
    return 0 if failures == 0 else 1


def run_matrix(cells, results_root, python):
    failures = 0
    for index, cell in enumerate(cells, start=1):
        if is_complete(results_root / cell.relative_dir()):
            print(f"[{index}/{len(cells)}] {cell} already complete",
                  flush=True)
            continue
        print(f"[{index}/{len(cells)}] {cell} ...", flush=True)
        result = run_cell(cell, results_root, python=python)
        if not result["success"]:
            failures += 1
        print(
            f"      success={result['success']} "
            f"duration={result['duration_s']} "
            f"reasons={result['failure_reasons']}",
            flush=True,
        )
    return failures


if __name__ == "__main__":
    sys.exit(main())
