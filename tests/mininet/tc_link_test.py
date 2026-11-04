"""Manual Mininet smoke test: two hosts, one switch, 50 ms TCLink, ping.

Mininet network creation requires root and must be an explicit action, so
this module never starts a network on import and pytest collects nothing
from it. Run it manually in the course VM:

    sudo -E python tests/mininet/tc_link_test.py

Exit codes: 0 = success, 1 = smoke test failed, 2 = Mininet missing or
not running as root.
"""

import os
import sys


def run_smoke_test():
    """Build h1 - s1 - h2 with 50 ms links and ping across it."""

    try:
        from mininet.net import Mininet
        from mininet.node import OVSBridge
        from mininet.link import TCLink
        from mininet.log import setLogLevel
    except ImportError as error:
        print(f"Mininet is not available: {error}")
        print("Install Mininet (or run this in the course VM) first.")
        return 2

    if hasattr(os, "geteuid") and os.geteuid() != 0:
        print("Mininet needs root to create namespaces: re-run with sudo.")
        return 2

    setLogLevel("info")

    net = Mininet(
        controller=None,
        switch=OVSBridge,
        link=TCLink
    )

    h1 = net.addHost("h1")
    h2 = net.addHost("h2")
    s1 = net.addSwitch("s1")

    net.addLink(h1, s1, cls=TCLink, delay="50ms")
    net.addLink(h2, s1, cls=TCLink, delay="50ms")

    try:
        net.start()
    except Exception as error:
        print(f"Mininet smoke test failed to start: {error}")
        try:
            net.stop()
        except Exception:
            pass
        return 1

    try:
        print("\n--- Testing connectivity ---")
        print(h1.cmd("ping -c 3", h2.IP()))
    finally:
        net.stop()

    return 0


if __name__ == "__main__":
    sys.exit(run_smoke_test())
