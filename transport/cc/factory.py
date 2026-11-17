"""Maps congestion-control mode names to controller instances."""

from transport.cc.delay_aware import DelayAwareController
from transport.cc.dctcp_style import DctcpController
from transport.cc.reno import INITIAL_CWND, INITIAL_SSTHRESH, RenoController

MODE_PARAMETERS = {
    "reno": (),
    "dctcp": ("g",),
    "delay": ("target_seconds", "increase_step"),
}

MODES = tuple(MODE_PARAMETERS)


def make_controller(mode, initial_cwnd=INITIAL_CWND, ssthresh=INITIAL_SSTHRESH,
                    **params):
    if mode not in MODE_PARAMETERS:
        raise ValueError(
            f"unknown congestion-control mode {mode!r}; "
            f"expected one of {MODES}"
        )

    unknown = sorted(set(params) - set(MODE_PARAMETERS[mode]))
    if unknown:
        raise ValueError(
            f"mode {mode!r} does not accept {', '.join(unknown)}"
        )

    if mode == "reno":
        return RenoController(initial_cwnd=initial_cwnd, ssthresh=ssthresh)
    if mode == "dctcp":
        return DctcpController(initial_cwnd=initial_cwnd, ssthresh=ssthresh,
                               **params)
    return DelayAwareController(initial_cwnd=initial_cwnd, ssthresh=ssthresh,
                                **params)
