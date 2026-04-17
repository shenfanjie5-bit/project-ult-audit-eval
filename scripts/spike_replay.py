"""Offline replay spike bound to fixture manifests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from audit_eval.audit.replay import reconstruct_replay_view


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycle-id", required=True)
    parser.add_argument("--object-ref", required=True)
    parser.add_argument(
        "--fixtures",
        type=Path,
        required=True,
        help="Root directory containing spike cycle fixtures.",
    )
    args = parser.parse_args(argv)

    replay_view = reconstruct_replay_view(
        cycle_id=args.cycle_id,
        object_ref=args.object_ref,
        fixture_root=args.fixtures,
    )
    json.dump(replay_view, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
