from __future__ import annotations

import argparse

from btc.qc import coverage

COMMANDS = {"coverage": coverage.main}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="btc qc", description="BTC sequencing QC tools.")
    parser.add_argument("command", nargs="?", choices=sorted(COMMANDS), help="QC tool to run.")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments for the selected tool.")
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return
    COMMANDS[args.command](args.args)
