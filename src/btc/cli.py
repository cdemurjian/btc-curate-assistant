from __future__ import annotations

import argparse

from btc.curate import cli as curate_cli
from btc.curate.scripts import pull_gbm_mongo, rename_s3_parentheses
from btc.fetch import cli as fetch_cli


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="btc",
        description="BTC curation and data operations toolkit.",
    )
    parser.add_argument("domain", nargs="?", choices=["curate", "fetch"], help="Tool family to run.")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments for the selected tool family.")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.domain == "curate":
        curate_args = list(args.args)
        if curate_args[:1] == ["rename"]:
            rename_s3_parentheses.main(curate_args[1:])
            return
        if curate_args[:1] == ["mongo"]:
            mongo_args = curate_args[1:]
            if mongo_args[:1] == ["export"]:
                pull_gbm_mongo.main(["export", *mongo_args[1:]])
                return
            if mongo_args:
                pull_gbm_mongo.main(mongo_args)
                return
            pull_gbm_mongo.main(["--help"])
            return
        curate_cli.main(curate_args)
        return

    if args.domain == "fetch":
        fetch_cli.main(args.args)
        return

    parser.print_help()
