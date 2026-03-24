import argparse
import subprocess
import sys


COMMANDS = {
    "earnings-handler": [sys.executable, "-m", "trading.earnings_trader_lambda_handler"],
    "close-options-handler": [sys.executable, "-m", "trading.close_options_lambda_handler"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convenience entrypoints for local trading automation workflows.",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=sorted(COMMANDS),
        help="Workflow entrypoint to execute.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.command:
        print("Available commands:")
        for name in sorted(COMMANDS):
            print(f"  - {name}")
        return 0

    completed = subprocess.run(COMMANDS[args.command], check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
