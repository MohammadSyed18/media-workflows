import argparse, json
from .pipeline import Pipeline


def main():
    parser = argparse.ArgumentParser(
        description="Create web-ready video renditions and a poster"
    )
    parser.add_argument("source")
    parser.add_argument("--output", default="outputs")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            Pipeline(args.output, workers=args.workers).process(
                args.source, dry_run=args.dry_run
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
