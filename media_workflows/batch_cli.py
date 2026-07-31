import argparse
import json
from .jobs import JobQueue
from .pipeline import Pipeline
from .worker import run_one


def main():
    parser = argparse.ArgumentParser(description="Durable media-processing batch queue")
    parser.add_argument("--database", default="data/jobs.sqlite3")
    commands = parser.add_subparsers(dest="command", required=True)
    add = commands.add_parser("add")
    add.add_argument("sources", nargs="+")
    commands.add_parser("list")
    commands.add_parser("recover")
    run = commands.add_parser("run")
    run.add_argument("--output", default="outputs")
    run.add_argument("--worker", default="local")
    args = parser.parse_args()
    queue = JobQueue(args.database)
    if args.command == "add":
        result = [queue.add(source) for source in args.sources]
    elif args.command == "list":
        result = queue.list()
    elif args.command == "recover":
        result = {"recovered": queue.recover_expired()}
    else:
        result = []
        pipeline = Pipeline(args.output)
        while True:
            item = run_one(queue, pipeline, args.worker)
            if item is None:
                break
            result.append(item)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
