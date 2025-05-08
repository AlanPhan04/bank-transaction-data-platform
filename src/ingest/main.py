import argparse
from src.ingest.plugins.Postgre import postgres_plugin
from src.ingest.plugins.MongoDB import mongodb_plugin

def run(source, table):
    if source == "postgres":
        postgres_plugin.run(table)
    elif source == "mongodb":
        mongodb_plugin.run(table)
    else:
        raise ValueError(f"Unsupported source: {source}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Data source to ingest")
    parser.add_argument("--table", required=True, help="Table to ingest")
    args = parser.parse_args()
    run(args.source, args.table)
