import argparse
from ingest import ingest
from transform import transform
import time

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="run data pipeline")
    parser.add_argument('--filter', help='filter based on type; eg: keep water type pokemon')
    parser.add_argument('--count', help='fetch the first 20 pokemons for example')
    args = parser.parse_args()

    print("start pipeline ...")
    
    ingest_exec_time = time.perf_counter()
    count = int(args.count) if args.count else None
    fetched_pok = ingest(count=count)
    ingest_exec_time = time.perf_counter()-ingest_exec_time 
    ingest_status = "success"
    if not fetched_pok:
        ingest_status = "failed!"
        ingest_exec_time = 0.0
    
    trans_exec_time = time.perf_counter()
    processed_pok = transform(filter_type=args.filter)
    trans_exec_time = time.perf_counter()-trans_exec_time 
    if trans_exec_time > 0:
        trans_status = "success"
    else:
        trans_status = "failed"

    report_lines = [
        "final report",
        f"-ingestion: {ingest_status}",
        f"-execution time of ingestion: {ingest_exec_time}",
        f"-transformation and validation: {trans_status}",
        f"-execution time transformation and validation: {trans_exec_time}",
        f"-number of fetched pokemons: {fetched_pok}",
        f"-numbers of pokemons processed: {processed_pok}"
    ]

    for line in report_lines:
        print(line)
