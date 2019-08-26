import argparse
import asyncio
import asyncpg
import datetime
import logging
import os
from compare_transaction_record_data import (
    ASSISTANCE_BROKER_USASPENDING_MAP,
    DETATCHED_AWARD_PROCURMENT,
    PROCUREMENT_BROKER_USASPENDING_MAP,
    PUBLISHED_AWARD_FINANCIAL_ASSISTANCE,
    TRANSACTION_FABS,
    TRANSACTION_FPDS,
)


class FailedQuery(Exception):
    pass


logger = logging.getLogger()
logger.setLevel(logging.INFO)
ls = logging.StreamHandler()
ls.setFormatter(logging.Formatter("[%(asctime)s] <%(levelname)s> %(message)s", datefmt="%Y/%m/%d %H:%M:%S %z (%Z)"))
logger.addHandler(ls)


broker_db = os.environ["DATA_BROKER_DATABASE_URL"]
usaspending_db = os.environ["DATABASE_URL"]


async def async_run_select(sql, dsn):
    conn = await asyncpg.connect(dsn=dsn)
    sql_result = await conn.fetch(sql)
    await conn.close()
    return sql_result


def convert_record_to_safe_dict(d):
    new_dict = {}
    for k, v in d.items():
        if v is None:
            new_dict[k] = v
        elif type(v) not in (str, float, int):
            new_dict[k] = str(v)
        else:
            new_dict[k] = v
    return new_dict
    # return {k: str(v) for k, v in d.items()}


def query_systems(broker_sql, usaspending_sql):
    loop = asyncio.new_event_loop()
    broker_row = asyncio.ensure_future(async_run_select(broker_sql, broker_db), loop=loop)
    usaspending_row = asyncio.ensure_future(async_run_select(usaspending_sql, usaspending_db), loop=loop)
    try:
        loop.run_until_complete(asyncio.gather(broker_row, usaspending_row))
    except (asyncpg.exceptions.UndefinedObjectError, asyncpg.exceptions.UndefinedColumnError):
        logger.error("Failed to obtain records for comparision")
        raise FailedQuery
    loop.close()

    if len(broker_row.result()) == 0:
        logger.error("No Record from Broker")
        raise SystemExit
    if len(usaspending_row.result()) == 0:
        logger.error("No Record from USAspending")
        raise SystemExit
    return broker_row.result()[0], usaspending_row.result()[0]


def main(is_fpds, surrogate_key):
    discrepancies = 0
    if args.published_award_financial_assistance_id:
        broker_sql, usaspending_sql = get_sql_strings(is_fpds, surrogate_key)
    else:
        broker_sql, usaspending_sql = get_sql_strings(is_fpds, surrogate_key)
    try:
        broker_record, usaspending_record = query_systems(broker_sql, usaspending_sql)
    except FailedQuery:
        logger.error("UNABLE TO PROCESS {} ({})".format(surrogate_key, "FPDS" if is_fpds else "FABS"))
        raise SystemExit

    if is_fpds:
        mapper = PROCUREMENT_BROKER_USASPENDING_MAP
    else:
        mapper = ASSISTANCE_BROKER_USASPENDING_MAP

    for broker, usaspending in mapper.items():
        if broker_record[broker] != usaspending_record[usaspending]:
            discrepancies += 1
            msg = "-- [{} / {}] Broker: '{}' USAspending: '{}'"
            logger.info(msg.format(broker, usaspending, broker_record[broker], usaspending_record[usaspending]))

    if DUMP_JSON:
        import json
        print("## BROKER #######################################")
        print(json.dumps(convert_record_to_safe_dict(broker_record), indent=4, sort_keys=True))
        print("---")
        print("## USASPENDING ##################################")
        print(json.dumps(convert_record_to_safe_dict(usaspending_record), indent=4, sort_keys=True))
        print("---")

    if discrepancies:
        msg = "REPORT for ID {} ({}): {} Total discrepancies!"
        logger.warn(msg.format(surrogate_key, "FPDS" if is_fpds else "FABS", discrepancies))
    else:
        logger.info("REPORT for ID {} ({}): No discrepancies!!!".format(surrogate_key, "FPDS" if is_fpds else "FABS"))


def get_sql_strings(is_fpds, surrogate_key):
    return get_broker_string(is_fpds, surrogate_key), get_usaspending_string(is_fpds, surrogate_key)


def get_broker_string(is_fpds, surrogate_key):
    sql = "SELECT {cols} FROM {table} WHERE {table}_id = {key}"
    if is_fpds:
        return sql.format(cols=DETATCHED_AWARD_PROCURMENT, table="detached_award_procurement", key=surrogate_key)
    return sql.format(
        cols=PUBLISHED_AWARD_FINANCIAL_ASSISTANCE, table="published_award_financial_assistance", key=surrogate_key
    )


def get_usaspending_string(is_fpds, surrogate_key):
    sql = "SELECT {cols} FROM transaction_{type} WHERE {table}_id = {key}"

    if is_fpds:
        return sql.format(cols=TRANSACTION_FPDS, type="fpds", table="detached_award_procurement", key=surrogate_key)

    return sql.format(
        cols=TRANSACTION_FABS, type="fabs", table="published_award_financial_assistance", key=surrogate_key
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump-json", action="store_true")
    mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)
    mutually_exclusive_group.add_argument("--detached_award_procurement_id", type=int)
    mutually_exclusive_group.add_argument("--published_award_financial_assistance_id", type=int)
    args = parser.parse_args()

    DUMP_JSON = args.dump_json

    if args.published_award_financial_assistance_id:
        main(False, args.published_award_financial_assistance_id)
    else:
        main(True, args.detached_award_procurement_id)

    # python usaspending_api/database_scripts/job_archive/compare_transaction_record.py --detached_award_procurement_id 17459715
