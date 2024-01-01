#! env python3

import argparse
import csv
from datetime import datetime
import json
import sys
import uuid
import sqlite3
from sqlalchemy import (
    create_engine,
    text,
    select,
    update,
    insert,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    bindparam,
)
from sqlalchemy.sql.expression import func
import adif_io  # https://gitlab.com/andreas_krueger_py/adif_io

engine = create_engine("postgresql+psycopg2://localhost:5433/ussjoin")
metadata_obj = MetaData()

qso_table = Table(
    "qso",
    metadata_obj,
    Column("id", String, primary_key=True),
    Column("user_id", String, nullable=False),
    Column("station", String, nullable=False),
    Column("callsign", String, nullable=False),
    Column("epoch", Integer, nullable=False),
    Column("station_loc", String),
    Column("callsign_loc", String),
    Column("frequency", String, nullable=False),
    Column("power", String),
    Column("mode", String, nullable=False),
    Column("s_report", String),
    Column("r_report", String),
    Column("original", String),
)


def parse_adif(file_object):
    """parse_adif(file_object):

    TODO description

    """

    # What we need for a QSL Card:
    ## Date
    ## Time (UTC)
    ## Their Callsign
    ## My QTH
    ## Frequency
    ## Power
    ## Mode
    ## Signal Reports

    # Optionally add:
    ## My POTA Location

    qsos_parsed = []

    # TODO: Handle ADIF header

    for line in file_object.readlines():
        # [0] because the read_from_string() returns a tuple of qsos_raw, adif_header
        # (We don't need the headers for this, so just dropping them on the floor)
        # Second [0] to get the only object in the array of len(1)
        qso = adif_io.read_from_string(line)[0][0]
        if not qso:
            next
        q_p = {}
        q_p["station"] = qso.get("STATION_CALLSIGN")
        q_p["callsign"] = qso.get("CALL")
        q_p["day"] = qso.get("QSO_DATE")
        q_p["day"] = f"{q_p['day'][0:4]}-{q_p['day'][4:6]}-{q_p['day'][6:8]}"

        q_p["time"] = qso.get("TIME_ON")
        q_p["time"] = f"{q_p['time'][0:2]}:{q_p['time'][2:4]}:{q_p['time'][4:6]}Z"
        q_p["epoch"] = adif_io.time_on(qso).strftime("%s")

        q_p["station_grid"] = qso.get("MY_GRIDSQUARE")
        if q_p["station_grid"] is None:
            print(
                f"The QSO with {q_p['callsign']} does not contain a MY_GRIDSQUARE, quitting."
            )
            sys.exit(1)

        q_p["call_grid"] = qso.get("GRIDSQUARE")

        q_p["frequency"] = qso.get("FREQ")

        q_p["power"] = qso.get("TX_PWR")
        if q_p["power"] and q_p["power"][-1].upper() == "W":
            q_p["power"] = q_p["power"][0:-1]  # Remove the W

        q_p["mode"] = qso.get("MODE")
        q_p["signals"] = qso.get("RST_SENT")
        q_p["signalr"] = qso.get("RST_RCVD")
        # If there wasn't a given key, Python handles that by setting it as "None".
        # Change that to just a None.
        if q_p["signalr"] == "None":
            q_p["signalr"] = None
        if q_p["signals"] == "None":
            q_p["signals"] = None
        if q_p["call_grid"] == "None":
            q_p["call_grid"] = None

        q_p["original_line"] = line
        qsos_parsed.append(q_p)

    print(f"Parsing file complete. Beginning DB check for {len(qsos_parsed)} records.")

    to_insert = []

    for q_p in qsos_parsed:
        # Dupe Check

        data = {
            "id": str(uuid.uuid4()),
            "user_id": "ussjoin", # TODO: Multiuser?
            "station": q_p["station"],
            "callsign": q_p["callsign"],
            "epoch": q_p["epoch"],
            "station_loc": q_p["station_grid"],
            "callsign_loc": q_p["station_grid"],
            "frequency": q_p["frequency"],
            "power": q_p["power"],
            "mode": q_p["mode"],
            "s_report": q_p["signals"],
            "r_report": q_p["signalr"],
            "original": q_p["original_line"],
        }

        # TODO error checking
        with engine.connect() as conn:
            stmt = select(func.count()).where(qso_table.c.original == data["original"])
            returns = conn.execute(stmt).scalar()
        if returns == 0:
            # So we didn't find anyone
            to_insert.append(data)
        else:
            # print(f"Found one for <<{stmt}>> <<{returns}>>")
            pass

    print(f"DB check complete. Beginning DB insert of {len(to_insert)} records.")

    # TODO error checking

    stmt = insert(qso_table).values(
        id=bindparam("id"),
        user_id=bindparam("user_id"),
        station=bindparam("station"),
        callsign=bindparam("callsign"),
        station_loc=bindparam("station_loc"),
        callsign_loc=bindparam("callsign_loc"),
        frequency=bindparam("frequency"),
        power=bindparam("power"),
        mode=bindparam("mode"),
        s_report=bindparam("s_report"),
        r_report=bindparam("r_report"),
        original=bindparam("original"),
    )

    if len(to_insert) > 0:
        with engine.begin() as conn:
            conn.execute(stmt, to_insert)

    print("DB insert complete.")

    return qsos_parsed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Turn an ADIF into DB records.")
    parser.add_argument(
        "-f", "--file", metavar="filename", help="the path to the ADIF file", type=open
    )

    # TODO add POTA support

    args = parser.parse_args()

    if args.file:
        qsos = parse_adif(args.file)
    else:
        print("You need to use the -f option. Use -h for help.")
        sys.exit(1)
