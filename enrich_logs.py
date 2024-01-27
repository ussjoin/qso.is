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
    Float,
    String,
    DateTime,
    bindparam,
)
from sqlalchemy.sql.expression import func
from gridtools import grid_distance, Grid
import ctyparser
import json

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
    Column("enr_distance", Float),
)

dx_entity_table = Table(
    "dx_entities",
    metadata_obj,
    Column("callsign", String, nullable=False),
    Column("dx_entity", String, nullable=False),
    Column("continent_abbrev", String, nullable=False),
    Column("country_code", String, nullable=False),
    Column("rarity", Integer, nullable=False),
)


class DxEntityFinder:
    def __init__(self, ctyfilepath=None, dxccjsonfilepath=None, clublogmwfilepath=None):
        self.cty = ctyparser.BigCty()
        self.dxcc_to_country_map = {}
        self.dxcc_to_rarity_map = {}

        if ctyfilepath is not None:
            self.cty.import_dat(ctyfilepath)
            print(f"Loaded BigCTY, Version {self.cty.formatted_version}")
        else:
            raise Exception(
                "A BigCTY file path must be provided to initialize the DxEntityFinder."
            )

        if dxccjsonfilepath is not None:
            with open(dxccjsonfilepath, "r") as f:
                big_array = json.load(f)
                for item in big_array["dxcc"]:
                    self.dxcc_to_country_map[item["name"]] = item

                # Here, we'll add *ahem* misalignments between country names.
                # These were caused by the dxcc.json guy taking a "loose" approach to naming.
                # Or someone, anyway. Sigh. Data is hard.
                # Left hand side of the = in this list is the cty.dat name.
                self.dxcc_to_country_map["United States"] = self.dxcc_to_country_map[
                    "United States of America"
                ]
                self.dxcc_to_country_map[
                    "Fed. Rep. of Germany"
                ] = self.dxcc_to_country_map["Germany"]
                self.dxcc_to_country_map["Pitcairn Island"] = self.dxcc_to_country_map[
                    "Pitcairn Islands"
                ]
                self.dxcc_to_country_map["Curacao"] = self.dxcc_to_country_map[
                    "Curaçao"
                ]
                self.dxcc_to_country_map[
                    "Wallis & Futuna Islands"
                ] = self.dxcc_to_country_map["Wallis and Futuna Islands"]
                self.dxcc_to_country_map[
                    "Republic of Korea"
                ] = self.dxcc_to_country_map["South Korea"]
                self.dxcc_to_country_map["Western Kiribati"] = self.dxcc_to_country_map[
                    "Gilbert Islands"
                ]
                self.dxcc_to_country_map["Eastern Kiribati"] = self.dxcc_to_country_map[
                    "Line Islands"
                ]
                self.dxcc_to_country_map["Central Kiribati"] = self.dxcc_to_country_map[
                    "Phoenix Islands"
                ]
                self.dxcc_to_country_map[
                    "Antigua & Barbuda"
                ] = self.dxcc_to_country_map["Antigua and Barbuda"]
                self.dxcc_to_country_map[
                    "Saba & St. Eustatius"
                ] = self.dxcc_to_country_map["Saba and Sint Eustatius"]
                self.dxcc_to_country_map["Madeira Islands"] = self.dxcc_to_country_map[
                    "Madeira"
                ]
                self.dxcc_to_country_map[
                    "Trinidad & Tobago"
                ] = self.dxcc_to_country_map["Trinidad and Tobago"]
                self.dxcc_to_country_map["St. Lucia"] = self.dxcc_to_country_map[
                    "Saint Lucia"
                ]
                self.dxcc_to_country_map["Vietnam"] = self.dxcc_to_country_map[
                    "Viet Nam"
                ]
                self.dxcc_to_country_map["St. Vincent"] = self.dxcc_to_country_map[
                    "Saint Vincent and the Grenadines"
                ]
                self.dxcc_to_country_map["Slovak Republic"] = self.dxcc_to_country_map[
                    "Slovakia"
                ]
                self.dxcc_to_country_map[
                    "Republic of Kosovo"
                ] = self.dxcc_to_country_map["Kosovo"]
                self.dxcc_to_country_map[
                    "Sicily (not DXCC)"
                ] = self.dxcc_to_country_map["Italy"]
                self.dxcc_to_country_map[
                    "Tristan da Cunha & Gough"
                ] = self.dxcc_to_country_map["Tristan da Cunha and Gough Islands"]
                self.dxcc_to_country_map["Timor - Leste"] = self.dxcc_to_country_map[
                    "Timor-Leste"
                ]
                self.dxcc_to_country_map[
                    "St. Kitts & Nevis"
                ] = self.dxcc_to_country_map["Saint Kitts and Nevis"]
        else:
            raise Exception(
                "A DXCC.json file path must be provided to initialize the DxEntityFinder."
            )

        if clublogmwfilepath is not None:
            with open(clublogmwfilepath, "r") as f:
                lines = f.readlines()
                lines = lines[1:]  # Trim off header
                # Format: rank \t prefix \t name
                # Names are all upper case, because...

                for line in lines:
                    arr = line.split("\t")
                    # print(f"<{arr[2].strip()}>")
                    self.dxcc_to_rarity_map[arr[2].strip()] = int(arr[0])

                # Yay, more data inconsistency!
                self.dxcc_to_rarity_map["UNITED STATES"] = self.dxcc_to_rarity_map[
                    "UNITED STATES OF AMERICA"
                ]
                self.dxcc_to_rarity_map[
                    "FED. REP. OF GERMANY"
                ] = self.dxcc_to_rarity_map["FEDERAL REPUBLIC OF GERMANY"]
                self.dxcc_to_rarity_map["FIJI"] = self.dxcc_to_rarity_map[
                    "FIJI ISLANDS"
                ]
                self.dxcc_to_rarity_map["ST. LUCIA"] = self.dxcc_to_rarity_map[
                    "SAINT LUCIA"
                ]
                self.dxcc_to_rarity_map["VIETNAM"] = self.dxcc_to_rarity_map["VIET NAM"]
                self.dxcc_to_rarity_map["SOUTH AFRICA"] = self.dxcc_to_rarity_map[
                    "REPUBLIC OF SOUTH AFRICA"
                ]
                self.dxcc_to_rarity_map["ST. VINCENT"] = self.dxcc_to_rarity_map[
                    "SAINT VINCENT"
                ]
                self.dxcc_to_rarity_map["SICILY (NOT DXCC)"] = self.dxcc_to_rarity_map[
                    "ITALY"
                ]
                self.dxcc_to_rarity_map[
                    "TRISTAN DA CUNHA & GOUGH"
                ] = self.dxcc_to_rarity_map["TRISTAN DA CUNHA & GOUGH ISLANDS"]
                self.dxcc_to_rarity_map["TIMOR - LESTE"] = self.dxcc_to_rarity_map[
                    "TIMOR-LESTE"
                ]
                self.dxcc_to_rarity_map[
                    "SABA & ST. EUSTATIUS"
                ] = self.dxcc_to_rarity_map["SABA & ST EUSTATIUS"]
                self.dxcc_to_rarity_map["ST. KITTS & NEVIS"] = self.dxcc_to_rarity_map[
                    "SAINT KITTS & NEVIS"
                ]
                self.dxcc_to_rarity_map["ST. HELENA"] = self.dxcc_to_rarity_map[
                    "SAINT HELENA"
                ]

        else:
            raise Exception(
                "A Clublog MW file path must be provided to initialize the DxEntityFinder."
            )

    def _find_country(self, query):
        # Taken, with my gratitude, from
        # https://github.com/miaowware/qrm2/blob/master/exts/dxcc.py#L41-L57
        # By suggestion of the miaowware crew
        # Keys on the query dict: https://github.com/miaowware/ctyparser/blob/master/ctyparser/bigcty.py#L116
        query = query.upper()
        full_query = query
        while query:
            if query in self.cty.keys():
                data = self.cty[query]
                return (data["entity"], data["continent"])
            else:
                query = query[:-1]
        return None

    def find_one_dx_entity(self, callsign):
        ret = {"callsign": callsign, "dx_entity": None, "continent_abbrev": None}

        result = self._find_country(callsign)
        if result:
            ret["dx_entity"] = result[0]
            ret["continent_abbrev"] = result[1]
            try:
                ret["country_code"] = self.dxcc_to_country_map[result[0]]["countryCode"]
            except KeyError:
                print(
                    f"Unable to find DXCC.json mapping for <{result[0]}>, rendering country code as XX"
                )
                ret["country_code"] = "XX"
            try:
                ret["rarity"] = self.dxcc_to_rarity_map[result[0].upper()]
            except KeyError:
                print(
                    f"Unable to find ClublogMW mapping for <{result[0]}>, rendering rarity as 9999"
                )
                ret["rarity"] = 9999
        return ret

    def do_find_dx_entities(self):
        with engine.connect() as conn:
            # This dirty trick adapted from https://stackoverflow.com/a/67518038
            stmt = select(qso_table.c.callsign).except_(
                select(dx_entity_table.c.callsign)
            )

            callsign_entity_structs = [
                self.find_one_dx_entity(row.callsign) for row in conn.execute(stmt)
            ]

            for s in callsign_entity_structs:
                if not "country_code" in s:
                    # That means the callsign is likely invalid.
                    # Example: C07HH.
                    callsign_entity_structs.remove(s)

            if len(callsign_entity_structs) > 0:
                stmt = insert(dx_entity_table).values(
                    callsign=bindparam("callsign"),
                    dx_entity=bindparam("dx_entity"),
                    continent_abbrev=bindparam("continent_abbrev"),
                    country_code=bindparam("country_code"),
                )

                conn.execute(stmt, callsign_entity_structs)
                conn.commit()


def calculate_distances():
    row_updates = []
    with engine.connect() as conn:
        stmt = select(
            qso_table.c.callsign, qso_table.c.station_loc, qso_table.c.callsign_loc
        ).where(
            qso_table.c.station_loc != None,
            qso_table.c.callsign_loc != None,
            qso_table.c.enr_distance == None,
        )
        # print(stmt)

        for row in conn.execute(stmt):
            dist_and_bearing = grid_distance(
                Grid(row.station_loc), Grid(row.callsign_loc)
            )
            if dist_and_bearing and dist_and_bearing[0]:
                row_updates.append(
                    {"b_callsign": row.callsign, "enr_distance": dist_and_bearing[0]}
                )

    if len(row_updates) > 0:
        with engine.begin() as conn:
            stmt = (
                update(qso_table)
                .where(qso_table.c.callsign == bindparam("b_callsign"))
                .values(
                    enr_distance=bindparam("enr_distance"),
                )
            )
            conn.execute(stmt, row_updates)


if __name__ == "__main__":
    print("Now enriching stored logs.")
    dx_finder = DxEntityFinder(
        ctyfilepath="datasources/cty.dat",
        dxccjsonfilepath="datasources/dxcc.json",
        clublogmwfilepath="datasources/clublog_most_wanted.tsv",
    )
    dx_finder.do_find_dx_entities()
    print("DX Entity finder complete.")
    calculate_distances()
    print("Distance calculator complete.")
