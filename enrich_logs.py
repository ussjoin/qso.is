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
from gridtools import grid_distance, Grid
import ctyparser

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

dx_entity_table = Table(
    "dx_entities",
    metadata_obj,
    Column("callsign", String, nullable=False),
    Column("dx_entity", String, nullable=False),
    Column("continent_abbrev", String, nullable=False),
)


class DxEntityFinder:
    def __init__(self, filepath=None):
        self.cty = ctyparser.BigCty()
        if filepath is not None:
            self.cty.import_dat(filepath)
            print(f"Loaded BigCTY, Version {self.cty.formatted_version}")
        else:
            raise Exception(
                "A BigCTY file path must be provided to initialize the DxEntityFinder."
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

        return ret

    def do_find_dx_entities(self):
        with engine.connect() as conn:
            # This dirty trick adapted from https://stackoverflow.com/a/67518038
            res = conn.execute(
                text(
                    """ 
                    SELECT callsign
                    FROM 
                    (
                        SELECT qso.callsign FROM qso 

                        UNION ALL

                        SELECT qso.callsign FROM qso 
                        INNER JOIN dx_entities ON qso.callsign = dx_entities.callsign
                    ) tbl
                    GROUP BY callsign
                    HAVING count(*) = 1
                    ORDER BY callsign"""
                )
            )
            callsign_entity_structs = [self.find_one_dx_entity(x[0]) for x in res.all()]
            
            if len(callsign_entity_structs) > 0:
                stmt = insert(dx_entity_table).values(
                    callsign=bindparam("callsign"),
                    dx_entity=bindparam("dx_entity"),
                    continent_abbrev=bindparam("continent_abbrev"),
                )
            
                conn.execute(stmt, callsign_entity_structs)
                conn.commit()


if __name__ == "__main__":
    print("Now enriching stored logs.")
    dx_finder = DxEntityFinder("cty.dat")
    dx_finder.do_find_dx_entities()
