import sys

from tap import Tap

from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_dewar_lut
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext


class Arguments(Tap):
    """Create the Hostal DB"""

    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    db_echo: bool = False  # output SQL statements?


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()
    dbcontext = DBContext(args.db_connection_url, args.db_echo)
    pucks = table_pucks(dbcontext.metadata)
    crystals = table_crystals(dbcontext.metadata, pucks)
    table_dewar_lut(dbcontext.metadata, pucks)
    table_diffractions(dbcontext.metadata, crystals)
    table_data_reduction(dbcontext.metadata, crystals)
    dbcontext.create_all(CreationMode.CHECK_FIRST)
    with dbcontext.connect() as conn:
        conn.execute(
            "CREATE VIEW Crystal_View AS select `c`.`crystal_id` AS `crystal_id`,"
            "`c`.`puck_id` AS `puck_id`,"
            "`c`.`puck_position_id` AS `puck_position_id`,`p`.`puck_type` AS "
            "`puck_type`,`p`.`owner` AS `puck_owner`,`d`.`run_id` AS `run_id`,`d`.`created` AS `diffraction_created`,"
            "`d`.`beamline` AS `beamline`,`d`.`dewar_position` AS `dewar_position`,`d`.`beam_intensity` AS "
            "`beam_intensity`,`d`.`pinhole` AS `pinhole`,`d`.`focusing` AS `focusing`,`d`.`comment` AS `comment`,"
            "`d`.`metadata` AS `metadata`,`d`.`diffraction` AS `diffraction`,`d`.`data_raw_filename_pattern` AS "
            "`data_raw_filename_pattern`,`d`.`microscope_image_filename_pattern` AS "
            "`microscope_image_filename_pattern` from `Crystals` `c` left join `Diffractions` `d` "
            "on `d`.`crystal_id` = `c`.`crystal_id` left join `Pucks` `p` on `c`.`puck_id` = `p`.`puck_id`"
            "group by `c`.`crystal_id`,`d`.`run_id`"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
