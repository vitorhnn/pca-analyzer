import gzip
import polars as pl
import json
import sys
from dateutil import parser


def read_ocap_json(path):
    with gzip.open(path, "rb") as file:
        return json.load(file)


def get_players(ocap_log):
    entities = pl.DataFrame(ocap_log["entities"])

    return entities


def get_hit_events(ocap_log):
    # due to some polars limitations we have to do some processing in python here
    def default(x, y):
        try:
            return x()
        except IndexError:
            return y

    hit_events = [
        [x[0], x[1], x[2], x[3][0], default(lambda: x[3][1], "Unknown"), x[4]]
        for x in ocap_log["events"]
        if x[1] == "hit" and x[3][0] != "null"
    ]

    return pl.DataFrame(
        hit_events,
        orient="row",
        columns=["time", "type", "victim", "causedBy", "weapon", "range"],
    )


def main(path):
    log = read_ocap_json(path)
    date = parser.parse(log["times"][0]["systemTimeUTC"] + "Z").strftime("%Y-%m-%d")
    opname = log["missionName"]

    entities = get_players(log).lazy()
    hit_events = get_hit_events(log).lazy()

    merged = (
        hit_events.join(entities, left_on="causedBy", right_on="id")
        .join(entities, left_on="victim", right_on="id", suffix="_victim")
        .filter(pl.col("isPlayer") == True)
        .select(["time", "name", "weapon", "range", "type_victim"])
        .with_columns(
            [
                pl.lit(date).alias("operation_date"),
                pl.lit(opname).alias("operation_name"),
            ]
        )
    )

    print(merged.collect().write_csv(None))


if __name__ == "__main__":
    try:
        path = sys.argv[1]
    except IndexError:
        print("no input file", file=sys.stderr)
        sys.exit(1)

    main(path)
