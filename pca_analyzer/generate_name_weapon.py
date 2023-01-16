import sys
import polars as pl


def main(path):
    combined_df = pl.scan_csv(f"{path}*.csv")

    agg = (
        combined_df.groupby(["name", "weapon"])
        .agg(
            [
                pl.all().sort_by("range").last(),
            ]
        )
        .sort("range", reverse=False)
        .collect()
    )

    print(agg.write_csv())


if __name__ == "__main__":
    try:
        path = sys.argv[1]
    except IndexError:
        print("no input directory", file=sys.stderr)
        sys.exit(1)

    main(path)
