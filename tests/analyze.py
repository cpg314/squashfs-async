#!/usr/bin/env python3
import polars as pl

pl.Config.set_tbl_rows(100)
for f in ["testdata.json", "record-zstd.json", "record-nocomp.json"]:
    print(f)
    x = pl.read_json(f)

    for spec in x.select(pl.col("spec")).unique().rows():
        print(spec)
        x2 = x.filter(pl.col("spec") == spec[0])
        x2 = x2.with_columns(pl.col("duration_ms").map(lambda s: s.struct.field("mean")))
        x2 = x2.with_columns((pl.col("filesize") / pl.col("duration_ms") / 1e6 * 1e3).round(0).alias("speed_mb_s"))
        baseline = x2.filter((pl.col("mount_name") == "squashfuse") & (pl.col("n_chunks") == 1))[0]["duration_ms"]
        print(x2.with_columns((pl.col("duration_ms") / baseline).round(2)))
    print("-" * 50)
