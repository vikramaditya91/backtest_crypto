import pathlib
import sqlite3


def merger():
    source_dir = pathlib.Path(__file__).parents[4] / 's3_sync' / 't1'

    main_file = "1h__25-01-2018_00-00-00__06-03-2018_11-12-00.db"
    merged_table = source_dir / main_file #f"merged_{source_dir.name}.db"

    for item in source_dir.iterdir():
        if main_file in item.name:
            continue

        con3 = sqlite3.connect(merged_table)

        con3.execute(f"ATTACH '{item}' as dba")

        con3.execute("BEGIN")
        for row in con3.execute("SELECT * FROM dba.sqlite_master WHERE type='table'"):
            combine = "INSERT INTO " + row[1] + " SELECT * FROM dba." + row[1]
            print(combine)
            con3.execute(combine)
        con3.commit()
        con3.execute("detach database dba")
    a = 1




def clean_duplicate():
    source_dir = pathlib.Path(__file__).parents[4] / 's3_sync' / 't1'
    file_name = source_dir / "25_Jan_2017_TO_23_May_2021_BTC_1h_1d.db"
    con3 = sqlite3.connect(file_name)
    cur = con3.cursor()
    cur.execute("SELECT * FROM COIN_HISTORY_open_BTC_1h")
    a = 1


if __name__ == "__main__":
    clean_duplicate()
