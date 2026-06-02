### calculateDailyConcentrations.py
### Summary: calculate daily pollen and mold concentrations from hourly metrics

import argparse
import os

from SQLAPI import SQLAPI


def load_env_file(path):
    env = {}
    with open(path, "r") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key] = value
    return env


def main():
    parser = argparse.ArgumentParser(
        description="Calculate daily concentrations at each pollen monitor."
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--env-file", default=".env")
    args = parser.parse_args()

    env = os.environ.copy()
    env.update(load_env_file(args.env_file))

    sql = SQLAPI(
        env.get("DB_NAME"),
        env.get("DB_USER"),
        env.get("DB_PW"),
        env.get("DB_HOST"),
        env.get("DB_PORT")
    )

    n_rows = sql.upsertDailyConcentrations(
        start_date=args.start_date,
        end_date=args.end_date,
        day_timezone=args.timezone
    )
    print("upserted %i daily concentration rows" % (n_rows))


if __name__ == "__main__":
    main()
