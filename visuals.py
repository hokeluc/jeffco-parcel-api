
import os
from urllib import parse

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

from query import (
    most_valuable_street_types,
    occupancy_counts_city,
    turnover_neighborhood,
)

load_dotenv()


def get_engine():
    login = parse.quote(str(os.getenv("DB_USERNAME")))
    secret = parse.quote(str(os.getenv("DB_PASSWORD")))
    url = f"postgresql+psycopg2://{login}:{secret}@ada.mines.edu:5432/csci403"
    engine = create_engine(url)
    return engine



# Average property value by street type
def plot_avg_value_by_street_type(
    engine,
    top_n: int = 10,
    save_path: str | None = None,
):

    df = most_valuable_street_types(engine).copy()

    # Drop rows where street_type or num_val is NULL
    df = df[df["street_type"].notna() & df["num_val"].notna()]

    # Make sure street_type is string for plotting labels
    df["street_type"] = df["street_type"].astype(str)

    # Sort by numeric value and take the top N street types
    df_sorted = df.sort_values("num_val", ascending=False).head(top_n)

    plt.figure(figsize=(10, 6))
    plt.bar(df_sorted["street_type"], df_sorted["num_val"])
    plt.xlabel("Street Type")
    plt.ylabel("Average Assessed Value per Parcel ($)")
    plt.title(f"Top {top_n} Street Types by Average Assessed Value")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300)
        print(f"Saved: {save_path}")
    else:
        plt.show()

    plt.close()



# Occupancy mix by city – PERCENTAGES
def plot_occupancy_mix_by_city_pct(
    engine,
    cities: list[str],
    save_path: str | None = None,
):

    records = []

    for city in cities:
        city_data = occupancy_counts_city(engine, city)

        for entry in city_data.get("occupancy_counts", []):
            occ_type = entry.get("occupancy_type")
            count = entry.get("count", 0)

            if occ_type is None:
                continue  

            records.append(
                {
                    "city": city_data["city"],
                    "occupancy_type": occ_type,
                    "count": count,
                }
            )

    if not records:
        print("No occupancy data returned for the requested cities.")
        return

    df = pd.DataFrame(records)

    # Pivot to get columns: commercial, owner_occupied, rental, etc.
    pivot = df.pivot(
        index="city",
        columns="occupancy_type",
        values="count",
    ).fillna(0)

    # Ensure consistent column order (add zero columns if missing)
    for col in ["owner_occupied", "rental", "commercial"]:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot = pivot[["owner_occupied", "rental", "commercial"]]

    totals = pivot.sum(axis=1)

    pct = pivot.div(totals.replace(0, np.nan), axis=0) * 100
    pct = pct.fillna(0)

    plt.figure(figsize=(10, 6))
    cities_index = pct.index

    owner_pct = pct["owner_occupied"]
    rental_pct = pct["rental"]
    commercial_pct = pct["commercial"]

    plt.bar(cities_index, owner_pct, label="Owner-Occupied")
    plt.bar(cities_index, rental_pct, bottom=owner_pct, label="Rental")
    plt.bar(
        cities_index,
        commercial_pct,
        bottom=owner_pct + rental_pct,
        label="Commercial",
    )

    plt.xlabel("City")
    plt.ylabel("Share of Parcels (%)")
    plt.title("Occupancy Mix by City (Percentage of Parcels)")
    plt.xticks(rotation=45, ha="right")
    plt.ylim(0, 100)
    plt.legend()
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300)
        print(f"Saved: {save_path}")
    else:
        plt.show()

    plt.close()

if __name__ == "__main__":
    engine = get_engine()

    # 1) Top street types by average value (y-axis = average TOTACTVAL per parcel)
    plot_avg_value_by_street_type(
        engine,
        top_n=10,
        save_path="fig_avg_value_by_street_type.png",
    )

    # 2) Occupancy mix by city (y-axis = percentage of parcels)
    cities_to_plot = ["GOLDEN", "LAKEWOOD", "ARVADA", "LITTLETON"]
    plot_occupancy_mix_by_city_pct(
        engine,
        cities=cities_to_plot,
        save_path="fig_occupancy_mix_by_city_pct.png",
    )


    print("All visualization figures saved.")