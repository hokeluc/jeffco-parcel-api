import os
from urllib import parse

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

import plotly.express as px
import plotly.graph_objects as go

from query import (
    most_valuable_street_types,
    occupancy_counts_city,
)

load_dotenv()

SCHEMA = "kkubaska"
TABLE = "jeffco_staging"


def get_engine():
    login = parse.quote(str(os.getenv("DB_USERNAME")))
    secret = parse.quote(str(os.getenv("DB_PASSWORD")))
    url = f"postgresql+psycopg2://{login}:{secret}@ada.mines.edu:5432/csci403"
    engine = create_engine(url)
    return engine


def get_all_cities(engine):
    query = f"""
        SELECT DISTINCT prpctynam AS city
        FROM {SCHEMA}.{TABLE}
        WHERE prpctynam IS NOT NULL AND TRIM(prpctynam) <> ''
        ORDER BY prpctynam;
    """
    df = pd.read_sql(query, engine)
    return df["city"].tolist()


def build_occupancy_df(engine, cities: list[str] | None = None, use_all_cities: bool = False) -> pd.DataFrame:
    if use_all_cities:
        cities = get_all_cities(engine)

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
                    "city": city,
                    "occupancy_type": occ_type,
                    "count": int(count),
                }
            )

    if not records:
        return pd.DataFrame(columns=["city", "occupancy_type", "count"])

    return pd.DataFrame(records)

def plot_avg_value_by_street_type(engine, top_n=10, save_path="fig_avg_value_by_street_type.png"):
    df = most_valuable_street_types(engine).copy()
    df = df[df["street_type"].notna() & df["num_val"].notna()]
    df["street_type"] = df["street_type"].astype(str)

    df_sorted = df.sort_values("num_val", ascending=False).head(top_n)

    plt.figure(figsize=(10, 6))
    plt.bar(df_sorted["street_type"], df_sorted["num_val"])
    plt.xlabel("Street Type")
    plt.ylabel("Average Assessed Value per Parcel ($)")
    plt.title(f"Top {top_n} Street Types by Average Assessed Value")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    plt.savefig(save_path, dpi=300)
    print(f"Saved PNG: {save_path}")
    plt.close()

def plot_occupancy_mix_by_city_pct(
    engine,
    cities,
    save_path="fig_occupancy_mix_by_city_pct.png",
):
    df = build_occupancy_df(engine, cities=cities, use_all_cities=False)
    if df.empty:
        print("No occupancy data returned.")
        return

    pivot = df.pivot(index="city", columns="occupancy_type", values="count").fillna(0)

    for col in ["owner_occupied", "rental", "commercial"]:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot = pivot[["owner_occupied", "rental", "commercial"]]
    totals = pivot.sum(axis=1)
    pct = pivot.div(totals.replace(0, np.nan), axis=0) * 100
    pct = pct.fillna(0)

    plt.figure(figsize=(10, 6))
    cities_index = pct.index

    plt.bar(cities_index, pct["owner_occupied"], label="Owner-Occupied")
    plt.bar(cities_index, pct["rental"], bottom=pct["owner_occupied"], label="Rental")
    plt.bar(
        cities_index,
        pct["commercial"],
        bottom=pct["owner_occupied"] + pct["rental"],
        label="Commercial",
    )

    plt.xlabel("City")
    plt.ylabel("Share of Parcels (%)")
    plt.title("Occupancy Mix by City (Percentage)")
    plt.xticks(rotation=45, ha="right")
    plt.ylim(0, 100)
    plt.legend()
    plt.tight_layout()

    plt.savefig(save_path, dpi=300)
    print(f"Saved PNG: {save_path}")
    plt.close()

def plot_occupancy_sunburst(
    engine,
    use_all_cities=True,
    save_png="fig_occupancy_sunburst.png",
    save_html="fig_occupancy_sunburst.html",
):
    df = build_occupancy_df(engine, use_all_cities=use_all_cities)
    if df.empty:
        print("No occupancy data available for sunburst.")
        return

    df["root"] = "Jeffco Parcels"

    fig = px.sunburst(
        df,
        path=["root", "city", "occupancy_type"],
        values="count",
        title="Sunburst of Parcel Occupancy by City",
        color="occupancy_type",
        color_discrete_map={
            "owner_occupied": "#4daf4a",
            "rental": "#377eb8",
            "commercial": "#e41a1c",
        },
    )

    fig.write_image(save_png)
    fig.write_html(save_html)

    print(f"Saved PNG: {save_png}")
    print(f"Saved HTML: {save_html}")

def plot_occupancy_sankey(
    engine,
    use_all_cities=True,
    save_png="fig_occupancy_sankey.png",
    save_html="fig_occupancy_sankey.html",
):
    df = build_occupancy_df(engine, use_all_cities=use_all_cities)

    if df.empty:
        print("No occupancy data available for sankey.")
        return

    cities_unique = sorted(df["city"].unique().tolist())
    occ_unique = ["owner_occupied", "rental", "commercial"]
    node_labels = cities_unique + occ_unique
    idx = {label: i for i, label in enumerate(node_labels)}

    sources, targets, values = [], [], []

    for city in cities_unique:
        subset = df[df["city"] == city]
        for occ in occ_unique:
            count = int(subset.loc[subset["occupancy_type"] == occ, "count"].sum())
            if count > 0:
                sources.append(idx[city])
                targets.append(idx[occ])
                values.append(count)

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    label=node_labels,
                    pad=15,
                    thickness=15,
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                ),
            )
        ]
    )

    fig.update_layout(title_text="Sankey Diagram: City → Occupancy Type", font=dict(size=10))

    fig.write_image(save_png)
    fig.write_html(save_html)

    print(f"Saved PNG: {save_png}")
    print(f"Saved HTML: {save_html}")

if __name__ == "__main__":
    engine = get_engine()

    plot_avg_value_by_street_type(engine)
    plot_occupancy_mix_by_city_pct(engine, ["GOLDEN", "LAKEWOOD", "ARVADA", "LITTLETON"])

    plot_occupancy_sunburst(engine)
    plot_occupancy_sankey(engine)

    print("All visualizations saved as PNGs.")