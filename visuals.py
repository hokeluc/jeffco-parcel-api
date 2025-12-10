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



def canonical_city_name(s: str) -> str:
    
    if s is None:
        return ""
    return s.strip().upper()


def pretty_city_label(canonical: str) -> str:
    
    return canonical.title()


def get_all_cities(engine):
    
    query = f"""
        SELECT DISTINCT UPPER(TRIM(prpctynam)) AS city
        FROM {SCHEMA}.{TABLE}
        WHERE prpctynam IS NOT NULL AND TRIM(prpctynam) <> ''
        ORDER BY UPPER(TRIM(prpctynam));
    """
    df = pd.read_sql(query, engine)
    return df["city"].tolist()


def build_occupancy_df(
    engine,
    cities: list[str] | None = None,
    use_all_cities: bool = False,
) -> pd.DataFrame:

    if use_all_cities:
        
        cities = get_all_cities(engine)
    else:
        
        if not cities:
            return pd.DataFrame(columns=["city", "occupancy_type", "count"])
        cities = [canonical_city_name(c) for c in cities]

    records = []

    for city_canonical in cities:
        if not city_canonical:
            continue

        city_data = occupancy_counts_city(engine, city_canonical)

        for entry in city_data.get("occupancy_counts", []):
            occ_type = entry.get("occupancy_type")
            count = entry.get("count", 0)
            if occ_type is None:
                continue
            records.append(
                {
                    "city": city_canonical,
                    "occupancy_type": occ_type,
                    "count": int(count),
                }
            )

    if not records:
        return pd.DataFrame(columns=["city", "occupancy_type", "count"])

    return pd.DataFrame(records)


def plot_avg_value_by_street_type(
    engine,
    top_n=10,
    save_path="fig_avg_value_by_street_type.png",
):
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
    plt.ticklabel_format(style='plain', axis='y')
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

    # For plotting labels, keep a pretty version of the city name
    df["city_label"] = df["city"].apply(pretty_city_label)

    pivot = df.pivot(
        index="city_label",
        columns="occupancy_type",
        values="count",
    ).fillna(0)

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
    cities=None,
    save_png="fig_occupancy_sunburst.png",
    #save_html="fig_occupancy_sunburst.html",
):
    
    if use_all_cities:
        df = build_occupancy_df(engine, use_all_cities=True)
    else:
        if not cities:
            raise ValueError("cities must be provided if use_all_cities=False")
        df = build_occupancy_df(engine, cities=cities, use_all_cities=False)

    if df.empty:
        print("No occupancy data available for sunburst.")
        return

    df["root"] = "Jeffco Parcels"
    
    df["city_label"] = df["city"].apply(pretty_city_label)

    fig = px.sunburst(
        df,
        path=["root", "city_label", "occupancy_type"],
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
    #fig.write_html(save_html)

    print(f"Saved PNG: {save_png}")
    #print(f"Saved HTML: {save_html}")

def plot_occupancy_sankey(
    engine,
    use_all_cities=True,
    cities=None,
    save_png="fig_occupancy_sankey.png",
    #save_html="fig_occupancy_sankey.html",
):
    
    if use_all_cities:
        df = build_occupancy_df(engine, use_all_cities=True)
    else:
        if not cities:
            raise ValueError("cities must be provided if use_all_cities=False")
        df = build_occupancy_df(engine, cities=cities, use_all_cities=False)

    if df.empty:
        print("No occupancy data available for sankey.")
        return

    
    cities_unique = sorted(df["city"].unique().tolist())
    occ_unique = ["owner_occupied", "rental", "commercial"]

   
    city_labels = [pretty_city_label(c) for c in cities_unique]
    occ_labels = ["Owner-Occupied", "Rental", "Commercial"]
    node_labels = city_labels + occ_labels

    
    city_index_map = {c: i for i, c in enumerate(cities_unique)}
    occ_index_map = {o: len(cities_unique) + i for i, o in enumerate(occ_unique)}

    sources, targets, values = [], [], []

    for city_canonical in cities_unique:
        subset = df[df["city"] == city_canonical]
        for occ in occ_unique:
            count = int(subset.loc[subset["occupancy_type"] == occ, "count"].sum())
            if count <= 0:
                continue
            sources.append(city_index_map[city_canonical])
            targets.append(occ_index_map[occ])
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

    fig.update_layout(
        title_text="Sankey Diagram: City → Occupancy Type",
        font=dict(size=10),
    )

    fig.write_image(save_png)
    #fig.write_html(save_html)

    print(f"Saved PNG: {save_png}")
    #print(f"Saved HTML: {save_html}")

if __name__ == "__main__":
    engine = get_engine()

    # 1) Street type value bar chart
    plot_avg_value_by_street_type(engine)

    # 2) Percentage stacked bar for selected cities (input can be any case)
    cities_to_plot = ["Golden", "LAKEWOOD", "arvada", "LITTLETON"]
    plot_occupancy_mix_by_city_pct(engine, cities_to_plot)

    # 3) Sunburst for ALL cities (no duplicates, labels in Title Case)
    plot_occupancy_sunburst(engine, use_all_cities=True)

    # 4) Sankey for ALL cities
    plot_occupancy_sankey(engine, use_all_cities=True)

    print("All visualizations saved.")