#!/usr/bin/env python3
"""
Create Superset datasets, charts, and dashboard for the NYC Taxi Lakehouse.

Usage:
    docker exec -i lakehouse-superset /app/.venv/bin/python - < scripts/setup_superset_dashboard.py
"""

import json
import uuid

import requests

BASE = "http://localhost:8088"
DB_NAME = "Trino Iceberg"

# ── Helpers ──────────────────────────────────────────────────────────────────


def api_session():
    """Authenticate and return a requests.Session with tokens."""
    s = requests.Session()
    r = s.post(
        f"{BASE}/api/v1/security/login",
        json={
            "username": "admin",
            "password": "admin",
            "provider": "db",
            "refresh": True,
        },
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    )
    csrf = s.get(f"{BASE}/api/v1/security/csrf_token/")
    csrf.raise_for_status()
    s.headers.update(
        {
            "X-CSRFToken": csrf.json()["result"],
            "Referer": BASE,
        }
    )
    return s


def get_database_id(s):
    """Find the Trino Iceberg database id."""
    r = s.get(f"{BASE}/api/v1/database/")
    r.raise_for_status()
    for db in r.json()["result"]:
        if db["database_name"] == DB_NAME:
            return db["id"]
    raise RuntimeError(f"Database '{DB_NAME}' not found in Superset")


def create_dataset(s, db_id, schema, table):
    """Create (or return existing) dataset and return its id."""
    # Check if it already exists
    q = f"?q=(filters:!((col:table_name,opr:eq,value:'{table}'),(col:schema,opr:eq,value:'{schema}')))"
    r = s.get(f"{BASE}/api/v1/dataset/{q}")
    if r.ok and r.json().get("count", 0) > 0:
        ds_id = r.json()["result"][0]["id"]
        print(f"  Dataset exists: {schema}.{table} (id={ds_id})")
        return ds_id

    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table,
    }
    r = s.post(f"{BASE}/api/v1/dataset/", json=payload)
    if r.status_code == 422 and "already exists" in r.text:
        # Fetch it
        r2 = s.get(f"{BASE}/api/v1/dataset/{q}")
        ds_id = r2.json()["result"][0]["id"]
        print(f"  Dataset already existed: {schema}.{table} (id={ds_id})")
        return ds_id
    r.raise_for_status()
    ds_id = r.json()["id"]
    print(f"  Created dataset: {schema}.{table} (id={ds_id})")
    return ds_id


def create_chart(s, name, ds_id, viz_type, params, dashboard_ids=None):
    """Create (or return existing) chart and return its id."""
    # Check if it already exists
    q = f"?q=(filters:!((col:slice_name,opr:eq,value:'{name}')))"
    r = s.get(f"{BASE}/api/v1/chart/{q}")
    if r.ok and r.json().get("count", 0) > 0:
        ch_id = r.json()["result"][0]["id"]
        print(f"  Chart exists: {name} (id={ch_id})")
        # Still update dashboard association if needed
        if dashboard_ids:
            s.put(f"{BASE}/api/v1/chart/{ch_id}", json={"dashboards": dashboard_ids})
        return ch_id

    payload = {
        "slice_name": name,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    if dashboard_ids:
        payload["dashboards"] = dashboard_ids
    r = s.post(f"{BASE}/api/v1/chart/", json=payload)
    if r.status_code == 422:
        print(f"  Chart creation warning for '{name}': {r.text}")
        r2 = s.get(f"{BASE}/api/v1/chart/{q}")
        if r2.ok and r2.json().get("count", 0) > 0:
            return r2.json()["result"][0]["id"]
    r.raise_for_status()
    ch_id = r.json()["id"]
    print(f"  Created chart: {name} (id={ch_id})")
    return ch_id


def create_dashboard_empty(s, title):
    """Create an empty dashboard and return its id."""
    q = f"?q=(filters:!((col:dashboard_title,opr:eq,value:'{title}')))"
    r = s.get(f"{BASE}/api/v1/dashboard/{q}")
    if r.ok and r.json().get("count", 0) > 0:
        dash_id = r.json()["result"][0]["id"]
        print(f"  Dashboard exists: {title} (id={dash_id})")
        return dash_id

    r = s.post(
        f"{BASE}/api/v1/dashboard/",
        json={
            "dashboard_title": title,
            "published": True,
        },
    )
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"  Created dashboard: {title} (id={dash_id})")
    return dash_id


def _uid():
    return str(uuid.uuid4())[:8]


def update_dashboard_layout(s, dash_id, title, chart_ids, chart_names):
    """Set the dashboard layout with all charts properly arranged."""
    # Build proper Superset v2 position layout
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": title},
        },
    }

    cols_per_row = 2
    col_span = 6  # out of 12
    row_height = 50  # pixels-ish in grid units
    grid_children = []

    for idx, (cid, cname) in enumerate(zip(chart_ids, chart_names)):
        row_idx = idx // cols_per_row
        col_idx = idx % cols_per_row
        row_id = f"ROW-N-{_uid()}"

        # Each chart gets its own row for simplicity, 2 per row
        if col_idx == 0:
            current_row_id = row_id
            position[current_row_id] = {
                "type": "ROW",
                "id": current_row_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            grid_children.append(current_row_id)

        chart_key = f"CHART-explore-{cid}-{_uid()}"
        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", current_row_id],
            "meta": {
                "width": col_span,
                "height": row_height,
                "chartId": cid,
                "sliceName": cname,
            },
        }
        position[current_row_id]["children"].append(chart_key)

    position["GRID_ID"]["children"] = grid_children

    r = s.put(
        f"{BASE}/api/v1/dashboard/{dash_id}",
        json={
            "position_json": json.dumps(position),
            "json_metadata": json.dumps(
                {
                    "timed_refresh_immune_slices": [],
                    "expanded_slices": {},
                    "refresh_frequency": 0,
                    "default_filters": "{}",
                    "color_scheme": "supersetColors",
                }
            ),
        },
    )
    if not r.ok:
        print(f"  Warning: dashboard layout update returned {r.status_code}: {r.text[:300]}")
    else:
        print(f"  Dashboard layout updated with {len(chart_ids)} charts")


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("NYC Taxi Lakehouse - Superset Dashboard Setup")
    print("=" * 60)

    s = api_session()
    print("[OK] Authenticated with Superset API\n")

    db_id = get_database_id(s)
    print(f"[OK] Database id: {db_id}\n")

    # ── 1. Create Datasets ───────────────────────────────────────────────
    print("--- Creating Datasets ---")
    ds_daily = create_dataset(s, db_id, "gold", "daily_trip_stats")
    ds_hourly = create_dataset(s, db_id, "gold", "hourly_location_analysis")
    ds_revenue = create_dataset(s, db_id, "gold", "revenue_by_payment_type")
    ds_clean = create_dataset(s, db_id, "silver", "nyc_taxi_clean")
    print()

    # ── 2. Create Dashboard (empty first, so we have its ID) ─────────
    print("--- Creating Dashboard ---")
    dash_id = create_dashboard_empty(s, "NYC Taxi Lakehouse Analytics")
    print()

    # ── 3. Create Charts (linked to the dashboard) ───────────────────
    print("--- Creating Charts ---")
    chart_ids = []
    chart_names = []

    # Chart 1: Total Trips by Day of Week (bar chart)
    name = "Total Trips by Day of Week"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "echarts_timeseries_bar",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "day_of_week",
            "metrics": [{"label": "Total Trips", "expressionType": "SQL", "sqlExpression": "SUM(total_trips)"}],
            "groupby": [],
            "order_desc": True,
            "row_limit": 10,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Day of Week (1=Mon, 7=Sun)",
            "y_axis_title": "Total Trips",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 2: Average Fare by Day of Week (line chart)
    name = "Average Fare by Day of Week"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "echarts_timeseries_line",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "echarts_timeseries_line",
            "x_axis": "day_of_week",
            "metrics": [{"label": "Avg Fare", "expressionType": "SQL", "sqlExpression": "AVG(avg_fare)"}],
            "groupby": [],
            "row_limit": 10,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Day of Week",
            "y_axis_title": "Average Fare ($)",
            "y_axis_format": "$,.2f",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 3: Revenue by Payment Type (pie chart)
    name = "Revenue by Payment Type"
    cid = create_chart(
        s,
        name,
        ds_revenue,
        "pie",
        {
            "datasource": f"{ds_revenue}__table",
            "viz_type": "pie",
            "metric": {"label": "Total Revenue", "expressionType": "SQL", "sqlExpression": "SUM(total_revenue)"},
            "groupby": ["payment_type"],
            "color_scheme": "supersetColors",
            "show_legend": True,
            "show_labels": True,
            "label_type": "key_percent",
            "number_format": "$,.2f",
            "row_limit": 10,
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 4: Trips by Payment Type (bar chart)
    name = "Trip Count by Payment Type"
    cid = create_chart(
        s,
        name,
        ds_revenue,
        "echarts_timeseries_bar",
        {
            "datasource": f"{ds_revenue}__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "payment_type",
            "metrics": [{"label": "Trip Count", "expressionType": "SQL", "sqlExpression": "SUM(trip_count)"}],
            "groupby": [],
            "row_limit": 10,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Payment Type",
            "y_axis_title": "Number of Trips",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 5: Hourly Trip Volume (bar chart)
    name = "Trip Volume by Hour of Day"
    cid = create_chart(
        s,
        name,
        ds_hourly,
        "echarts_timeseries_bar",
        {
            "datasource": f"{ds_hourly}__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "hour_of_day",
            "metrics": [{"label": "Trip Count", "expressionType": "SQL", "sqlExpression": "SUM(trip_count)"}],
            "groupby": [],
            "order_desc": False,
            "row_limit": 24,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Hour of Day (0-23)",
            "y_axis_title": "Number of Trips",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 6: Hourly Revenue (line chart)
    name = "Revenue by Hour of Day"
    cid = create_chart(
        s,
        name,
        ds_hourly,
        "echarts_timeseries_line",
        {
            "datasource": f"{ds_hourly}__table",
            "viz_type": "echarts_timeseries_line",
            "x_axis": "hour_of_day",
            "metrics": [{"label": "Hourly Revenue", "expressionType": "SQL", "sqlExpression": "SUM(hourly_revenue)"}],
            "groupby": [],
            "row_limit": 24,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Hour of Day",
            "y_axis_title": "Revenue ($)",
            "y_axis_format": "$,.2f",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 7: Total Revenue KPI (big number)
    name = "Total Revenue (All Trips)"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "big_number_total",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "big_number_total",
            "metric": {"label": "Total Revenue", "expressionType": "SQL", "sqlExpression": "SUM(total_revenue)"},
            "y_axis_format": "$,.2f",
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 8: Total Trips KPI (big number)
    name = "Total Trips (All Data)"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "big_number_total",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "big_number_total",
            "metric": {"label": "Total Trips", "expressionType": "SQL", "sqlExpression": "SUM(total_trips)"},
            "y_axis_format": ",d",
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 9: Avg Trip Distance (big number)
    name = "Average Trip Distance"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "big_number_total",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "big_number_total",
            "metric": {"label": "Avg Distance", "expressionType": "SQL", "sqlExpression": "AVG(avg_trip_distance)"},
            "y_axis_format": ",.2f",
            "subheader": "miles",
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 10: Avg Trip Duration (big number)
    name = "Average Trip Duration"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "big_number_total",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "big_number_total",
            "metric": {"label": "Avg Duration", "expressionType": "SQL", "sqlExpression": "AVG(avg_trip_duration)"},
            "y_axis_format": ",.1f",
            "subheader": "minutes",
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 11: Trip Distance Distribution (histogram via bar)
    name = "Avg Distance vs Avg Fare by Location"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "echarts_timeseries_bar",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "pickup_location_id",
            "metrics": [
                {"label": "Avg Distance", "expressionType": "SQL", "sqlExpression": "AVG(avg_trip_distance)"},
                {"label": "Avg Fare", "expressionType": "SQL", "sqlExpression": "AVG(avg_fare)"},
            ],
            "groupby": [],
            "order_desc": True,
            "row_limit": 20,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Pickup Location ID",
            "y_axis_title": "Avg Value",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    # Chart 12: Top pickup locations by revenue (table)
    name = "Top 15 Pickup Locations by Revenue"
    cid = create_chart(
        s,
        name,
        ds_daily,
        "table",
        {
            "datasource": f"{ds_daily}__table",
            "viz_type": "table",
            "metrics": [
                {"label": "Total Revenue", "expressionType": "SQL", "sqlExpression": "SUM(total_revenue)"},
                {"label": "Total Trips", "expressionType": "SQL", "sqlExpression": "SUM(total_trips)"},
                {"label": "Avg Fare", "expressionType": "SQL", "sqlExpression": "AVG(avg_fare)"},
                {"label": "Avg Distance", "expressionType": "SQL", "sqlExpression": "AVG(avg_trip_distance)"},
            ],
            "groupby": ["pickup_location_id"],
            "order_desc": True,
            "row_limit": 15,
            "color_pn": True,
            "include_search": True,
            "table_timestamp_format": "smart_date",
        },
        dashboard_ids=[dash_id],
    )
    chart_ids.append(cid)
    chart_names.append(name)

    print(f"\n[OK] Created {len(chart_ids)} charts\n")

    # ── 4. Update Dashboard Layout ───────────────────────────────────────
    print("--- Updating Dashboard Layout ---")
    update_dashboard_layout(s, dash_id, "NYC Taxi Lakehouse Analytics", chart_ids, chart_names)
    print(f"\n{'=' * 60}")
    print(f"Dashboard ready at: {BASE}/superset/dashboard/{dash_id}/")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
