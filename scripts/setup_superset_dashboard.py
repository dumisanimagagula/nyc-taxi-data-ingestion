#!/usr/bin/env python3
"""
Automated Superset provisioning for the NYC Taxi Lakehouse.

Reads dashboard JSON definitions from a directory and creates all datasources,
datasets, charts, and dashboards via the Superset REST API.  Designed to run
inside the Superset container on startup or ad-hoc.

Usage (inside container):
    python /app/superset_home/setup_superset_dashboard.py

Usage (from host):
    docker exec -i lakehouse-superset python /app/superset_home/setup_superset_dashboard.py

Environment variables:
    SUPERSET_URL               Base URL (default: http://localhost:8088)
    SUPERSET_ADMIN_USERNAME    Admin username (default: admin)
    SUPERSET_ADMIN_PASSWORD    Admin password (default: admin)
    DASHBOARD_DIR              Path to dashboard JSON files
                               (default: /app/superset_home/dashboards)
    TRINO_HOST                 Trino hostname (default: lakehouse-trino)
    TRINO_PORT                 Trino port (default: 8080)
"""

import glob
import json
import os
import sys
import time
import uuid

import requests

# ── Configuration ────────────────────────────────────────────────────────────

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
ADMIN_PASS = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
DASHBOARD_DIR = os.environ.get("DASHBOARD_DIR", "/app/superset_home/dashboards")
TRINO_HOST = os.environ.get("TRINO_HOST", "lakehouse-trino")
TRINO_PORT = os.environ.get("TRINO_PORT", "8080")
DB_NAME = "Trino Iceberg"

MAX_RETRIES = 30
RETRY_DELAY = 10  # seconds


# ── Helpers ──────────────────────────────────────────────────────────────────


def wait_for_superset():
    """Block until the Superset API is responsive."""
    print("Waiting for Superset API ...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(f"{BASE}/health", timeout=5)
            if r.ok:
                print(f"  Superset healthy (attempt {attempt})")
                return
        except requests.ConnectionError:
            pass
        print(f"  Attempt {attempt}/{MAX_RETRIES} - not ready, retrying in {RETRY_DELAY}s")
        time.sleep(RETRY_DELAY)
    print("ERROR: Superset did not become ready in time")
    sys.exit(1)


def api_session():
    """Authenticate and return a requests.Session with tokens."""
    s = requests.Session()
    r = s.post(
        f"{BASE}/api/v1/security/login",
        json={
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
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


def create_or_get_database(s):
    """Register the Trino database connection or return existing id."""
    r = s.get(f"{BASE}/api/v1/database/")
    r.raise_for_status()
    for db in r.json()["result"]:
        if db["database_name"] == DB_NAME:
            print(f"  Database exists: {DB_NAME} (id={db['id']})")
            return db["id"]

    sqlalchemy_uri = f"trino://trino@{TRINO_HOST}:{TRINO_PORT}/lakehouse"
    payload = {
        "database_name": DB_NAME,
        "engine": "trino",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_run_async": True,
    }
    r = s.post(f"{BASE}/api/v1/database/", json=payload)
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  Created database: {DB_NAME} (id={db_id})")
    return db_id


def create_dataset(s, db_id, schema, table):
    """Create (or return existing) dataset and return its id."""
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
    q = f"?q=(filters:!((col:slice_name,opr:eq,value:'{name}')))"
    r = s.get(f"{BASE}/api/v1/chart/{q}")
    if r.ok and r.json().get("count", 0) > 0:
        ch_id = r.json()["result"][0]["id"]
        print(f"  Chart exists: {name} (id={ch_id})")
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


def create_dashboard_empty(s, title, published=True):
    """Create an empty dashboard and return its id."""
    q = f"?q=(filters:!((col:dashboard_title,opr:eq,value:'{title}')))"
    r = s.get(f"{BASE}/api/v1/dashboard/{q}")
    if r.ok and r.json().get("count", 0) > 0:
        dash_id = r.json()["result"][0]["id"]
        print(f"  Dashboard exists: {title} (id={dash_id})")
        return dash_id

    r = s.post(
        f"{BASE}/api/v1/dashboard/",
        json={"dashboard_title": title, "published": published},
    )
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"  Created dashboard: {title} (id={dash_id})")
    return dash_id


def _uid():
    return str(uuid.uuid4())[:8]


def build_dashboard_layout(s, dash_id, title, chart_ids, chart_names, layout_spec, refresh_frequency=0, color_scheme="supersetColors"):
    """Build and apply a Superset v2 position layout from a layout spec.

    layout_spec should contain:
        cols_per_row  - number of KPI cards per row (e.g. 4)
        kpi_row       - list of chart indices for the KPI row
        chart_rows    - list of [idx_a, idx_b] pairs for full-width chart rows
    """
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

    grid_children = []

    def add_row(indices, col_span, row_height):
        row_id = f"ROW-N-{_uid()}"
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for idx in indices:
            cid = chart_ids[idx]
            cname = chart_names[idx]
            chart_key = f"CHART-explore-{cid}-{_uid()}"
            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "width": col_span,
                    "height": row_height,
                    "chartId": cid,
                    "sliceName": cname,
                },
            }
            position[row_id]["children"].append(chart_key)
        grid_children.append(row_id)

    # KPI row: small cards, typically 4 across (col_span=3 each out of 12)
    kpi_indices = layout_spec.get("kpi_row", [])
    cols_per_row = layout_spec.get("cols_per_row", 4)
    kpi_col_span = 12 // max(cols_per_row, 1)
    if kpi_indices:
        add_row(kpi_indices, kpi_col_span, 13)

    # Chart rows: pairs of 2 charts (col_span=6 each)
    for row_indices in layout_spec.get("chart_rows", []):
        chart_col_span = 12 // max(len(row_indices), 1)
        add_row(row_indices, chart_col_span, 50)

    position["GRID_ID"]["children"] = grid_children

    r = s.put(
        f"{BASE}/api/v1/dashboard/{dash_id}",
        json={
            "position_json": json.dumps(position),
            "json_metadata": json.dumps(
                {
                    "timed_refresh_immune_slices": [],
                    "expanded_slices": {},
                    "refresh_frequency": refresh_frequency,
                    "default_filters": "{}",
                    "color_scheme": color_scheme,
                }
            ),
        },
    )
    if not r.ok:
        print(f"  Warning: layout update returned {r.status_code}: {r.text[:300]}")
    else:
        print(f"  Dashboard layout updated with {len(chart_ids)} charts")


# ── JSON-driven provisioning ─────────────────────────────────────────────────


def load_dashboard_definitions(directory):
    """Load all dashboard JSON files from a directory."""
    pattern = os.path.join(directory, "*.json")
    files = sorted(glob.glob(pattern))
    definitions = []
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "dashboard_title" not in data:
            print(f"  Skipping {os.path.basename(filepath)} (no dashboard_title)")
            continue
        definitions.append((filepath, data))
    return definitions


def provision_dashboard(s, db_id, definition):
    """Provision a single dashboard from its JSON definition."""
    title = definition["dashboard_title"]
    published = definition.get("published", True)
    refresh = definition.get("refresh_frequency", 0)
    color_scheme = definition.get("color_scheme", "supersetColors")

    # 1. Create datasets and build lookup: (schema, table) -> dataset_id
    dataset_lookup = {}
    for ds_spec in definition.get("datasets", []):
        schema = ds_spec["schema"]
        table = ds_spec["table"]
        ds_id = create_dataset(s, db_id, schema, table)
        dataset_lookup[(schema, table)] = ds_id

    # 2. Create dashboard
    dash_id = create_dashboard_empty(s, title, published=published)

    # 3. Create charts
    chart_ids = []
    chart_names = []
    for chart_spec in definition.get("charts", []):
        slice_name = chart_spec["slice_name"]
        viz_type = chart_spec["viz_type"]
        ds_ref = chart_spec["dataset"]
        ds_key = (ds_ref["schema"], ds_ref["table"])
        ds_id = dataset_lookup.get(ds_key)
        if ds_id is None:
            # Dataset not in definition's list; create on-the-fly
            ds_id = create_dataset(s, db_id, ds_ref["schema"], ds_ref["table"])
            dataset_lookup[ds_key] = ds_id

        params = dict(chart_spec.get("params", {}))
        params["datasource"] = f"{ds_id}__table"
        params["viz_type"] = viz_type

        cid = create_chart(s, slice_name, ds_id, viz_type, params, dashboard_ids=[dash_id])
        chart_ids.append(cid)
        chart_names.append(slice_name)

    # 4. Apply layout
    layout_spec = definition.get("layout", {})
    if layout_spec:
        build_dashboard_layout(
            s, dash_id, title, chart_ids, chart_names,
            layout_spec, refresh_frequency=refresh, color_scheme=color_scheme,
        )
    else:
        # Fallback: 2 charts per row, uniform sizing
        fallback_layout = {
            "cols_per_row": 2,
            "kpi_row": [],
            "chart_rows": [
                list(range(i, min(i + 2, len(chart_ids))))
                for i in range(0, len(chart_ids), 2)
            ],
        }
        build_dashboard_layout(
            s, dash_id, title, chart_ids, chart_names,
            fallback_layout, refresh_frequency=refresh, color_scheme=color_scheme,
        )

    return dash_id, len(chart_ids)


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("NYC Taxi Lakehouse - Automated Superset Provisioning")
    print("=" * 60)
    print(f"  Superset URL:    {BASE}")
    print(f"  Dashboard dir:   {DASHBOARD_DIR}")
    print(f"  Trino endpoint:  {TRINO_HOST}:{TRINO_PORT}")
    print()

    # Wait for Superset to be ready
    wait_for_superset()

    # Authenticate
    s = api_session()
    print("[OK] Authenticated with Superset API\n")

    # Register or find the Trino database connection
    print("--- Registering Database Connection ---")
    db_id = create_or_get_database(s)
    print(f"[OK] Database id: {db_id}\n")

    # Load dashboard definitions
    print("--- Loading Dashboard Definitions ---")
    definitions = load_dashboard_definitions(DASHBOARD_DIR)
    if not definitions:
        print("  No dashboard JSON files found; nothing to provision.")
        return
    print(f"  Found {len(definitions)} dashboard definition(s)\n")

    # Provision each dashboard
    total_charts = 0
    total_dashboards = 0
    for filepath, definition in definitions:
        filename = os.path.basename(filepath)
        title = definition["dashboard_title"]
        print(f"--- Provisioning: {title} ({filename}) ---")
        dash_id, n_charts = provision_dashboard(s, db_id, definition)
        total_dashboards += 1
        total_charts += n_charts
        print(f"  Dashboard ready at: {BASE}/superset/dashboard/{dash_id}/\n")

    # Summary
    print("=" * 60)
    print(f"Provisioning complete: {total_dashboards} dashboard(s), {total_charts} chart(s)")
    print(f"Superset UI: {BASE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
