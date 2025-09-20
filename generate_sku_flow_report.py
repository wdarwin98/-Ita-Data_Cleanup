#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate consolidated SKU flow report from four datasets.

Inputs (expected in the same folder as this script or adjust the paths):
- 2025-09-02T03_44_52_230_inboundOrderLine (3).xlsx  (sheet: inboundOrderLine)
- Production_Consolidated.csv
- (ITA) Inbound from Production.xlsx                  (sheets: Aug25 Prod Order-DK, Aug25 Prod Order - DE)
- Shipments_Consolidated.csv

Output:
- SKU_Flow_Report.csv

Notes:
- Columns are mapped exactly as provided in your instructions.
- "In Production", "Serial no.", and "Variance" are included with reasonable default formulas/placeholders.
  Adjust these formulas to fit your operational definition (see TODOs below).
"""

import pandas as pd
from pathlib import Path

# ---------- Configuration ----------
BASE_DIR = Path(__file__).parent.resolve()
INBOUND_ORDER_XLSX = "2025-09-02T03_44_52_230_inboundOrderLine (3).xlsx"
INBOUND_ORDER_SHEET = "inboundOrderLine"

PRODUCTION_CONSOLIDATED_CSV = "Production_Consolidated.csv"

INBOUND_FROM_PROD_XLSX = "(ITA) Inbound from Production.xlsx"
INBOUND_FROM_PROD_SHEETS = ["Aug25 Prod Order-DK", "Aug25 Prod Order - DE"]

SHIPMENTS_CONSOLIDATED_CSV = "Shipments_Consolidated.csv"

OUTPUT_CSV = "SKU_Flow_Report.csv"

# Column names per source (change here if your files change)
COL_SKU_INBOUND = "Varenummer"
COL_QTY_RECEIVED = "Mængde modtaget"

COL_SKU_PROD = "ItemNumber"
COL_QTY_PRODUCED = "QuantityProduced"

COL_QTY_PACKED_INBOUND = "QuantityPacked"  # from inbound-from-production (finished goods received to warehouse)

COL_QTY_PACKED_SHIP = "QuantityPacked"     # from outbound shipments

# ---------- Load Data ----------
def load_inbound_order(path_xlsx: str, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(str((BASE_DIR / path_xlsx).resolve()), sheet_name=sheet)
    # Keep only needed columns
    needed = [c for c in df.columns if c in {COL_SKU_INBOUND, COL_QTY_RECEIVED}]
    df = df[needed].copy()
    return df

def load_production_consolidated(path_csv: str) -> pd.DataFrame:
    df = pd.read_csv(str((BASE_DIR / path_csv).resolve()))
    needed = [c for c in df.columns if c in {COL_SKU_PROD, COL_QTY_PRODUCED}]
    df = df[needed].copy()
    return df

def load_inbound_from_production(path_xlsx: str, sheets: list[str]) -> pd.DataFrame:
    frames = []
    for s in sheets:
        df = pd.read_excel(str((BASE_DIR / path_xlsx).resolve()), sheet_name=s)
        # Require ItemNumber + QuantityPacked
        needed = [c for c in df.columns if c in {COL_SKU_PROD, COL_QTY_PACKED_INBOUND}]
        df = df[needed].copy()
        frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame(columns=[COL_SKU_PROD, COL_QTY_PACKED_INBOUND])

def load_shipments_consolidated(path_csv: str) -> pd.DataFrame:
    df = pd.read_csv(str((BASE_DIR / path_csv).resolve()))
    needed = [c for c in df.columns if c in {COL_SKU_PROD, COL_QTY_PACKED_SHIP}]
    df = df[needed].copy()
    return df

# ---------- Aggregations ----------
def agg_goods_in(df: pd.DataFrame) -> pd.DataFrame:
    # SUMIF by SKU from inbound order lines
    out = df.groupby(COL_SKU_INBOUND, dropna=False)[COL_QTY_RECEIVED].sum().reset_index()
    out = out.rename(columns={COL_SKU_INBOUND: "SKU", COL_QTY_RECEIVED: "Goods In"})
    return out

def agg_moved_to_production(df: pd.DataFrame) -> pd.DataFrame:
    # SUMIF by SKU for quantity produced (as per your instruction)
    out = df.groupby(COL_SKU_PROD, dropna=False)[COL_QTY_PRODUCED].sum().reset_index()
    out = out.rename(columns={COL_SKU_PROD: "SKU", COL_QTY_PRODUCED: "Moved to Production"})
    return out

def agg_inbound_from_production(df: pd.DataFrame) -> pd.DataFrame:
    out = df.groupby(COL_SKU_PROD, dropna=False)[COL_QTY_PACKED_INBOUND].sum().reset_index()
    out = out.rename(columns={COL_SKU_PROD: "SKU", COL_QTY_PACKED_INBOUND: "Inbound from Production"})
    return out

def agg_shipments_out(df: pd.DataFrame) -> pd.DataFrame:
    out = df.groupby(COL_SKU_PROD, dropna=False)[COL_QTY_PACKED_SHIP].sum().reset_index()
    out = out.rename(columns={COL_SKU_PROD: "SKU", COL_QTY_PACKED_SHIP: "Shipments Out"})
    return out

# ---------- Report builder ----------
def build_report() -> pd.DataFrame:
    inbound_order_df = load_inbound_order(INBOUND_ORDER_XLSX, INBOUND_ORDER_SHEET)
    production_df = load_production_consolidated(PRODUCTION_CONSOLIDATED_CSV)
    inbound_from_prod_df = load_inbound_from_production(INBOUND_FROM_PROD_XLSX, INBOUND_FROM_PROD_SHEETS)
    shipments_df = load_shipments_consolidated(SHIPMENTS_CONSOLIDATED_CSV)

    goods_in = agg_goods_in(inbound_order_df)
    moved_to_prod = agg_moved_to_production(production_df)
    inbound_from_prod = agg_inbound_from_production(inbound_from_prod_df)
    shipments_out = agg_shipments_out(shipments_df)

    report = goods_in.merge(moved_to_prod, on="SKU", how="outer") \
                     .merge(inbound_from_prod, on="SKU", how="outer") \
                     .merge(shipments_out, on="SKU", how="outer")

    # Fill missing numeric values
    for col in ["Goods In", "Moved to Production", "Inbound from Production", "Shipments Out"]:
        if col in report.columns:
            report[col] = report[col].fillna(0)

    # ---------- Extra columns (adjust as needed) ----------
    # TODO: Define In Production precisely per your process.
    # A common proxy is WIP = Moved to Production - Inbound from Production (items started vs. received back).
    report["In Production"] = report["Moved to Production"] - report["Inbound from Production"]

    # TODO: Serial no. - requires a serial number field in your sources. Left blank by default.
    report["Serial no."] = pd.NA

    # Inventory ULTIMO (ending inventory) - a simple balance for finished goods:
    # Goods In (purchased/received) + Inbound from Production - Shipments Out
    # Adjust if "Moved to Production" should impact finished vs. raw inventory differently in your setup.
    report["Inventory ULTIMO"] = report["Goods In"] + report["Inbound from Production"] - report["Shipments Out"]

    # TODO: Variance - placeholder. Define variance logic (e.g., cycle count variance, plan vs. actual, etc.).
    report["Variance"] = 0

    # Reorder columns
    desired_cols = [
        "SKU", "Goods In", "Moved to Production", "In Production",
        "Inbound from Production", "Shipments Out", "Serial no.",
        "Variance", "Inventory ULTIMO"
    ]
    existing_cols = [c for c in desired_cols if c in report.columns]
    report = report[existing_cols]

    return report

def main():
    report = build_report()
    report.sort_values(by=["SKU"], inplace=True, kind="stable")
    report.to_csv(str((BASE_DIR / OUTPUT_CSV).resolve()), index=False, encoding="utf-8-sig")
    print(f"Saved: {Path(OUTPUT_CSV).resolve()}")
    print("Rows:", len(report))

if __name__ == "__main__":
    main()
