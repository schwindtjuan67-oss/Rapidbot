import os
import json
import math
import argparse
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors


def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        v = float(x)
        if not np.isfinite(v):
            return default
        return v
    except Exception:
        return default


def load_artifacts(outdir: str):
    trades_path = os.path.join(outdir, "trades.csv")
    eq_path = os.path.join(outdir, "equity_curve.csv")
    metrics_path = os.path.join(outdir, "metrics.json")

    if not os.path.exists(metrics_path):
        raise FileNotFoundError(f"Missing {metrics_path}")
    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    trades_df = pd.DataFrame()
    if os.path.exists(trades_path):
        trades_df = pd.read_csv(trades_path)
        # parse datetimes if present
        for c in ["signal_ts", "entry_ts", "exit_ts"]:
            if c in trades_df.columns:
                trades_df[c] = pd.to_datetime(trades_df[c], utc=True, errors="coerce")

    eq_df = pd.DataFrame()
    if os.path.exists(eq_path):
        eq_df = pd.read_csv(eq_path)
        if "ts" in eq_df.columns:
            eq_df["ts"] = pd.to_datetime(eq_df["ts"], utc=True, errors="coerce")
        if "equity_usdt" in eq_df.columns:
            eq_df["equity_usdt"] = pd.to_numeric(eq_df["equity_usdt"], errors="coerce")

    return trades_df, eq_df, metrics


def compute_drawdown(eq: pd.Series):
    e = eq.astype(float).values
    if len(e) == 0:
        return np.array([])
    peak = np.maximum.accumulate(e)
    dd = (e - peak) / peak
    return dd


def monthly_returns_from_equity(eq_df: pd.DataFrame):
    if eq_df.empty or "ts" not in eq_df.columns or "equity_usdt" not in eq_df.columns:
        return pd.Series(dtype=float)

    df = eq_df.dropna(subset=["ts", "equity_usdt"]).sort_values("ts").copy()
    if df.empty:
        return pd.Series(dtype=float)

    df["month"] = df["ts"].dt.to_period("M").astype(str)
    first_last = df.groupby("month")["equity_usdt"].agg(["first", "last"])
    ret = (first_last["last"] / first_last["first"]) - 1.0
    ret.name = "monthly_return"
    return ret


def save_chart(path: str, title: str, x, y, xlabel="", ylabel=""):
    plt.figure()
    plt.plot(x, y)
    plt.title(title)
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()


def save_hist(path: str, title: str, data, xlabel=""):
    data = np.asarray(data)
    data = data[np.isfinite(data)]
    plt.figure()
    plt.hist(data, bins=50)
    plt.title(title)
    if xlabel:
        plt.xlabel(xlabel)
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()


def build_summary_table(metrics: dict):
    # pick the most useful headline numbers
    rows = []
    def add(k, label=None, fmt=None):
        if label is None:
            label = k
        v = metrics.get(k, None)
        if v is None:
            return
        if fmt == "pct":
            try:
                rows.append([label, f"{float(v):.3f}%"])
            except Exception:
                rows.append([label, str(v)])
        elif fmt == "float":
            try:
                rows.append([label, f"{float(v):.6f}"])
            except Exception:
                rows.append([label, str(v)])
        else:
            rows.append([label, str(v)])

    add("dataset_start", "Dataset start")
    add("dataset_end", "Dataset end")
    add("trades", "Trades")
    add("winrate", "Winrate", None)
    add("profit_factor", "Profit factor", "float")
    add("return_pct", "Return %", "pct")
    add("max_drawdown_pct", "Max DD %", "pct")
    add("final_equity_usdt", "Final equity", "float")
    add("trades_per_month_avg", "Trades / month avg", "float")

    # costs
    if "total_funding_cost_usdt" in metrics:
        add("total_funding_cost_usdt", "Total funding cost", "float")
    if "avg_funding_cost_per_trade" in metrics:
        add("avg_funding_cost_per_trade", "Avg funding / trade", "float")

    data = [["Metric", "Value"]] + rows
    return data


def generate_pdf(out_pdf: str, outdir: str):
    trades_df, eq_df, metrics = load_artifacts(outdir)

    # Create charts to embed
    tmp_dir = os.path.join(outdir, "_audit_pack_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    charts = {}

    # Equity & drawdown
    if not eq_df.empty and {"ts", "equity_usdt"}.issubset(eq_df.columns):
        df = eq_df.dropna(subset=["ts", "equity_usdt"]).sort_values("ts")
        x = df["ts"].dt.tz_convert(None)
        y = df["equity_usdt"].astype(float).values
        equity_png = os.path.join(tmp_dir, "equity.png")
        save_chart(equity_png, "Equity curve", x, y, xlabel="Time", ylabel="Equity (USDT)")
        charts["equity"] = equity_png

        dd = compute_drawdown(df["equity_usdt"])
        dd_png = os.path.join(tmp_dir, "drawdown.png")
        save_chart(dd_png, "Drawdown (fraction)", x, dd, xlabel="Time", ylabel="Drawdown")
        charts["drawdown"] = dd_png

        mret = monthly_returns_from_equity(df)
        if len(mret) > 0:
            mr_png = os.path.join(tmp_dir, "monthly_returns.png")
            plt.figure()
            plt.bar(mret.index.astype(str), mret.values)
            plt.title("Monthly returns (fraction)")
            plt.xticks(rotation=60, ha="right")
            plt.tight_layout()
            plt.savefig(mr_png, dpi=160)
            plt.close()
            charts["monthly_returns"] = mr_png

    # Trade distributions
    if not trades_df.empty:
        if "pnl_net_usdt" in trades_df.columns:
            pnl_png = os.path.join(tmp_dir, "pnl_hist.png")
            save_hist(pnl_png, "PnL net distribution (USDT)", trades_df["pnl_net_usdt"].values, xlabel="PnL net (USDT)")
            charts["pnl_hist"] = pnl_png

        if "exit_reason" in trades_df.columns:
            vc = trades_df["exit_reason"].astype(str).value_counts()
            if len(vc) > 0:
                er_png = os.path.join(tmp_dir, "exit_reasons.png")
                plt.figure()
                plt.bar(vc.index.astype(str), vc.values)
                plt.title("Exit reasons (count)")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                plt.savefig(er_png, dpi=160)
                plt.close()
                charts["exit_reasons"] = er_png

    # Build PDF
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(out_pdf, pagesize=A4, title="Audit Pack")
    story = []

    title = f"AUDIT PACK — SOLUSDT VWAP Bot"
    story.append(Paragraph(title, styles["Title"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph(f"Outdir: {outdir}", styles["Normal"]))
    story.append(Paragraph(f"Generated: {datetime.utcnow().isoformat()}Z", styles["Normal"]))
    story.append(Spacer(1, 12))

    # Summary table
    summary = build_summary_table(metrics)
    tbl = Table(summary, hAlign="LEFT", colWidths=[180, 340])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(Paragraph("Summary", styles["Heading2"]))
    story.append(tbl)
    story.append(Spacer(1, 12))

    # Add charts
    def add_img(path, caption):
        story.append(Paragraph(caption, styles["Heading3"]))
        story.append(Image(path, width=520, height=300))
        story.append(Spacer(1, 10))

    if "equity" in charts:
        add_img(charts["equity"], "Equity curve")
    if "drawdown" in charts:
        add_img(charts["drawdown"], "Drawdown")
    if "monthly_returns" in charts:
        add_img(charts["monthly_returns"], "Monthly returns")
    if "pnl_hist" in charts:
        add_img(charts["pnl_hist"], "Trade PnL net distribution")
    if "exit_reasons" in charts:
        add_img(charts["exit_reasons"], "Exit reasons")

    # Last trades table (small)
    if not trades_df.empty:
        cols = [c for c in ["side","entry_ts","exit_ts","pnl_net_usdt","pnl_R","exit_reason","took_tp1","funding_cost_usdt","fees_usdt"] if c in trades_df.columns]
        tail = trades_df.sort_values("exit_ts" if "exit_ts" in trades_df.columns else trades_df.index).tail(12)
        if len(cols) > 0 and not tail.empty:
            story.append(Paragraph("Last trades (tail)", styles["Heading2"]))
            view = tail[cols].copy()
            # stringify for reportlab
            data = [cols] + view.astype(str).values.tolist()
            t = Table(data, hAlign="LEFT")
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]))
            story.append(t)

    doc.build(story)
    print(f"Saved PDF: {out_pdf}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", type=str, default="./results")
    ap.add_argument("--pdf", type=str, default=None)
    args = ap.parse_args()

    outdir = args.outdir
    out_pdf = args.pdf or os.path.join(outdir, "audit_pack.pdf")
    generate_pdf(out_pdf, outdir)


if __name__ == "__main__":
    main()
