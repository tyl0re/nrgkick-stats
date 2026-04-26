"""
NRGkick Statistiken (interaktiv)
--------------------------------
Erzeugt einen interaktiven HTML-Report aus der SQLite-DB des NRGkick-Loggers.

Alle Grafiken sind **interaktiv** (Plotly):
  - Box-Zoom: Bereich mit der Maus aufziehen
  - Pan:      mit gedrueckter Shift-Taste verschieben
  - Reset:    Doppelklick oder Button oben rechts
  - Legende:  Einzelne Serien per Klick ein-/ausblenden
  - Zeit-Shortcuts (1h / 6h / 24h / 7d / Alles) im Zeitreihen-Plot

Aufrufe:
    python nrgkick_stats.py                            # 24h, Default-Tab Dashboard
    python nrgkick_stats.py --range 7d --default temps --open
    python nrgkick_stats.py --range all --open
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd


from nrgkick_config import (
    load_config as _load_cfg,
    db_path as _db_path,
    reports_dir as _reports_dir,
    DEFAULTS as _CFG_DEFAULTS,
)


log = logging.getLogger(__name__)


# Wird in main() mit der konkreten Config gefuellt, damit Hilfsfunktionen
# weiterhin Zugriff haben, ohne die Signatur ueberall aendern zu muessen.
CFG: dict = {}
REPORT_RANGE_NAME = "24h"


def _cfg_get(path: str, default=None):
    """Holt einen verschachtelten Config-Wert via 'section.key.sub'."""
    node = CFG if CFG else _CFG_DEFAULTS
    for part in path.split("."):
        if not isinstance(node, dict):
            return default
        node = node.get(part)
        if node is None:
            return default
    return node


def _db_file() -> Path:
    return _db_path(CFG) if CFG else _db_path(_CFG_DEFAULTS)


def _report_dir() -> Path:
    return _reports_dir(CFG) if CFG else _reports_dir(_CFG_DEFAULTS)


def _report_tzinfo():
    tz_name = _cfg_get("ui.timezone", None)
    if tz_name:
        try:
            return ZoneInfo(str(tz_name))
        except ZoneInfoNotFoundError as exc:
            raise SystemExit(f"Unbekannte ui.timezone: {tz_name}") from exc
    return datetime.now().astimezone().tzinfo


# Legacy-kompatible Symbole (damit wenig Code geaendert werden muss)
DATA_DIR = _db_file().parent
DB_FILE = _db_file()
REPORT_DIR = _report_dir()

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


# ---------------------------------------------------------------------------
# Zeitraum / Daten
# ---------------------------------------------------------------------------

RANGES = {
    "today": None,
    "24h":  timedelta(hours=24),
    "7d":   timedelta(days=7),
    "30d":  timedelta(days=30),
    "all":  None,
}


def resolve_range(name: str) -> tuple[datetime | None, datetime]:
    now = datetime.now(timezone.utc).astimezone(_report_tzinfo())
    if name == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    if name == "all":
        return None, now
    delta = RANGES.get(name)
    if delta is None:
        raise SystemExit(f"Unbekannter --range: {name}")
    return now - delta, now


def load_samples(start: datetime | None, end: datetime) -> pd.DataFrame:
    if not DB_FILE.exists():
        raise SystemExit(f"DB nicht gefunden: {DB_FILE}")
    with sqlite3.connect(str(DB_FILE)) as conn:
        if start is None:
            df = pd.read_sql_query(
                "SELECT * FROM samples ORDER BY ts_utc ASC", conn
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM samples WHERE ts_utc >= ? ORDER BY ts_utc ASC",
                conn,
                params=(start.astimezone(timezone.utc).isoformat(timespec="seconds"),),
            )
    if df.empty:
        return df
    # Fuer DST/sommer-winterzeitfeste Auswertung immer ts_utc als gemeinsame
    # Basis parsen und erst danach in die Anzeige-Zeitzone konvertieren.
    # Direkte ts_local-Parse scheitert bei gemischten Offsets (+01/+02).
    df["ts_local_dt"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert(_report_tzinfo())
    df = df.set_index("ts_local_dt").sort_index()
    return df


# ---------------------------------------------------------------------------
# Plotly figure builders
# Jeder Builder liefert ein dict mit Keys "data" und "layout" (JSON-serialisierbar).
# ---------------------------------------------------------------------------

def _ts_to_list(idx: pd.DatetimeIndex) -> list[str]:
    # Plotly kommt am besten mit ISO-Strings klar (bleibt zeitzonen-neutral
    # so wie die ts_local-Spalte in der DB).
    return [t.isoformat(sep=" ", timespec="seconds") for t in idx]


def _col(df: pd.DataFrame, col: str) -> list | None:
    if col not in df.columns:
        return None
    s = df[col]
    if s.notna().sum() == 0:
        return None
    # NaN -> None (damit Plotly die Luecken zeichnet)
    return [None if pd.isna(v) else (float(v) if isinstance(v, (int, float, np.floating, np.integer)) else v)
            for v in s.tolist()]


# Gemeinsames Layout-Fragment fuer Zeitreihen -------------------------------

def _timeseries_layout(title: str, yaxis_title: str, height: int = 420) -> dict:
    last_button_label = "Alles" if REPORT_RANGE_NAME == "all" else "Zeitraum"
    return {
        "title": title,
        "height": height,
        "margin": {"l": 60, "r": 20, "t": 55, "b": 50},
        "hovermode": "x unified",
        "xaxis": {
            "title": "Zeit",
            "rangeslider": {"visible": False},
            "rangeselector": {
                "buttons": [
                    {"count": 1,  "step": "hour", "stepmode": "backward", "label": "1h"},
                    {"count": 6,  "step": "hour", "stepmode": "backward", "label": "6h"},
                    {"count": 24, "step": "hour", "stepmode": "backward", "label": "24h"},
                    {"count": 7,  "step": "day",  "stepmode": "backward", "label": "7T"},
                    {"count": 30, "step": "day",  "stepmode": "backward", "label": "30T"},
                    {"step": "all", "label": last_button_label},
                ]
            },
            "type": "date",
        },
        "yaxis": {"title": yaxis_title, "zeroline": False},
        "legend": {"orientation": "h", "y": -0.2},
        "template": "plotly_white",
    }


# Temperatures --------------------------------------------------------------
#
# Alle von der NRGkick gelieferten Temperatur-Sensoren, gruppiert nach Ort:
#   * Auto-Seite (Typ2-Stecker): connector_l1/l2/l3 und ggf. N
#   * Wand-Seite (Schuko-Adapter): domestic_plug_1/_2
#   * Gehaeuse: housing
#
# legendgroup erlaubt gruppiertes Ein-/Ausblenden per Klick in der Legende.

TEMP_DEFS: list[tuple[str, str, str, str]] = [
    # (col, label, color, group)
    ("temp_connector_l1",    "Typ2-Stecker L1",   "#1f77b4", "auto"),
    ("temp_connector_l2",    "Typ2-Stecker L2",   "#2ca02c", "auto"),
    ("temp_connector_l3",    "Typ2-Stecker L3",   "#9467bd", "auto"),
    ("temp_connector_n",     "Typ2-Stecker N",    "#7f7f7f", "auto"),
    ("temp_domestic_plug_1", "Schuko Sensor 1",   "#ff7f0e", "schuko"),
    ("temp_domestic_plug_2", "Schuko Sensor 2",   "#e45756", "schuko"),
    ("temp_domestic_plug",   "Schuko (Mittel)",   "#ffbf7f", "schuko"),
    ("temp_housing",         "Gehaeuse",          "#d62728", "housing"),
]

TEMP_GROUP_LABELS = {
    "auto":    "Auto-Seite (Typ2)",
    "schuko":  "Wand-Seite (Schuko)",
    "housing": "Gehaeuse",
}


def _build_temp_traces(df: pd.DataFrame, *,
                       use_scattergl: bool = True,
                       include_legacy_mean: bool = True) -> list[dict]:
    """Liefert Plotly-Traces fuer alle vorhandenen Temperatur-Sensoren."""
    x = _ts_to_list(df.index)
    traces: list[dict] = []
    mode = "scattergl" if use_scattergl else "scatter"
    min_points = 5
    for col, label, color, group in TEMP_DEFS:
        if col == "temp_domestic_plug" and not include_legacy_mean:
            continue
        y = _col(df, col)
        if y is None:
            continue
        if col in {"temp_domestic_plug_1", "temp_domestic_plug_2"}:
            valid_points = sum(v is not None for v in y)
            if valid_points < min_points:
                continue
        dash = "dot" if col == "temp_domestic_plug" else "solid"
        traces.append({
            "type": mode,
            "mode": "lines",
            "x": x, "y": y,
            "name": label,
            "legendgroup": group,
            "legendgrouptitle": {"text": TEMP_GROUP_LABELS[group]},
            "line": {"color": color, "width": 1.5, "dash": dash},
            "hovertemplate": "%{y:.2f} °C<extra>" + label + "</extra>",
        })
    return traces


def fig_temperatures_all(df: pd.DataFrame) -> dict | None:
    """Uebersichts-Plot: alle Temperatursensoren in einem Diagramm mit
    gruppierter Legende (klickbar)."""
    traces = _build_temp_traces(df, use_scattergl=True, include_legacy_mean=True)
    if not traces:
        return None
    layout = _timeseries_layout(
        "Alle Temperatur-Sensoren", "Temperatur (°C)", height=500,
    )
    layout["legend"] = {
        "orientation": "v",
        "x": 1.02, "y": 1.0,
        "groupclick": "togglegroup",
    }
    layout["margin"] = {"l": 60, "r": 180, "t": 55, "b": 50}
    return {"data": traces, "layout": layout}


def fig_temperatures(df: pd.DataFrame) -> dict | None:
    """Klassische Temperaturen-Uebersicht (kompakt, ohne Schuko-Einzelsensoren)."""
    # Nur die "Haupt"-Sensoren: Gehaeuse + 3 Phasen + Schuko-Mittel
    keep = {"temp_housing", "temp_connector_l1", "temp_connector_l2",
            "temp_connector_l3", "temp_connector_n", "temp_domestic_plug"}
    x = _ts_to_list(df.index)
    traces = []
    for col, label, color, group in TEMP_DEFS:
        if col not in keep:
            continue
        y = _col(df, col)
        if y is None:
            continue
        # Fallback fuer "Schuko (Mittel)" -> Label ohne "Mittel"-Klammer,
        # wenn Einzel-Sensoren nicht vorhanden sind
        display_label = label
        if col == "temp_domestic_plug":
            display_label = "Schuko-Stecker"
        traces.append({
            "type": "scattergl",
            "mode": "lines",
            "x": x, "y": y,
            "name": display_label,
            "line": {"color": color, "width": 1.6},
            "hovertemplate": "%{y:.2f} °C<extra>" + display_label + "</extra>",
        })
    if not traces:
        return None
    return {"data": traces, "layout": _timeseries_layout("Temperaturen (Kompakt)", "Temperatur (°C)")}


def temperature_tiles_html(df: pd.DataFrame) -> str:
    """Erzeugt Kacheln mit aktuellem Wert, Min/Max, farbcodiert (Ampel)."""
    if df.empty:
        return ""
    # Welche Sensoren vorhanden sind
    present: list[tuple[str, str, str]] = []  # (col, label, group)
    for col, label, _color, group in TEMP_DEFS:
        if col == "temp_domestic_plug":
            continue  # Mittelwert nicht als Kachel
        if col in df.columns and df[col].notna().any():
            present.append((col, label, group))
    if not present:
        return ""

    def _color_for(v: float) -> tuple[str, str]:
        """Liefert (Hintergrund, Akzent-Farbe) abhaengig von Temperatur."""
        if v >= 75:
            return ("rgba(214,39,40,0.18)", "#d62728")
        if v >= 60:
            return ("rgba(255,127,14,0.18)", "#ff7f0e")
        if v >= 40:
            return ("rgba(255,215,0,0.18)", "#c9a227")
        return ("rgba(44,160,44,0.12)", "#2ca02c")

    tiles: list[str] = []
    for col, label, group in present:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            continue
        cur = float(s.iloc[-1])
        mn = float(s.min())
        mx = float(s.max())
        bg, accent = _color_for(cur)
        tiles.append(
            f'<div class="temp-tile" style="background:{bg};border-color:{accent}">'
            f'<div class="tt-label">{label}</div>'
            f'<div class="tt-value" style="color:{accent}">{cur:.1f} <span>°C</span></div>'
            f'<div class="tt-range">min {mn:.1f} / max <b>{mx:.1f}</b></div>'
            f'</div>'
        )
    return '<div class="temp-tiles">' + "".join(tiles) + '</div>'


# Power & Current -----------------------------------------------------------

def fig_power_current(df: pd.DataFrame) -> dict | None:
    if "power_w" not in df.columns or df["power_w"].notna().sum() == 0:
        return None
    x = _ts_to_list(df.index)
    traces = [{
        "type": "scattergl",
        "mode": "lines",
        "x": x, "y": _col(df, "power_w"),
        "name": "Leistung (W)",
        "line": {"color": "#9467bd", "width": 1.5},
        "hovertemplate": "%{y:.0f} W<extra>Leistung</extra>",
        "yaxis": "y1",
    }]

    palette = [("current_l1_a", "I L1", "#1f77b4"),
               ("current_l2_a", "I L2", "#2ca02c"),
               ("current_l3_a", "I L3", "#d62728")]
    for col, label, color in palette:
        y = _col(df, col)
        if y is None:
            continue
        traces.append({
            "type": "scattergl",
            "mode": "lines",
            "x": x, "y": y,
            "name": label,
            "line": {"color": color, "width": 1.2, "dash": "solid"},
            "hovertemplate": "%{y:.2f} A<extra>" + label + "</extra>",
            "yaxis": "y2",
        })

    if "set_current_a" in df.columns and df["set_current_a"].notna().any():
        traces.append({
            "type": "scattergl",
            "mode": "lines",
            "x": x, "y": _col(df, "set_current_a"),
            "name": "I soll",
            "line": {"color": "#888", "width": 1.0, "dash": "dash"},
            "hovertemplate": "%{y:.1f} A<extra>I soll</extra>",
            "yaxis": "y2",
        })

    layout = _timeseries_layout("Leistung & Strom je Phase", "Leistung (W)", height=520)
    # 2. Y-Achse rechts fuer Ampere
    layout["yaxis2"] = {
        "title": "Strom (A)",
        "overlaying": "y",
        "side": "right",
        "zeroline": False,
        "showgrid": False,
    }
    return {"data": traces, "layout": layout}


# Energy per day ------------------------------------------------------------

def fig_energy_per_day(df: pd.DataFrame) -> tuple[dict | None, pd.DataFrame | None]:
    if "energy_total_wh" not in df.columns or df["energy_total_wh"].isna().all():
        return None, None
    s = df["energy_total_wh"].dropna()
    if s.empty:
        return None, None
    daily = s.groupby(s.index.normalize()).agg(["first", "last"])
    daily["wh"] = (daily["last"] - daily["first"]).clip(lower=0)
    daily["kwh"] = daily["wh"] / 1000.0
    if daily["kwh"].sum() == 0:
        return None, daily

    trace = {
        "type": "bar",
        "x": [d.strftime("%Y-%m-%d") for d in daily.index],
        "y": [float(v) for v in daily["kwh"].tolist()],
        "text": [f"{v:.2f}" if v > 0 else "" for v in daily["kwh"].tolist()],
        "textposition": "outside",
        "marker": {"color": "#2ca02c", "line": {"color": "#1b5e20", "width": 1}},
        "hovertemplate": "%{x}<br>%{y:.2f} kWh<extra></extra>",
        "name": "kWh/Tag",
    }
    layout = {
        "title": "Lademenge je Tag",
        "height": 420,
        "margin": {"l": 60, "r": 20, "t": 55, "b": 70},
        "xaxis": {"title": "Datum", "type": "category"},
        "yaxis": {"title": "Energie (kWh)"},
        "template": "plotly_white",
    }
    return {"data": [trace], "layout": layout}, daily


# Heatmap power -------------------------------------------------------------

def fig_power_heatmap(df: pd.DataFrame) -> dict | None:
    if "power_w" not in df.columns or df["power_w"].isna().all():
        return None
    s = df["power_w"].copy()
    # Standby/Idle: alles unter "standby_power_w" (Config) zaehlt als
    # "nicht geladen" -> wird in der Heatmap nicht angezeigt.
    standby_w = float(_cfg_get("thresholds.standby_power_w", 50.0))
    s = s.where(s >= standby_w)
    if s.dropna().empty:
        return None
    pivot = (
        s.groupby([s.index.normalize(), s.index.hour])
         .mean()
         .unstack(level=1)
    )
    if pivot.empty:
        return None
    pivot = pivot.reindex(columns=range(24))
    # NaN -> None (JSON null), damit Plotly die Zellen als Luecken zeichnet
    # statt sie mit der mittleren Farbe der Colorscale zu fuellen.
    z = [[None if pd.isna(v) else float(v) for v in row]
         for row in pivot.values.tolist()]
    y = [d.strftime("%a %d.%m") for d in pivot.index]
    x = [f"{h:02d}" for h in range(24)]

    # Max Leistung fuer Farbskalierung
    zmax = float(pivot.max().max())

    # Colorscale aus Config (default: blau -> gruen -> orange -> rot)
    colorscale = _cfg_get("ui.heatmap_colorscale") or [
        [0.00, "#2c3e8f"], [0.25, "#3b9cff"], [0.50, "#2ca02c"],
        [0.75, "#ff7f0e"], [1.00, "#d62728"],
    ]

    trace = {
        "type": "heatmap",
        "x": x, "y": y, "z": z,
        "zmin": 0,
        "zmax": max(zmax, 1.0),
        "zauto": False,
        "colorscale": colorscale,
        "colorbar": {"title": "W"},
        "hoverongaps": False,  # leere Zellen nicht hovern
        "connectgaps": False,  # und nicht farblich fuellen
        "xgap": 1, "ygap": 1, # 1px Abstand zwischen Zellen -> leere sichtbar
        "hovertemplate": "%{y} %{x}h<br>%{z:.0f} W<extra></extra>",
    }
    layout = {
        "title": "Ladeaktivitaet - mittlere Leistung (W) nach Tag &amp; Stunde",
        "height": max(280, 28 * len(pivot) + 120),
        "margin": {"l": 100, "r": 20, "t": 55, "b": 50},
        # categoryorder + category-Typ zwingt Plotly, alle 24 Stunden als
        # gleich breite Kategorien darzustellen (sonst wirken "00-02" schmaler
        # als die grosse Luecke dazwischen)
        "xaxis": {
            "title": "Stunde",
            "type": "category",
            "showgrid": False,
            "tickmode": "array",
            "tickvals": x,
            "ticktext": x,
        },
        "yaxis": {
            "type": "category",
            "autorange": "reversed",
            "showgrid": False,
        },
        "template": "plotly_white",
        # Plot-Hintergrund weiss (bzw. dunkel im Dark-Mode via transparent):
        # leere Zellen wirken damit klar als "Luecke"
        "plot_bgcolor": "rgba(0,0,0,0)",
    }
    return {"data": [trace], "layout": layout}


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def detect_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Erkennt zusammenhaengende CHARGING-Phasen und versucht, Session-Startpunkte
    ueber energy_session_wh-Spruenge zu detektieren (wenn Logger nicht lueckenlos war).

    Returns:
        DataFrame mit is_charging-Spalte und session_id.
    """
    if df.empty or "charging_state" not in df.columns:
        return df.assign(is_charging=0, session_id=-1)

    s = df.copy()
    s["is_charging"] = (s["charging_state"] == "CHARGING").astype(int)

    # Session-Grenzen detektieren durch Luecken > X Minuten
    dt = s.index.to_series().diff().dt.total_seconds().fillna(0)
    positive_dt = dt[dt > 0]
    median_dt = float(positive_dt.median()) if not positive_dt.empty else 0.0
    min_gap_s = float(_cfg_get("thresholds.session_gap_minutes", 15.0)) * 60.0
    gap_threshold = max(median_dt * 3.0, min_gap_s)

    gap_break = (dt > gap_threshold).astype(int)
    state_change = (s["is_charging"].diff().abs().fillna(1) > 0).astype(int)
    s["group"] = (state_change | gap_break).cumsum()

    # Energie-Spruenge suchen (wenn energy_session_wh zurueckgesetzt wurde)
    if "energy_session_wh" in s.columns:
        e = pd.to_numeric(s["energy_session_wh"], errors="coerce")
        gap = e.diff()
        large_drop = gap < -50  # Groesser als -50 Wh Sprung (Reset)
        reset_idxs = s.index[large_drop].tolist()

        if len(reset_idxs) > 1:
            log.info("energy_session_wh Resets gefunden bei %d Zeitpunkten", len(reset_idxs))

    sessions = []
    for _gid, grp in s.groupby("group"):
        if grp["is_charging"].iloc[0] != 1:
            continue
        start = grp.index[0]
        end   = grp.index[-1]
        energy_wh = None
        if "energy_total_wh" in grp and grp["energy_total_wh"].notna().any():
            col = grp["energy_total_wh"].dropna()
            if len(col) >= 2:
                energy_wh = float(col.iloc[-1] - col.iloc[0])
        if energy_wh is None and "energy_session_wh" in grp and grp["energy_session_wh"].notna().any():
            energy_wh = float(grp["energy_session_wh"].max())
        max_p = float(grp["power_w"].max()) if "power_w" in grp else float("nan")
        if "current_l1_a" in grp:
            phase_mean = pd.concat(
                [grp.get("current_l1_a"), grp.get("current_l2_a"), grp.get("current_l3_a")],
                axis=1,
            ).mean(axis=1).mean()
            mean_i = float(phase_mean) if pd.notna(phase_mean) else float("nan")
        else:
            mean_i = float("nan")
        energy_session_start = None
        energy_session_end = None
        energy_total_start = None
        energy_total_end = None
        if "energy_session_wh" in grp and grp["energy_session_wh"].notna().any():
            es = pd.to_numeric(grp["energy_session_wh"], errors="coerce").dropna()
            if not es.empty:
                energy_session_start = float(es.iloc[0])
                energy_session_end   = float(es.iloc[-1])
        if "energy_total_wh" in grp and grp["energy_total_wh"].notna().any():
            et = pd.to_numeric(grp["energy_total_wh"], errors="coerce").dropna()
            if not et.empty:
                energy_total_start = float(et.iloc[0])
                energy_total_end = float(et.iloc[-1])

        # Wenn der Gruppe-Start nicht dem ersten Reset folgt, versuche den Start ueber Resets zu finden
        actual_start = start
        if "energy_session_wh" in grp and len(grp) > 1:
            try:
                e_grp = pd.to_numeric(grp["energy_session_wh"], errors="coerce")
                # Suche Reset-Punkte innerhalb dieser Gruppe (groesse Sprung nach unten)
                drops = []
                for i in range(1, len(e_grp)):
                    if e_grp.iloc[i] < e_grp.iloc[i-1] - 50:
                        drops.append(grp.index[i])

                # Wenn es einen Reset gibt, ist der Start danach
                if len(drops) > 0 and drops[0] > start:
                    actual_start = drops[0]
                    log.info("Session %d Start ueber energy_reset von %s auf %s", _gid, start, actual_start)
            except Exception as e:
                log.debug("Failed to detect session start via reset: %s", e)

        sessions.append({
            "start":       start,
            "ende":        end,
            "dauer":       end - start,
            "energie_kwh": (energy_wh / 1000.0) if energy_wh is not None else None,
            "max_w":       max_p,
            "mittel_a":    mean_i,
            "samples":     int(len(grp)),
            "_es_start":   energy_session_start,
            "_es_end":     energy_session_end,
            "_et_start":   energy_total_start,
            "_et_end":     energy_total_end,
            "_start_est":  actual_start,
        })

    merged: list[dict] = []
    for cur in sessions:
        if not merged:
            merged.append(cur)
            continue
        prev = merged[-1]
        same_charge_counter = (
            prev.get("_es_end") is not None and cur.get("_es_start") is not None
            and cur["_es_start"] >= prev["_es_end"]
        )
        same_total_counter = (
            prev.get("_et_end") is not None and cur.get("_et_start") is not None
            and cur["_et_start"] >= prev["_et_end"]
        )
        gap_s = (cur["start"] - prev["ende"]).total_seconds()
        # Messluecken koennen innerhalb derselben Ladung auftreten. Wenn die
        # Energiezaehler einfach weiterlaufen, fuehren wir beide Teile wieder
        # zu einer Sitzung zusammen.
        if gap_s > 0 and (same_charge_counter or same_total_counter):
            prev["ende"] = cur["ende"]
            prev["dauer"] = prev["ende"] - prev["start"]
            prev["samples"] += cur["samples"]
            prev["max_w"] = max(prev.get("max_w") or float("nan"), cur.get("max_w") or float("nan"))
            if prev.get("mittel_a") is None or pd.isna(prev.get("mittel_a")):
                prev["mittel_a"] = cur.get("mittel_a")
            elif cur.get("mittel_a") is not None and not pd.isna(cur.get("mittel_a")):
                prev["mittel_a"] = float(np.nanmean([prev["mittel_a"], cur["mittel_a"]]))
            if prev.get("_et_start") is not None and cur.get("_et_end") is not None:
                prev["energie_kwh"] = max(0.0, (cur["_et_end"] - prev["_et_start"]) / 1000.0)
            elif cur.get("_es_end") is not None:
                base = prev.get("_es_start") or 0.0
                prev["energie_kwh"] = max(0.0, (cur["_es_end"] - base) / 1000.0)
            prev["_es_end"] = cur.get("_es_end") if cur.get("_es_end") is not None else prev.get("_es_end")
            prev["_et_end"] = cur.get("_et_end") if cur.get("_et_end") is not None else prev.get("_et_end")
            continue
        merged.append(cur)

    out = pd.DataFrame(merged)
    return out.drop(columns=[c for c in ["_es_start", "_es_end", "_et_start", "_et_end"] if c in out.columns])


def sessions_table_html(sess: pd.DataFrame) -> str:
    if sess.empty:
        return "<p><i>Keine Ladesitzungen im Zeitraum erkannt.</i></p>"
    df = sess.copy()
    df["start"] = df["start"].dt.strftime("%Y-%m-%d %H:%M")
    df["ende"]  = df["ende"].dt.strftime("%Y-%m-%d %H:%M")
    df["dauer"] = df["dauer"].apply(
        lambda td: f"{int(td.total_seconds() // 3600):d}h {int((td.total_seconds() % 3600) // 60):02d}m"
    )
    df["energie_kwh"] = df["energie_kwh"].map(
        lambda v: "-" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.2f}"
    )
    df["max_w"]    = df["max_w"].map(lambda v: "-" if pd.isna(v) else f"{v:.0f}")
    df["mittel_a"] = df["mittel_a"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    return df.to_html(
        index=False,
        columns=["start", "ende", "dauer", "energie_kwh", "max_w", "mittel_a", "samples"],
        classes="sessions",
        border=0,
        escape=False,
    )


def display_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Sichtbare Sitzungen fuer Dashboard/Ladesitzungen.

    Diese Sicht soll mit der Auswahl im Analyse-Tab konsistent sein und basiert
    daher auf den Einsteck-Bloecken statt auf separater CHARGING-Phasenlogik.
    """
    rows: list[dict] = []
    for start, end, sub in _connect_blocks(df):
        display_start = _session_connect_start(sub) or start
        energy_kwh = _session_energy_kwh(sub)
        max_w = float(sub["power_w"].max()) if "power_w" in sub and sub["power_w"].notna().any() else float("nan")
        phase_cols = [c for c in ["current_l1_a", "current_l2_a", "current_l3_a"] if c in sub.columns]
        if phase_cols:
            phase_mean = sub[phase_cols].mean(axis=1).mean()
            mean_i = float(phase_mean) if pd.notna(phase_mean) else float("nan")
        else:
            mean_i = float("nan")
        rows.append({
            "start": display_start,
            "ende": end,
            "dauer": end - display_start,
            "energie_kwh": energy_kwh,
            "max_w": max_w,
            "mittel_a": mean_i,
            "samples": int(len(sub)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# KPI
# ---------------------------------------------------------------------------

def kpi_html(df: pd.DataFrame, sess: pd.DataFrame) -> str:
    if df.empty:
        return ""
    items: list[tuple[str, str]] = []
    if "energy_total_wh" in df and df["energy_total_wh"].notna().any():
        s = df["energy_total_wh"].dropna()
        if len(s) >= 2:
            kwh = (s.iloc[-1] - s.iloc[0]) / 1000.0
            items.append((f"{kwh:.2f} kWh", "Energie im Zeitraum"))
    if "power_w" in df and df["power_w"].notna().any():
        items.append((f"{df['power_w'].max():.0f} W", "Spitzenleistung"))
        items.append((f"{df['power_w'].mean():.0f} W", "durchschn. Leistung"))
    if "temp_housing" in df and df["temp_housing"].notna().any():
        items.append((f"{df['temp_housing'].max():.1f} °C", "max. Gehaeuse"))
    # Typ2-Stecker am Auto
    connector_cols = [c for c in ["temp_connector_l1", "temp_connector_l2", "temp_connector_l3"]
                      if c in df.columns]
    if connector_cols:
        max_con = df[connector_cols].max().max()
        if pd.notna(max_con):
            items.append((f"{max_con:.1f} °C", "max. Typ2-Stecker (Auto)"))
    # Schuko-Adapter an der Wand - hier wird es typischerweise am heissesten
    plug_cols = [c for c in ["temp_domestic_plug", "temp_domestic_plug_1", "temp_domestic_plug_2"]
                 if c in df.columns]
    if plug_cols:
        max_plug = df[plug_cols].max().max()
        if pd.notna(max_plug):
            items.append((f"{max_plug:.1f} °C", "max. Schuko-Adapter (Wand)"))
    analysis_sessions = len(_connect_blocks(df))
    if analysis_sessions:
        items.append((str(analysis_sessions), "Ladevorgaenge"))
    elif not sess.empty:
        items.append((str(len(sess)), "Ladesitzungen"))
    if not sess.empty:
        total_h = sess["dauer"].sum().total_seconds() / 3600.0
        items.append((f"{total_h:.1f} h", "Ladezeit gesamt"))
    quality = _data_quality_stats(df)
    if quality.get("coverage_pct") is not None:
        items.append((f"{quality['coverage_pct']:.0f} %", "Datenabdeckung"))
    if quality.get("gap_count"):
        items.append((str(quality["gap_count"]), "Messluecken"))
        items.append((f"{quality['max_gap_min']:.0f} min", "groesste Luecke"))
    if not items:
        return ""
    parts = "".join(
        f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div></div>'
        for value, label in items
    )
    return f'<div class="kpis">{parts}</div>'


# ---------------------------------------------------------------------------
# "Aktuelle Session" = seit dem letzten Einstecken
# ---------------------------------------------------------------------------

def _augment_connect_time_from_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Fuellt vehicle_connect_time aus raw_values_json, falls Spalten fehlen/NaN.
    So koennen auch alte Samples genutzt werden, die vor der Migration entstanden
    sind."""
    if "vehicle_connect_time" in df.columns and df["vehicle_connect_time"].notna().any():
        # Schon vorhanden - nur luecken aus raw fuellen, falls raw_values_json da ist
        missing = df["vehicle_connect_time"].isna()
        if missing.any() and "raw_values_json" in df.columns:
            def _from_raw(s):
                if not isinstance(s, str):
                    return None
                try:
                    d = json.loads(s)
                    return (d.get("general") or {}).get("vehicle_connect_time")
                except Exception:
                    return None
            filled = df.loc[missing, "raw_values_json"].apply(_from_raw)
            df.loc[missing, "vehicle_connect_time"] = filled
        return df

    # Spalte fehlt komplett -> aus raw ableiten
    if "raw_values_json" in df.columns:
        def _from_raw(s):
            if not isinstance(s, str):
                return None
            try:
                d = json.loads(s)
                return (d.get("general") or {}).get("vehicle_connect_time")
            except Exception:
                return None
        df = df.copy()
        df["vehicle_connect_time"] = df["raw_values_json"].apply(_from_raw)
    return df


def find_current_session(df: pd.DataFrame) -> tuple[pd.DataFrame, bool | None]:
    """Liefert den DataFrame-Ausschnitt seit dem letzten 'Einstecken'.

    Heuristik anhand `vehicle_connect_time` (Sekunden seit Anschluss):
      - solange > 0 -> Auto ist angesteckt
      - ein Sprung auf 0 / None bedeutet: wurde abgezogen
      - wir suchen die *letzte* zusammenhaengende Phase, in der
        vehicle_connect_time > 0 ist UND die bis zum letzten Sample reicht.

    Returns: (session_df, start_from_counter)
      - session_df: DataFrame mit den Daten der aktuellen Session
      - start_from_counter: True wenn der Start per vehicle_connect_time berechnet wurde
                            None wenn der erste Messpunkt als Start verwendet wird
    """
    if df.empty:
        return pd.DataFrame(), None

    df = _augment_connect_time_from_raw(df)
    if "vehicle_connect_time" not in df.columns:
        return pd.DataFrame(), None

    ct = pd.to_numeric(df["vehicle_connect_time"], errors="coerce")
    connected = ct.fillna(0) > 0

    # Manche Firmware-/API-Staende lassen vehicle_connect_time nach dem
    # Abziehen stehen. STANDBY bedeutet laut Status-Enum aber explizit:
    # kein Fahrzeug angesteckt. Daher beendet STANDBY eine aktuelle Session.
    if "charging_state" in df.columns:
        state = df["charging_state"].fillna("").astype(str).str.upper()
        connected &= state.ne("STANDBY")

    if not connected.any():
        return pd.DataFrame(), None

    # Wenn das letzte Sample "nicht mehr verbunden" ist -> gar keine aktive Session
    if not bool(connected.iloc[-1]):
        return pd.DataFrame(), None

    # Bloecke mit connected=True finden, den letzten nehmen
    block_id = (connected != connected.shift(fill_value=False)).cumsum()
    last_block = block_id.iloc[-1]

    mask = (block_id == last_block) & connected
    session_df = df.loc[mask].copy()
    return session_df, True if _session_connect_start(session_df) is not None else None


def _session_connect_start(sess_df: pd.DataFrame) -> pd.Timestamp | None:
    """Berechnet den Einsteckzeitpunkt aus dem Wallbox-Zaehler.

    `vehicle_connect_time` ist die Anzahl Sekunden seit dem Einstecken. Die
    Messzeitachse bleibt unveraendert; der zurueckgerechnete Start wird nur fuer
    Anzeige und Dauer-KPI genutzt.
    """
    if sess_df.empty or "vehicle_connect_time" not in sess_df:
        return None
    ct = pd.to_numeric(sess_df["vehicle_connect_time"], errors="coerce").dropna()
    if ct.empty:
        return None
    last_ct = float(ct.iloc[-1])
    if last_ct <= 0:
        return None
    return sess_df.index[-1] - pd.Timedelta(seconds=last_ct)


def _session_energy_kwh(sess_df: pd.DataFrame) -> float | None:
    """Lademenge fuer eine Einsteck-Session.

    Wenn der Report-Zeitraum erst nach dem Einstecken beginnt, ist die
    Lifetime-Zaehler-Differenz nur eine Teilmenge. Dann ist der Wallbox-
    Sessionzaehler konsistenter zu Startzeit und Dauer.
    """
    if sess_df.empty:
        return None

    es_kwh = None
    if "energy_session_wh" in sess_df and sess_df["energy_session_wh"].notna().any():
        es = pd.to_numeric(sess_df["energy_session_wh"], errors="coerce").dropna()
        if not es.empty:
            es_kwh = max(0.0, float(es.max()) / 1000.0)

    et_kwh = None
    if "energy_total_wh" in sess_df and sess_df["energy_total_wh"].notna().any():
        et = pd.to_numeric(sess_df["energy_total_wh"], errors="coerce").dropna()
        if len(et) >= 2:
            et_kwh = max(0.0, float(et.iloc[-1] - et.iloc[0]) / 1000.0)

    connect_start = _session_connect_start(sess_df)
    if connect_start is not None and connect_start < sess_df.index[0] and es_kwh is not None:
        return es_kwh
    return et_kwh if et_kwh is not None else es_kwh


def _fmt_duration(seconds: float) -> str:
    if seconds is None or pd.isna(seconds):
        return "-"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, _   = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    return f"{m}m"


def _fmt_optional_float(value, suffix: str = "") -> str:
    try:
        if value is None or pd.isna(value):
            return "-"
        return f"{float(value):.1f}{suffix}"
    except Exception:
        return "-"


def _configured_float(path: str) -> float | None:
    value = _cfg_get(path, None)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _session_cost_eur(kwh: float | None) -> float | None:
    price = _configured_float("costs.electricity_price_eur_per_kwh")
    if price is None or kwh is None:
        return None
    return max(0.0, float(kwh) * price)


def _session_co2_kg(kwh: float | None) -> float | None:
    # Show CO2 estimate; if no CO2 factor configured, fallback to 0.0
    if kwh is None:
        return None
    co2_g_per_kwh = _configured_float("costs.co2_g_per_kwh")
    if co2_g_per_kwh is None:
        return 0.0
    return max(0.0, float(kwh) * co2_g_per_kwh / 1000.0)


def _max_relevant_temp(sess_df: pd.DataFrame) -> float | None:
    temp_cols = [
        c for c in [
            "temp_domestic_plug",
            "temp_domestic_plug_1",
            "temp_domestic_plug_2",
            "temp_connector_l1",
            "temp_connector_l2",
            "temp_connector_l3",
            "temp_housing",
        ]
        if c in sess_df.columns and sess_df[c].notna().any()
    ]
    if not temp_cols:
        return None
    max_temp = pd.to_numeric(sess_df[temp_cols].stack(), errors="coerce").max()
    return float(max_temp) if pd.notna(max_temp) else None


def _data_quality_stats(df: pd.DataFrame) -> dict:
    if df.empty or len(df) < 2:
        return {}
    dt = df.index.to_series().diff().dt.total_seconds().dropna()
    if dt.empty:
        return {}
    interval_s = float(_cfg_get("polling.interval_seconds", 360.0))
    expected = int(max(1, round((df.index[-1] - df.index[0]).total_seconds() / interval_s) + 1))
    coverage = min(100.0, max(0.0, len(df) / expected * 100.0)) if expected else None
    gap_threshold = max(interval_s * 1.5, interval_s + 60.0)
    gaps = dt[dt > gap_threshold]
    return {
        "coverage_pct": coverage,
        "max_gap_min": float(dt.max()) / 60.0,
        "gap_count": int(len(gaps)),
        "expected_samples": expected,
    }


def current_session_kpis(sess_df: pd.DataFrame) -> list[tuple[str, str]]:
    """Berechnet KPIs für aktuelle Session.

    Berechnet die Einsteckdauer basierend auf vehicle_connect_time.
    """
    if sess_df.empty:
        return []

    start = sess_df.index[0]
    end   = sess_df.index[-1]

    connect_s = None
    if "vehicle_connect_time" in sess_df and sess_df["vehicle_connect_time"].notna().any():
        ct = pd.to_numeric(sess_df["vehicle_connect_time"], errors="coerce").dropna()
        if not ct.empty and float(ct.iloc[-1]) > 0:
            connect_s = float(ct.iloc[-1])

    if connect_s is None:
        connect_s = (end - start).total_seconds()

    charging_s = None
    if "vehicle_charging_time" in sess_df and sess_df["vehicle_charging_time"].notna().any():
        charging_s = float(pd.to_numeric(sess_df["vehicle_charging_time"], errors="coerce").dropna().iloc[-1])

    kwh = _session_energy_kwh(sess_df)

    cur_p = None
    if "power_w" in sess_df and sess_df["power_w"].notna().any():
        cur_p = float(sess_df["power_w"].iloc[-1])

    avg_p = None
    if charging_s and charging_s > 0 and kwh is not None:
        avg_p = (kwh * 1000.0) * 3600.0 / charging_s
    elif "power_w" in sess_df:
        # Mittel ueber aktive Ladephasen
        actively = sess_df.loc[sess_df["charging_state"] == "CHARGING", "power_w"]
        if not actively.empty:
            avg_p = float(actively.mean())

    state = sess_df["charging_state"].iloc[-1] if "charging_state" in sess_df else None
    set_i = sess_df["set_current_a"].iloc[-1] if "set_current_a" in sess_df else None

    # Energy-Limit (aktuellster control-Wert in Wh; 0 = kein Limit)
    limit_wh = None
    if "energy_limit_wh" in sess_df and sess_df["energy_limit_wh"].notna().any():
        lim = pd.to_numeric(sess_df["energy_limit_wh"], errors="coerce").dropna()
        if not lim.empty:
            limit_wh = float(lim.iloc[-1])

    items: list[tuple[str, str]] = []
    if kwh is not None:
        items.append((f"{kwh:.2f} kWh", "seit Einstecken geladen"))
    items.append((_fmt_duration(connect_s), "angesteckt seit"))
    if charging_s is not None:
        items.append((_fmt_duration(charging_s), "aktive Ladezeit"))
    standby_s = max(0.0, connect_s - charging_s) if charging_s is not None else None
    if standby_s is not None:
        items.append((_fmt_duration(standby_s), "Standzeit"))
    if kwh is not None and connect_s > 0:
        items.append((f"{(kwh * 3600.0 / connect_s):.2f} kW", "effektiv angesteckt"))
    if kwh is not None and charging_s and charging_s > 0:
        items.append((f"{(kwh * 3600.0 / charging_s):.2f} kW", "effektiv aktiv"))
    if cur_p is not None:
        items.append((f"{cur_p:.0f} W", "aktuelle Leistung"))
    if avg_p is not None:
        items.append((f"{avg_p:.0f} W", "durchschn. Leistung"))
    if state:
        items.append((str(state), "Status"))
    if set_i is not None and pd.notna(set_i):
        items.append((f"{float(set_i):.0f} A", "Soll-Strom"))
    max_temp = _max_relevant_temp(sess_df)
    hot_temp = float(_cfg_get("thresholds.temperature_hot", 75.0))
    if max_temp is not None:
        items.append((f"{max(0.0, hot_temp - max_temp):.1f} °C", "thermische Reserve"))
    cost = _session_cost_eur(kwh)
    if cost is not None:
        items.append((f"{cost:.2f} €", "Kosten geschaetzt"))
    co2 = _session_co2_kg(kwh)
    if co2 is not None:
        items.append((f"{co2:.1f} kg", "CO2 geschaetzt"))
    if limit_wh is not None:
        if limit_wh <= 0:
            items.append(("- kein Limit -", "Energy-Limit"))
        else:
            items.append((f"{limit_wh/1000.0:.2f} kWh", "Energy-Limit"))
    return items


def fig_session_status(sess_df: pd.DataFrame) -> dict | None:
    """Status-Band: farbige Zeitleiste CHARGING / IDLE / PAUSE / ERROR."""
    if sess_df.empty or "charging_state" not in sess_df.columns:
        return None
    color_map = {
        "CHARGING":  "#2ca02c",
        "WAITING":   "#1f77b4",
        "SIGNALED":  "#17becf",
        "CONNECTED": "#1f77b4",
        "PAUSED":    "#ff7f0e",
        "ERROR":     "#d62728",
    }
    default_color = "#9aa0a6"
    x = _ts_to_list(sess_df.index)
    states = sess_df["charging_state"].fillna("UNKNOWN").astype(str).tolist()
    uniq_order: list[str] = []
    for s in states:
        if s not in uniq_order:
            uniq_order.append(s)

    # Fuer ein "Gantt-artiges" Band: pro Zustand eine Serie, die genau dann y=1 hat
    # wenn der Zustand aktiv ist.
    traces = []
    for s in uniq_order:
        y = [1 if st == s else None for st in states]
        traces.append({
            "type": "scatter",
            "mode": "lines",
            "x": x, "y": y,
            "name": s,
            "line": {"color": color_map.get(s, default_color), "width": 18},
            "connectgaps": False,
            "hovertemplate": "%{x}<br>" + s + "<extra></extra>",
        })
    layout = {
        "title": "Status im Verlauf der Session",
        "height": 160,
        "margin": {"l": 60, "r": 20, "t": 40, "b": 40},
        "xaxis": {"title": None, "type": "date"},
        "yaxis": {"visible": False, "range": [0.5, 1.5]},
        "showlegend": True,
        "legend": {"orientation": "h", "y": -0.3},
        "template": "plotly_white",
    }
    return {"data": traces, "layout": layout}


def fig_session_energy(sess_df: pd.DataFrame) -> dict | None:
    """kWh seit Einstecken als monotone Kurve."""
    if sess_df.empty:
        return None
    if "energy_total_wh" not in sess_df.columns or sess_df["energy_total_wh"].isna().all():
        # fallback: energy_session_wh direkt
        if "energy_session_wh" in sess_df and sess_df["energy_session_wh"].notna().any():
            s = sess_df["energy_session_wh"].astype(float) / 1000.0
        else:
            return None
    else:
        base = sess_df["energy_total_wh"].ffill()
        s = (base - base.iloc[0]).clip(lower=0) / 1000.0

    x = _ts_to_list(sess_df.index)
    trace = {
        "type": "scatter",
        "mode": "lines",
        "x": x,
        "y": [float(v) if pd.notna(v) else None for v in s.tolist()],
        "name": "kWh seit Einstecken",
        "line": {"color": "#2ca02c", "width": 2.2},
        "fill": "tozeroy",
        "fillcolor": "rgba(44,160,44,0.15)",
        "hovertemplate": "%{x}<br>%{y:.3f} kWh<extra></extra>",
    }
    layout = _timeseries_layout("Energie seit Einstecken", "Energie (kWh)", height=360)
    # In einer aktiven Session brauchen wir die globalen Range-Shortcuts nicht
    layout["xaxis"].pop("rangeselector", None)
    return {"data": [trace], "layout": layout}


def fig_session_power(sess_df: pd.DataFrame) -> dict | None:
    if sess_df.empty or "power_w" not in sess_df.columns \
            or sess_df["power_w"].notna().sum() == 0:
        return None
    x = _ts_to_list(sess_df.index)
    trace = {
        "type": "scatter",
        "mode": "lines",
        "x": x, "y": _col(sess_df, "power_w"),
        "name": "Leistung",
        "line": {"color": "#1f77b4", "width": 1.8},
        "fill": "tozeroy",
        "fillcolor": "rgba(31,119,180,0.12)",
        "hovertemplate": "%{x}<br>%{y:.0f} W<extra></extra>",
    }
    layout = _timeseries_layout("Leistung waehrend der Session", "Leistung (W)", height=340)
    layout["xaxis"].pop("rangeselector", None)
    return {"data": [trace], "layout": layout}


def fig_session_currents(sess_df: pd.DataFrame) -> dict | None:
    if sess_df.empty:
        return None
    defs = [("current_l1_a", "I L1", "#1f77b4"),
            ("current_l2_a", "I L2", "#2ca02c"),
            ("current_l3_a", "I L3", "#d62728")]
    x = _ts_to_list(sess_df.index)
    traces = []
    for col, label, color in defs:
        y = _col(sess_df, col)
        if y is None:
            continue
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x, "y": y, "name": label,
            "line": {"color": color, "width": 1.5},
            "hovertemplate": "%{y:.2f} A<extra>" + label + "</extra>",
        })
    if "set_current_a" in sess_df.columns and sess_df["set_current_a"].notna().any():
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x, "y": _col(sess_df, "set_current_a"),
            "name": "I soll",
            "line": {"color": "#888", "width": 1.0, "dash": "dash"},
            "hovertemplate": "%{y:.1f} A<extra>I soll</extra>",
        })
    if not traces:
        return None
    layout = _timeseries_layout("Strom pro Phase", "Strom (A)", height=320)
    layout["xaxis"].pop("rangeselector", None)
    return {"data": traces, "layout": layout}


def _energy_limit_progress_html(sess_df: pd.DataFrame) -> str:
    """Erzeugt den Fortschrittsbalken fuer das Energy-Limit, falls eines
    gesetzt ist und bereits Ladeenergie vorhanden ist."""
    if "energy_limit_wh" not in sess_df.columns:
        return ""
    lim = pd.to_numeric(sess_df["energy_limit_wh"], errors="coerce").dropna()
    if lim.empty:
        return ""
    limit_wh = float(lim.iloc[-1])
    if limit_wh <= 0:
        return ""  # kein Limit gesetzt
    loaded_wh = (_session_energy_kwh(sess_df) or 0.0) * 1000.0

    pct = max(0.0, min(100.0, (loaded_wh / limit_wh) * 100.0))
    remaining_wh = max(0.0, limit_wh - loaded_wh)
    # Farbcodierung: orange ab 80 %, rot ab 95 % / erreicht
    if pct >= 95:
        bar_bg = "linear-gradient(90deg, #d62728, #b00)"
    elif pct >= 80:
        bar_bg = "linear-gradient(90deg, #ff7f0e, #d06000)"
    else:
        bar_bg = "linear-gradient(90deg, #2ca02c, #1b5e20)"

    return (
        '<div class="limit-card">'
        '<div class="limit-head">'
        f'<span><b>Energy-Limit:</b> {limit_wh/1000.0:.2f} kWh</span>'
        f'<span class="limit-stats">'
        f'{loaded_wh/1000.0:.2f} / {limit_wh/1000.0:.2f} kWh '
        f'&middot; noch <b>{remaining_wh/1000.0:.2f} kWh</b> '
        f'&middot; {pct:.0f} %</span>'
        '</div>'
        '<div class="limit-bar-bg">'
        f'<div class="limit-bar-fg" style="width:{pct:.1f}%;background:{bar_bg};"></div>'
        '</div>'
        '</div>'
    )


def current_session_html(sess_df: pd.DataFrame,
                         plots_out: dict,
                         start_from_counter: bool | None = None) -> str:
    """Baut den kompletten Panel-Inhalt fuer den 'current'-Tab
    und registriert die zugehoerigen Plot-Specs in plots_out.
    
    start_from_counter=True -> Start wurde aus vehicle_connect_time berechnet
    start_from_counter=None -> erster Messpunkt wird als Start angezeigt
    """
    if sess_df.empty:
        return ('<section class="panel" id="panel-current">'
                '<h2>Aktuelle Session</h2>'
                '<p><i>Momentan ist kein Fahrzeug angesteckt '
                '(oder die DB enthaelt keine Messwerte mit '
                '<code>vehicle_connect_time &gt; 0</code>). '
                'Sobald das Auto eingesteckt wird, zeigt dieser Tab den '
                'aktuellen Lade-Fortschritt.</i></p>'
                '</section>')

    # KPI
    items = current_session_kpis(sess_df)
    kpi_parts = "".join(
        f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div></div>'
        for value, label in items
    )
    kpi_block = f'<div class="kpis">{kpi_parts}</div>' if kpi_parts else ""
    # Fallback: ensure KPI strip is visible even if data is momentarily unavailable
    if not kpi_block:
        kpi_block = (
            '<div class="kpis">'
            '<div class="kpi"><div class="v">-</div><div class="l">Kosten geschaetzt</div></div>'
            '<div class="kpi"><div class="v">-</div><div class="l">CO2 geschaetzt</div></div>'
            '</div>'
        )
    limit_block = _energy_limit_progress_html(sess_df)

    # Figures registrieren
    fig_status   = fig_session_status(sess_df)
    fig_energy   = fig_session_energy(sess_df)
    fig_power    = fig_session_power(sess_df)
    fig_currents = fig_session_currents(sess_df)

    blocks: list[str] = []
    if fig_status:
        plots_out["plot-cur-status"] = fig_status
        blocks.append(_plot_div("plot-cur-status"))
    if fig_energy:
        plots_out["plot-cur-energy"] = fig_energy
        blocks.append(_plot_div("plot-cur-energy"))
    if fig_power:
        plots_out["plot-cur-power"] = fig_power
        blocks.append(_plot_div("plot-cur-power"))
    if fig_currents:
        plots_out["plot-cur-currents"] = fig_currents
        blocks.append(_plot_div("plot-cur-currents"))

    actual_start_ts = _session_connect_start(sess_df)
    start_ts = actual_start_ts if actual_start_ts is not None else sess_df.index[0]
    start_str = start_ts.strftime("%Y-%m-%d %H:%M")
    end_str   = sess_df.index[-1].strftime("%Y-%m-%d %H:%M")

    # Klar kommunizieren, wenn Startzeitpunkt ungewiss ist
    if start_from_counter is True:
        head = (f'<p class="sub">angesteckt seit <b>{start_str}</b>'
                f' (aus Wallbox-Zaehler) &middot; Stand <b>{end_str}</b> '
                f'&middot; {len(sess_df)} Messpunkte</p>')
    else:
        head = (f'<p class="sub">angesteckt seit <b>{start_str}</b> &middot; '
                f'Stand <b>{end_str}</b> &middot; {len(sess_df)} Messpunkte</p>')
    hint = ('<p class="hint">Bereich mit der Maus aufziehen = hineinzoomen '
            '&middot; Doppelklick = Reset &middot; '
            'Legenden-Eintraege toggeln Serien.</p>')

    return ('<section class="panel" id="panel-current">'
            '<h2>Aktuelle Session - seit dem letzten Einstecken</h2>'
            + head + kpi_block + limit_block + hint + "".join(blocks) +
            '</section>')


# ---------------------------------------------------------------------------
# Ladevorgang-Analyse (Tab "analysis")
# ---------------------------------------------------------------------------

def _connect_blocks(df: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp, pd.DataFrame]]:
    """Liefert alle Einsteck-Intervalle (vehicle_connect_time > 0 in einem
    zusammenhaengenden Block). Jede Session = "ans Auto anstecken bis abziehen"."""
    if df.empty:
        return []
    df = _augment_connect_time_from_raw(df)
    if "vehicle_connect_time" not in df.columns:
        return []
    ct = pd.to_numeric(df["vehicle_connect_time"], errors="coerce").fillna(0)
    connected = ct > 0

    # Gleiche Schutzlogik wie bei find_current_session(): vehicle_connect_time
    # kann nach dem Abziehen stehenbleiben, STANDBY bedeutet aber "kein Fahrzeug
    # angesteckt" und muss daher den Session-Block beenden.
    if "charging_state" in df.columns:
        state = df["charging_state"].fillna("").astype(str).str.upper()
        connected &= state.ne("STANDBY")

    if not connected.any():
        return []

    # zusammenhaengende True-Laeufe sammeln
    block_id = (connected != connected.shift(fill_value=False)).cumsum()
    results: list[tuple[pd.Timestamp, pd.Timestamp, pd.DataFrame]] = []
    for bid, is_conn_vals in zip(block_id.unique(),
                                 [connected[block_id == b].iloc[0] for b in block_id.unique()]):
        if not bool(is_conn_vals):
            continue
        sub = df[block_id == bid].copy()
        if not sub.empty:
            results.append((sub.index[0], sub.index[-1], sub))
    return results


def detect_derating_events(sess: pd.DataFrame) -> pd.DataFrame:
    """Erkennt Drosselungs-/Erhoehungs-Ereignisse der Wallbox.

    Heuristik (adaptiv, ohne fixe Schwellen):
      * Aenderungen im Soll-Strom (set_current_a) > 1 A innerhalb <= 2 min werden
        als Kandidat gewertet.
      * Wenn die maximale Temperatur (Stecker/Gehaeuse) im Fenster [-2min, +2min]
        ueber dem 75%-Quantil der bisherigen Session-Temps liegt, ist das Event
        "thermisch" (Derating). Sonst "manuell/gesteuert".
      * Erhoehungen nach einem thermischen Derating gelten als "Recovery", wenn
        die Temp zum Zeitpunkt >= 3 °C unter dem letzten thermischen Peak liegt.
    """
    if sess.empty or "set_current_a" not in sess.columns:
        return pd.DataFrame()
    s = sess.copy()
    s["set_current_a"] = pd.to_numeric(s["set_current_a"], errors="coerce")
    s["set_current_a"] = s["set_current_a"].ffill()

    temp_cols = [c for c in ["temp_connector_l1", "temp_connector_l2",
                             "temp_connector_l3", "temp_housing",
                             "temp_domestic_plug", "temp_domestic_plug_1",
                             "temp_domestic_plug_2"]
                 if c in s.columns and s[c].notna().any()]
    max_temp = s[temp_cols].max(axis=1) if temp_cols else pd.Series(dtype=float)

    # Quantil-Schwelle nur wenn genug Varianz (aus Config)
    min_delta_a     = float(_cfg_get("derating.min_delta_a",        1.0))
    window_minutes  = float(_cfg_get("derating.window_minutes",     2.0))
    temp_quantile   = float(_cfg_get("derating.temp_quantile",      0.75))
    recovery_cool_c = float(_cfg_get("derating.recovery_cooldown_c", 3.0))

    temp_threshold = None
    if not max_temp.empty and max_temp.notna().sum() >= 8:
        q = float(max_temp.quantile(temp_quantile))
        median = float(max_temp.median())
        if q - median >= 1.5:   # ueberhaupt spuerbare Temperatur-Dynamik
            temp_threshold = q

    diff = s["set_current_a"].diff()
    events: list[dict] = []
    last_thermal_peak = None
    window = pd.Timedelta(minutes=window_minutes)
    for ts, d in diff.items():
        if pd.isna(d) or abs(d) < min_delta_a:
            continue
        w_temp = max_temp.loc[ts - window: ts + window] \
            if not max_temp.empty else pd.Series(dtype=float)
        w_max = float(w_temp.max()) if not w_temp.empty and w_temp.notna().any() else None

        is_decrease = d < 0
        if is_decrease:
            thermal = (temp_threshold is not None and w_max is not None
                       and w_max >= temp_threshold)
            reason = "Derating (thermisch)" if thermal else "Reduktion"
            if thermal:
                last_thermal_peak = w_max
            events.append({
                "zeit": ts,
                "typ": reason,
                "delta_a": float(d),
                "set_a_vor": float(s["set_current_a"].shift(1).loc[ts]) if ts in s.index else None,
                "set_a_nach": float(s["set_current_a"].loc[ts]),
                "max_temp_c": w_max,
                "thermisch": bool(thermal),
            })
        else:
            # Erhoehung
            is_recovery = (last_thermal_peak is not None and w_max is not None
                           and w_max <= last_thermal_peak - recovery_cool_c)
            reason = "Recovery (Temp gesunken)" if is_recovery else "Erhoehung"
            events.append({
                "zeit": ts,
                "typ": reason,
                "delta_a": float(d),
                "set_a_vor": float(s["set_current_a"].shift(1).loc[ts]) if ts in s.index else None,
                "set_a_nach": float(s["set_current_a"].loc[ts]),
                "max_temp_c": w_max,
                "thermisch": bool(is_recovery),
            })
            if is_recovery:
                last_thermal_peak = None
    return pd.DataFrame(events)


# ---- Session-Aggregate ----------------------------------------------------

def session_aggregates(sess: pd.DataFrame, events: pd.DataFrame) -> dict:
    if sess.empty:
        return {}
    out: dict = {}
    out["start"] = sess.index[0]
    out["ende"]  = sess.index[-1]
    # Primaer: letzter vehicle_connect_time-Wert der API (zeigt wahre Dauer,
    # auch wenn die Session teilweise aus Zeiten vor DB-Migration kommt).
    if "vehicle_connect_time" in sess and sess["vehicle_connect_time"].notna().any():
        ct = pd.to_numeric(sess["vehicle_connect_time"], errors="coerce").dropna()
        out["angesteckt_min"] = float(ct.iloc[-1]) / 60.0
    else:
        out["angesteckt_min"] = (sess.index[-1] - sess.index[0]).total_seconds() / 60.0

    if "vehicle_charging_time" in sess and sess["vehicle_charging_time"].notna().any():
        out["aktiv_min"] = float(pd.to_numeric(
            sess["vehicle_charging_time"], errors="coerce").dropna().iloc[-1]) / 60.0
    else:
        out["aktiv_min"] = float((sess["charging_state"] == "CHARGING").sum())  # 1/min

    out["kwh"] = _session_energy_kwh(sess) or 0.0
    out["standby_min"] = max(0.0, out["angesteckt_min"] - out["aktiv_min"])
    out["p_eff_plugged_w"] = (
        out["kwh"] * 1000.0 * 60.0 / out["angesteckt_min"]
        if out["angesteckt_min"] > 0 else 0.0
    )
    out["p_eff_active_w"] = (
        out["kwh"] * 1000.0 * 60.0 / out["aktiv_min"]
        if out["aktiv_min"] > 0 else 0.0
    )
    out["cost_eur"] = _session_cost_eur(out["kwh"])
    out["co2_kg"] = _session_co2_kg(out["kwh"])

    if "power_w" in sess:
        out["p_max_w"]   = float(sess["power_w"].max())
        actively = sess.loc[sess["charging_state"] == "CHARGING", "power_w"]
        out["p_avg_w"]   = float(actively.mean()) if not actively.empty else 0.0
    else:
        out["p_max_w"] = out["p_avg_w"] = 0.0

    if "set_current_a" in sess and sess["set_current_a"].notna().any():
        set_a = pd.to_numeric(sess["set_current_a"], errors="coerce").dropna()
        if not set_a.empty:
            out["set_a_last"] = float(set_a.iloc[-1])
            out["set_a_min"] = float(set_a.min())
            out["set_a_max"] = float(set_a.max())
    if "set_a_last" not in out:
        out["set_a_last"] = out["set_a_min"] = out["set_a_max"] = None

    # Typ2-Stecker-Temp (am Auto)
    t2_cols = [c for c in ["temp_connector_l1", "temp_connector_l2",
                           "temp_connector_l3"]
               if c in sess.columns and sess[c].notna().any()]
    out["t_stecker_max"] = float(sess[t2_cols].max().max()) if t2_cols else None
    # Schuko-Adapter (an der Wand) - beim Schuko-Laden der entscheidende Sensor
    schuko_cols = [c for c in ["temp_domestic_plug", "temp_domestic_plug_1",
                               "temp_domestic_plug_2"]
                   if c in sess.columns and sess[c].notna().any()]
    out["t_schuko_max"] = float(sess[schuko_cols].max().max()) if schuko_cols else None
    out["t_housing_max"] = float(sess["temp_housing"].max()) \
        if "temp_housing" in sess and sess["temp_housing"].notna().any() else None
    out["t_max"] = _max_relevant_temp(sess)
    hot_temp = float(_cfg_get("thresholds.temperature_hot", 75.0))
    out["thermal_reserve_c"] = (
        max(0.0, hot_temp - out["t_max"]) if out["t_max"] is not None else None
    )
    if out["t_schuko_max"] is not None and out.get("set_a_last"):
        out["schuko_c_per_a"] = out["t_schuko_max"] / out["set_a_last"]
    else:
        out["schuko_c_per_a"] = None

    if not events.empty:
        out["n_derating"] = int((events["typ"] == "Derating (thermisch)").sum())
        out["n_recovery"] = int((events["typ"] == "Recovery (Temp gesunken)").sum())
    else:
        out["n_derating"] = out["n_recovery"] = 0
    return out


# ---- Plots Ladevorgang-Analyse -------------------------------------------

def fig_analysis_stacked(sess: pd.DataFrame, events: pd.DataFrame) -> dict | None:
    """3-Subplot-Figur: Leistung + Steckertemperaturen + Soll-Strom.
    Geteilte X-Achse, Derating-Bereiche werden als vrect markiert."""
    if sess.empty:
        return None
    x = _ts_to_list(sess.index)
    traces: list[dict] = []

    # Row 1: Power
    traces.append({
        "type": "scatter", "mode": "lines",
        "x": x, "y": _col(sess, "power_w"),
        "name": "Leistung (W)",
        "line": {"color": "#1f77b4", "width": 1.6},
        "fill": "tozeroy", "fillcolor": "rgba(31,119,180,0.10)",
        "hovertemplate": "%{y:.0f} W<extra>Leistung</extra>",
        "xaxis": "x", "yaxis": "y",
    })
    # Row 2: Temperaturen - alle relevanten Sensoren (inkl. Schuko, denn
    # der wird beim Schuko-Laden am heissesten und ist derating-relevant).
    # Sensoren mit zu wenigen Datenpunkten (<5) werden weggelassen, damit sie
    # die Autoscale der Temp-Achse nicht verfaelschen.
    MIN_POINTS = 5
    tcols = [("temp_connector_l1",    "Typ2 L1",      "#1f77b4"),
             ("temp_connector_l2",    "Typ2 L2",      "#2ca02c"),
             ("temp_connector_l3",    "Typ2 L3",      "#9467bd"),
             ("temp_domestic_plug_1", "Schuko L",     "#ff7f0e"),
             ("temp_domestic_plug_2", "Schuko N",     "#e45756"),
             ("temp_domestic_plug",   "Schuko",       "#ffbf7f"),
             ("temp_housing",         "Gehaeuse",     "#d62728")]
    for col, label, color in tcols:
        if col not in sess.columns:
            continue
        series = pd.to_numeric(sess[col], errors="coerce")
        valid = int(series.notna().sum())
        if valid < MIN_POINTS:
            continue
        y = _col(sess, col)
        if y is None:
            continue
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x, "y": y, "name": label,
            "line": {"color": color, "width": 1.3},
            "hovertemplate": "%{y:.1f} °C<extra>" + label + "</extra>",
            "xaxis": "x2", "yaxis": "y2",
        })
    # Row 3: Soll-Strom + Ist-Strom (Summe der Phasen als Orientierung)
    set_y = _col(sess, "set_current_a")
    if set_y:
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x, "y": set_y, "name": "I soll",
            "line": {"color": "#e45756", "width": 2.0, "shape": "hv"},
            "hovertemplate": "%{y:.1f} A<extra>I soll</extra>",
            "xaxis": "x3", "yaxis": "y3",
        })
    for col, label, color in [("current_l1_a", "I L1", "#1f77b4"),
                              ("current_l2_a", "I L2", "#2ca02c"),
                              ("current_l3_a", "I L3", "#9467bd")]:
        y = _col(sess, col)
        if y is None:
            continue
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x, "y": y, "name": label,
            "line": {"color": color, "width": 1.0, "dash": "dot"},
            "hovertemplate": "%{y:.2f} A<extra>" + label + "</extra>",
            "xaxis": "x3", "yaxis": "y3",
        })

    shapes = []
    annotations = []
    # Thermische Derating-Phasen als vertikale Baender: von Derating-Event
    # bis zum naechsten Recovery (oder Session-Ende)
    derating_times: list[pd.Timestamp] = []
    recovery_times: list[pd.Timestamp] = []
    if not events.empty and {"typ", "thermisch", "zeit"}.issubset(events.columns):
        thermals = events[events["thermisch"] == True]  # noqa: E712
        if not thermals.empty:
            derating_times = thermals.loc[thermals["typ"] == "Derating (thermisch)", "zeit"].tolist()
            recovery_times = thermals.loc[thermals["typ"] == "Recovery (Temp gesunken)", "zeit"].tolist()
    for dt in derating_times:
        # naechster Recovery-Zeitpunkt oder Session-Ende
        recs = [r for r in recovery_times if r > dt]
        end = recs[0] if recs else sess.index[-1]
        x0 = dt.isoformat(sep=" ", timespec="seconds")
        x1 = end.isoformat(sep=" ", timespec="seconds")
        for yref in ("paper",):  # ueber alle Subplots
            shapes.append({
                "type": "rect", "xref": "x", "yref": "paper",
                "x0": x0, "x1": x1, "y0": 0, "y1": 1,
                "fillcolor": "rgba(214,39,40,0.08)",
                "line": {"width": 0},
                "layer": "below",
            })
        annotations.append({
            "x": x0, "y": 1.02, "xref": "x", "yref": "paper",
            "text": "Derating",
            "showarrow": False,
            "font": {"color": "#d62728", "size": 10},
            "xanchor": "left",
        })

    layout = {
        "title": "Leistung - Temperatur - Soll-Strom (gleiche Zeitachse)",
        "height": 720,
        "margin": {"l": 60, "r": 20, "t": 60, "b": 50},
        "hovermode": "x unified",
        "template": "plotly_white",
        "legend": {"orientation": "h", "y": -0.08},
        "grid": {"rows": 3, "columns": 1, "pattern": "independent"},
        "xaxis":  {"domain": [0, 1], "anchor": "y",  "matches": "x3", "showticklabels": False},
        "xaxis2": {"domain": [0, 1], "anchor": "y2", "matches": "x3", "showticklabels": False},
        "xaxis3": {"domain": [0, 1], "anchor": "y3", "title": "Zeit", "type": "date"},
        "yaxis":  {"domain": [0.68, 1.0],  "title": "Leistung (W)"},
        "yaxis2": {"domain": [0.36, 0.64], "title": "Temperatur (°C)"},
        "yaxis3": {"domain": [0.00, 0.32], "title": "Strom (A)"},
        "shapes": shapes,
        "annotations": annotations,
    }
    return {"data": traces, "layout": layout}


def _filter_analysis_by_amp(sess: pd.DataFrame, amp: int | None) -> pd.DataFrame:
    if sess.empty or amp is None:
        return sess
    d = sess.copy()
    if "set_current_a" in d.columns and d["set_current_a"].notna().any():
        i_bin = pd.to_numeric(d["set_current_a"], errors="coerce").round()
    else:
        phase_cols = [c for c in ["current_l1_a", "current_l2_a", "current_l3_a"] if c in d.columns]
        if not phase_cols:
            return d.iloc[0:0].copy()
        i_bin = d[phase_cols].max(axis=1).round()
    return d[i_bin == int(amp)].copy()


def fig_analysis_scatter_p_vs_t(sess: pd.DataFrame, amp: int | None = None) -> dict | None:
    if sess.empty or "power_w" not in sess.columns:
        return None
    charging = _filter_analysis_by_amp(sess[sess["charging_state"] == "CHARGING"].copy(), amp)
    if charging.empty:
        return None
    # Waermste Stelle: Typ2, Schuko, Gehaeuse - was immer am heissesten ist,
    # bestimmt realistisch die Drosselung.
    temp_cols = [c for c in ["temp_connector_l1", "temp_connector_l2",
                             "temp_connector_l3",
                             "temp_domestic_plug", "temp_domestic_plug_1",
                             "temp_domestic_plug_2",
                             "temp_housing"]
                 if c in charging.columns and charging[c].notna().any()]
    if not temp_cols:
        return None
    t_max = charging[temp_cols].max(axis=1)
    p = charging["power_w"]
    mask = t_max.notna() & p.notna()
    if mask.sum() < 5:
        return None
    trace = {
        "type": "scattergl", "mode": "markers",
        "x": [float(v) for v in t_max[mask].tolist()],
        "y": [float(v) for v in p[mask].tolist()],
        "marker": {
            "size": 6, "opacity": 0.6,
            "color": [(t - t_max.min()) / max(1.0, (t_max.max() - t_max.min())) for t in t_max[mask].tolist()],
            "colorscale": "Turbo",
            "showscale": False,
        },
        "hovertemplate": "max Temp: %{x:.1f} °C<br>Leistung: %{y:.0f} W<extra></extra>",
        "name": "Sample",
    }
    layout = {
        "title": ("Leistung vs. waermster Sensor"
                  + (f" ({amp} A)" if amp is not None else " (nur CHARGING-Samples)")),
        "height": 380,
        "margin": {"l": 60, "r": 20, "t": 50, "b": 60},
        "xaxis": {"title": "Temperatur des waermsten Sensors (°C)"},
        "yaxis": {"title": "Leistung (W)"},
        "template": "plotly_white",
    }
    return {"data": [trace], "layout": layout}


def fig_analysis_socket_scatter_p_vs_t(sess: pd.DataFrame, amp: int | None = None) -> dict | None:
    if sess.empty or "power_w" not in sess.columns:
        return None
    charging = _filter_analysis_by_amp(sess[sess["charging_state"] == "CHARGING"].copy(), amp)
    if charging.empty:
        return None
    socket_cols = [c for c in ["temp_domestic_plug", "temp_domestic_plug_1", "temp_domestic_plug_2"]
                   if c in charging.columns and charging[c].notna().any()]
    if not socket_cols:
        return None
    t_socket = charging[socket_cols].max(axis=1)
    p = charging["power_w"]
    mask = t_socket.notna() & p.notna()
    if mask.sum() < 5:
        return None
    trace = {
        "type": "scattergl", "mode": "markers",
        "x": [float(v) for v in t_socket[mask].tolist()],
        "y": [float(v) for v in p[mask].tolist()],
        "marker": {
            "size": 6, "opacity": 0.6,
            "color": [(t - t_socket.min()) / max(1.0, (t_socket.max() - t_socket.min())) for t in t_socket[mask].tolist()],
            "colorscale": "Turbo",
            "showscale": False,
        },
        "hovertemplate": "Steckdose: %{x:.1f} °C<br>Leistung: %{y:.0f} W<extra></extra>",
        "name": "Sample",
    }
    layout = {
        "title": ("Leistung vs. Steckdose / Schuko"
                  + (f" ({amp} A)" if amp is not None else "")),
        "height": 380,
        "margin": {"l": 60, "r": 20, "t": 50, "b": 60},
        "xaxis": {"title": "Temperatur Steckdose / Schuko (°C)"},
        "yaxis": {"title": "Leistung (W)"},
        "template": "plotly_white",
    }
    return {"data": [trace], "layout": layout}


def fig_analysis_power_histogram(sess: pd.DataFrame) -> dict | None:
    if sess.empty or "power_w" not in sess.columns:
        return None
    p = sess.loc[sess["charging_state"] == "CHARGING", "power_w"].dropna()
    if len(p) < 5:
        return None
    trace = {
        "type": "histogram",
        "x": [float(v) for v in p.tolist()],
        "xbins": {"size": 100},
        "marker": {"color": "#2ca02c", "line": {"color": "#1b5e20", "width": 1}},
        "hovertemplate": "%{x} W<br>%{y} Samples<extra></extra>",
        "name": "Leistungen",
    }
    layout = {
        "title": "Haeufigkeitsverteilung der Ladeleistung (Minuten-Samples)",
        "height": 320,
        "margin": {"l": 60, "r": 20, "t": 50, "b": 60},
        "xaxis": {"title": "Leistung (W)"},
        "yaxis": {"title": "Anzahl Samples"},
        "bargap": 0.05,
        "template": "plotly_white",
    }
    return {"data": [trace], "layout": layout}


def fig_analysis_progress(sess: pd.DataFrame) -> dict | None:
    """kWh-Fortschritt + kumulierte aktive Ladezeit."""
    if sess.empty:
        return None
    if "energy_total_wh" in sess and sess["energy_total_wh"].notna().any():
        e = sess["energy_total_wh"].ffill()
        kwh = (e - e.iloc[0]).clip(lower=0) / 1000.0
    elif "energy_session_wh" in sess:
        kwh = sess["energy_session_wh"].astype(float) / 1000.0
    else:
        return None

    if "charging_state" in sess:
        # kumulierte aktive Ladezeit in Minuten (Minuten-Samples)
        ch = (sess["charging_state"] == "CHARGING").astype(int)
        # Differenz zwischen aufeinanderfolgenden Timestamps, nur wo geladen wurde
        dt = sess.index.to_series().diff().dt.total_seconds().fillna(60) / 60.0
        active_min = (ch * dt).cumsum()
    else:
        active_min = None

    x = _ts_to_list(sess.index)
    traces = [{
        "type": "scatter", "mode": "lines",
        "x": x, "y": [float(v) for v in kwh.tolist()],
        "name": "Geladene kWh",
        "line": {"color": "#2ca02c", "width": 2.2},
        "fill": "tozeroy", "fillcolor": "rgba(44,160,44,0.12)",
        "hovertemplate": "%{y:.3f} kWh<extra>geladen</extra>",
        "yaxis": "y1",
    }]
    if active_min is not None:
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x, "y": [float(v) for v in active_min.tolist()],
            "name": "aktive Ladezeit (min)",
            "line": {"color": "#ff7f0e", "width": 1.6, "dash": "dot"},
            "hovertemplate": "%{y:.1f} min<extra>aktiv</extra>",
            "yaxis": "y2",
        })
    layout = {
        "title": "Lade-Fortschritt",
        "height": 340,
        "margin": {"l": 60, "r": 60, "t": 50, "b": 50},
        "xaxis": {"title": "Zeit", "type": "date"},
        "yaxis": {"title": "kWh", "side": "left"},
        "yaxis2": {"title": "aktiv (min)", "overlaying": "y", "side": "right",
                   "showgrid": False},
        "template": "plotly_white",
        "hovermode": "x unified",
        "legend": {"orientation": "h", "y": -0.2},
    }
    return {"data": traces, "layout": layout}


def events_table_html(events: pd.DataFrame) -> str:
    if events.empty:
        return "<p><i>Keine nennenswerten Aenderungen des Soll-Stroms erkannt.</i></p>"
    df = events.copy()
    df["zeit"] = df["zeit"].dt.strftime("%H:%M:%S")
    df["delta_a"]  = df["delta_a"].map(lambda v: f"{v:+.1f}")
    df["set_a_vor"]  = df["set_a_vor"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    df["set_a_nach"] = df["set_a_nach"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    df["max_temp_c"] = df["max_temp_c"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    return df.to_html(
        index=False,
        columns=["zeit", "typ", "delta_a", "set_a_vor", "set_a_nach", "max_temp_c"],
        classes="sessions",
        border=0,
        escape=False,
    )


def session_label(start: pd.Timestamp, end: pd.Timestamp, n_samples: int) -> str:
    now = pd.Timestamp.now(tz=_report_tzinfo())
    start_cmp = start if start.tzinfo is not None else start.tz_localize(_report_tzinfo())
    end_cmp   = end if end.tzinfo is not None else end.tz_localize(_report_tzinfo())
    dur_min = (end_cmp - start_cmp).total_seconds() / 60.0
    same_day = start_cmp.normalize() == now.normalize()
    # 10 min Toleranz - passt zu 6 min Polling-Intervall
    if same_day and (now - end_cmp).total_seconds() < 600:
        return f"Aktuell (seit {start_cmp.strftime('%H:%M')}, {dur_min:.0f} min)"
    if same_day:
        return f"Heute {start_cmp.strftime('%H:%M')}-{end_cmp.strftime('%H:%M')} ({dur_min:.0f} min)"
    return f"{start_cmp.strftime('%a %d.%m. %H:%M')} ({dur_min:.0f} min)"


def build_analysis_section(df: pd.DataFrame, plots_out: dict) -> str:
    """Erzeugt die Ladevorgang-Analyse. Pro Session werden alle Plots und
    Tabellen erzeugt; per JS-Dropdown wird nur einer sichtbar gemacht."""
    blocks = _connect_blocks(df)
    if not blocks:
        return ('<section class="panel" id="panel-analysis">'
                '<h2>Ladevorgang-Analyse</h2>'
                '<p><i>Keine Sessions mit <code>vehicle_connect_time</code> '
                'in der DB gefunden. Sobald das Auto angesteckt war, werden '
                'hier die Ladevorgaenge im Detail analysiert.</i></p>'
                '</section>')

    # juengste zuerst
    blocks = list(reversed(blocks))

    agg_rows: list[dict] = []
    panels: list[str] = []
    options: list[str] = []

    for idx, (start, end, sub) in enumerate(blocks):
        events = detect_derating_events(sub)
        agg = session_aggregates(sub, events)

        label_start = _session_connect_start(sub) or start
        agg["_label"] = session_label(label_start, end, len(sub))
        agg["_label_start"] = label_start
        agg_rows.append(agg)

        sid = f"an-{idx}"
        selected = " selected" if idx == 0 else ""
        options.append(f'<option value="{sid}"{selected}>{agg["_label"]}</option>')

        # Plots registrieren (eigene IDs je Session)
        fig_stack   = fig_analysis_stacked(sub, events)
        fig_prog    = fig_analysis_progress(sub)
        fig_scatter = fig_analysis_scatter_p_vs_t(sub)
        fig_socket  = fig_analysis_socket_scatter_p_vs_t(sub)
        fig_hist    = fig_analysis_power_histogram(sub)
        amp_bins = sorted(int(v) for v in pd.to_numeric(sub.get("set_current_a"), errors="coerce").dropna().round().astype(int).unique().tolist()) if "set_current_a" in sub.columns and sub["set_current_a"].notna().any() else []

        plot_blocks: list[str] = []
        if fig_stack:
            pid = f"plot-{sid}-stack"
            plots_out[pid] = fig_stack
            plot_blocks.append(_plot_div(pid))
        if fig_prog:
            pid = f"plot-{sid}-progress"
            plots_out[pid] = fig_prog
            plot_blocks.append(_plot_div(pid))
        scatter_views: list[str] = []
        if fig_scatter:
            pid = f"plot-{sid}-scatter"
            plots_out[pid] = fig_scatter
            scatter_views.append(f'<div class="analysis-scatter-view" id="{sid}-scatter-warmest-all">' + _plot_div(pid) + '</div>')
        if fig_socket:
            pid = f"plot-{sid}-socket"
            plots_out[pid] = fig_socket
            scatter_views.append(f'<div class="analysis-scatter-view hidden" id="{sid}-scatter-socket-all">' + _plot_div(pid) + '</div>')
        for amp in amp_bins:
            fig_scatter_amp = fig_analysis_scatter_p_vs_t(sub, amp)
            fig_socket_amp = fig_analysis_socket_scatter_p_vs_t(sub, amp)
            if fig_scatter_amp:
                pid = f"plot-{sid}-scatter-{amp}a"
                plots_out[pid] = fig_scatter_amp
                scatter_views.append(f'<div class="analysis-scatter-view hidden" id="{sid}-scatter-warmest-{amp}a">' + _plot_div(pid) + '</div>')
            if fig_socket_amp:
                pid = f"plot-{sid}-socket-{amp}a"
                plots_out[pid] = fig_socket_amp
                scatter_views.append(f'<div class="analysis-scatter-view hidden" id="{sid}-scatter-socket-{amp}a">' + _plot_div(pid) + '</div>')
        if scatter_views:
            scatter_select = (
                '<div class="session-selector">'
                f'<label for="{sid}-scatter-type">Grafik: </label>'
                f'<select id="{sid}-scatter-type" onchange="selectAnalysisScatter(\'{sid}\')">'
                '<option value="warmest" selected>waermster Sensor</option>'
                '<option value="socket">Steckdose / Schuko</option>'
                '</select>'
                f'<label for="{sid}-scatter-amp">Strom: </label>'
                f'<select id="{sid}-scatter-amp" onchange="selectAnalysisScatter(\'{sid}\')">'
                '<option value="all" selected>alle Punkte</option>'
                + ''.join(f'<option value="{amp}a">{amp} A</option>' for amp in amp_bins)
                + '</select>'
                '</div>'
            )
            plot_blocks.append(scatter_select + ''.join(scatter_views))
        if fig_hist:
            pid = f"plot-{sid}-hist"
            plots_out[pid] = fig_hist
            plot_blocks.append(_plot_div(pid))

        # Kennzahlen pro Session als Kacheln
        kpis = []
        if agg.get("kwh") is not None:
            kpis.append((f"{agg['kwh']:.2f} kWh", "geladen"))
        kpis.append((_fmt_duration(agg.get("angesteckt_min", 0) * 60), "angesteckt"))
        kpis.append((_fmt_duration(agg.get("aktiv_min", 0) * 60), "aktiv geladen"))
        kpis.append((_fmt_duration(agg.get("standby_min", 0) * 60), "Standzeit"))
        if agg.get("p_eff_plugged_w"):
            kpis.append((f"{agg['p_eff_plugged_w']/1000.0:.2f} kW", "effektiv angesteckt"))
        if agg.get("p_eff_active_w"):
            kpis.append((f"{agg['p_eff_active_w']/1000.0:.2f} kW", "effektiv aktiv"))
        if agg.get("p_max_w"):
            kpis.append((f"{agg['p_max_w']:.0f} W", "Spitzenleistung"))
        if agg.get("p_avg_w"):
            kpis.append((f"{agg['p_avg_w']:.0f} W", "Ø Leistung"))
        if agg.get("set_a_last") is not None:
            if agg.get("set_a_min") is not None and agg.get("set_a_max") is not None and agg["set_a_min"] != agg["set_a_max"]:
                kpis.append((f"{agg['set_a_last']:.0f} A ({agg['set_a_min']:.0f}-{agg['set_a_max']:.0f} A)", "eingestellter Strom"))
            else:
                kpis.append((f"{agg['set_a_last']:.0f} A", "eingestellter Strom"))
        if agg.get("t_stecker_max") is not None:
            kpis.append((f"{agg['t_stecker_max']:.1f} °C", "max. Typ2-Stecker (Auto)"))
        if agg.get("t_schuko_max") is not None:
            kpis.append((f"{agg['t_schuko_max']:.1f} °C", "max. Schuko-Adapter (Wand)"))
        if agg.get("t_housing_max") is not None:
            kpis.append((f"{agg['t_housing_max']:.1f} °C", "max. Gehaeuse"))
        if agg.get("thermal_reserve_c") is not None:
            kpis.append((f"{agg['thermal_reserve_c']:.1f} °C", "thermische Reserve"))
        if agg.get("schuko_c_per_a") is not None:
            kpis.append((f"{agg['schuko_c_per_a']:.2f} °C/A", "Schuko pro Ampere"))
        if agg.get("cost_eur") is not None:
            kpis.append((f"{agg['cost_eur']:.2f} €", "Kosten geschaetzt"))
        if agg.get("co2_kg") is not None:
            kpis.append((f"{agg['co2_kg']:.1f} kg", "CO2 geschaetzt"))
        if agg.get("n_derating"):
            kpis.append((str(agg["n_derating"]), "therm. Derating-Events"))
        if agg.get("n_recovery"):
            kpis.append((str(agg["n_recovery"]), "Recovery-Events"))

        kpi_html_ = "".join(
            f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div></div>'
            for value, label in kpis
        )
        label_start = agg.get("_label_start", start)
        head = (f'<p class="sub">Session: <b>{agg["_label"]}</b> &middot; '
                f'{label_start.strftime("%Y-%m-%d %H:%M")} - {end.strftime("%H:%M")} '
                f'&middot; {len(sub)} Messpunkte</p>')
        hide = "" if idx == 0 else " hidden"
        events_tbl_header = ("<h3>Aenderungs-Events</h3>"
                             '<p class="hint">Rot unterlegte Phasen im oberen Plot = '
                             'thermisches Derating (automatisch erkannt: Reduktion des '
                             'Soll-Stroms + Temperatur im oberen Quartil).</p>')

        panels.append(
            f'<div class="analysis-session{hide}" id="{sid}">'
            f'{head}<div class="kpis">{kpi_html_}</div>'
            f'{"".join(plot_blocks)}'
            f'{events_tbl_header}{events_table_html(events)}'
            f'</div>'
        )

    # Aggregat-Tabelle ueber alle Sessions
    agg_df = pd.DataFrame(agg_rows).copy()
    start_col = agg_df["_label_start"] if "_label_start" in agg_df.columns else agg_df["start"]
    agg_df["start"]  = pd.to_datetime(start_col).dt.strftime("%Y-%m-%d %H:%M")
    agg_df["dauer"]  = agg_df["angesteckt_min"].map(lambda m: _fmt_duration(m * 60))
    agg_df["aktiv"]  = agg_df["aktiv_min"].map(lambda m: _fmt_duration(m * 60))
    agg_df["standby"] = agg_df["standby_min"].map(lambda m: _fmt_duration(m * 60))
    agg_df["kwh"]    = agg_df["kwh"].map(lambda v: f"{v:.2f}")
    agg_df["p_eff"] = agg_df["p_eff_plugged_w"].map(lambda v: f"{v:.0f}")
    agg_df["p_max"]  = agg_df["p_max_w"].map(lambda v: f"{v:.0f}")
    agg_df["p_avg"]  = agg_df["p_avg_w"].map(lambda v: f"{v:.0f}")
    agg_df["set_a"]  = agg_df["set_a_last"].map(lambda v: "-" if pd.isna(v) else f"{v:.0f} A")
    agg_df["t_t2"]   = agg_df["t_stecker_max"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    agg_df["t_sch"]  = agg_df.get("t_schuko_max", pd.Series([None]*len(agg_df))).map(
        lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    agg_df["t_hou"]  = agg_df["t_housing_max"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    agg_df["reserve"] = agg_df["thermal_reserve_c"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    agg_df["cost"] = agg_df["cost_eur"].map(lambda v: "-" if pd.isna(v) else f"{v:.2f}")
    agg_df["co2"] = agg_df["co2_kg"].map(lambda v: "-" if pd.isna(v) else f"{v:.1f}")
    agg_df["n_der"]  = agg_df["n_derating"]
    agg_df["n_rec"]  = agg_df["n_recovery"]
    agg_columns = [
        "Start", "angesteckt", "aktiv", "Standzeit", "kWh", "eff. W", "max W", "Ø W", "Soll A",
        "Typ2 °C", "Schuko °C", "Gehaeuse °C", "Reserve °C",
    ]
    if agg_df["cost_eur"].notna().any():
        agg_columns.append("Kosten €")
    if agg_df["co2_kg"].notna().any():
        agg_columns.append("CO2 kg")
    agg_columns.extend(["Derating", "Recovery"])
    agg_table = agg_df.rename(columns={
        "start": "Start", "dauer": "angesteckt", "aktiv": "aktiv", "standby": "Standzeit",
        "kwh": "kWh", "p_eff": "eff. W", "p_max": "max W", "p_avg": "Ø W", "set_a": "Soll A",
        "t_t2":  "Typ2 °C", "t_sch": "Schuko °C", "t_hou": "Gehaeuse °C",
        "reserve": "Reserve °C", "cost": "Kosten €", "co2": "CO2 kg",
        "n_der": "Derating", "n_rec": "Recovery",
    }).to_html(
        index=False,
        columns=agg_columns,
        classes="sessions", border=0, escape=False,
    )

    select_html = (
        '<div class="session-selector">'
        '<label for="analysis-select">Ladevorgang: </label>'
        f'<select id="analysis-select" onchange="selectAnalysisSession(this.value)">'
        f'{"".join(options)}</select>'
        '</div>'
    )

    return (
        '<section class="panel" id="panel-analysis">'
        '<h2>Ladevorgang-Analyse</h2>'
        '<p class="hint">Waehle links einen Ladevorgang - alle Grafiken und '
        'Events werden dann fuer diese Session angezeigt. '
        'Die Drosselungserkennung arbeitet adaptiv und orientiert sich am '
        'oberen Quartil der Temperaturen <i>innerhalb</i> der jeweiligen Session.</p>'
        f'{select_html}{"".join(panels)}'
        f'<h3>Uebersicht</h3>{agg_table}'
        '</section>'
    )


# ---------------------------------------------------------------------------
# Ereignisse (Errors / Warnings)
# ---------------------------------------------------------------------------

NORMAL_CODES = {"NO_ERROR", "NO_WARNING", None, ""}


def _episodes_from_series(s: pd.Series) -> list[dict]:
    """Fasst aufeinanderfolgende gleiche Codes zu Episoden zusammen.
    Erwartet eine Series mit DatetimeIndex und String-Werten."""
    if s.empty:
        return []
    s = s.astype(object).fillna("").astype(str)
    # Gruppenwechsel erkennen
    grp = (s != s.shift()).cumsum()
    episodes: list[dict] = []
    for _gid, block in s.groupby(grp):
        code = block.iloc[0]
        start = block.index[0]
        end   = block.index[-1]
        episodes.append({
            "code":  code,
            "start": start,
            "ende":  end,
            "samples": int(len(block)),
            "dauer_min": (end - start).total_seconds() / 60.0,
        })
    return episodes


def build_events_panel(df: pd.DataFrame, plots_out: dict) -> str:
    if df.empty:
        return ('<section class="panel" id="panel-events">'
                '<h2>Ereignisse</h2>'
                '<p><i>Keine Daten im gewaehlten Zeitraum.</i></p>'
                '</section>')

    # ---- Errors & Warnings ---------------------------------------------
    err_series = df["error_code"]   if "error_code"   in df.columns else pd.Series(dtype=object)
    warn_series = df["warning_code"] if "warning_code" in df.columns else pd.Series(dtype=object)

    err_eps  = _episodes_from_series(err_series)
    warn_eps = _episodes_from_series(warn_series)

    def _agg(eps: list[dict], kind: str) -> pd.DataFrame:
        rows = [e for e in eps if e["code"] not in NORMAL_CODES]
        if not rows:
            return pd.DataFrame()
        agg: dict[str, dict] = {}
        for ep in rows:
            d = agg.setdefault(ep["code"], {
                "code": ep["code"], "count": 0, "first": ep["start"],
                "last": ep["ende"], "total_min": 0.0, "episodes": 0,
            })
            d["count"] += ep["samples"]
            d["episodes"] += 1
            if ep["start"] < d["first"]:
                d["first"] = ep["start"]
            if ep["ende"] > d["last"]:
                d["last"] = ep["ende"]
            d["total_min"] += ep["dauer_min"]
        out = pd.DataFrame(agg.values())
        out["beschreibung"] = out["code"].apply(lambda c: _decode_code(c, kind))
        return out.sort_values("count", ascending=False)

    err_df  = _agg(err_eps, "error")
    warn_df = _agg(warn_eps, "warning")

    def _fmt_ts(t: pd.Timestamp) -> str:
        try:
            return t.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(t)

    def _render_table(df: pd.DataFrame, kind: str) -> str:
        if df.empty:
            return ('<p><i>Keine '
                    + ('Fehler' if kind == 'error' else 'Warnungen')
                    + ' im Zeitraum aufgetreten.</i></p>')
        rows = []
        sev_class = "sev-error" if kind == "error" else "sev-warn"
        for _, r in df.iterrows():
            rows.append(
                f'<tr>'
                f'<td class="code {sev_class}">{r["code"]}</td>'
                f'<td>{r["beschreibung"]}</td>'
                f'<td>{int(r["episodes"])}</td>'
                f'<td>{int(r["count"])}</td>'
                f'<td>{_fmt_duration(r["total_min"] * 60)}</td>'
                f'<td>{_fmt_ts(r["first"])}</td>'
                f'<td>{_fmt_ts(r["last"])}</td>'
                f'</tr>'
            )
        return (
            '<table class="events"><thead><tr>'
            '<th>Code</th><th>Bedeutung</th>'
            '<th>Episoden</th><th>Samples</th>'
            '<th>Gesamt-Dauer</th>'
            '<th>zuerst</th><th>zuletzt</th>'
            '</tr></thead><tbody>' + "".join(rows) + '</tbody></table>'
        )

    err_table  = _render_table(err_df,  "error")
    warn_table = _render_table(warn_df, "warning")

    # ---- Timeline: nur nicht-normale Codes --------------------------
    def _build_timeline(eps: list[dict], kind: str) -> dict | None:
        non_normal = [e for e in eps if e["code"] not in NORMAL_CODES]
        if not non_normal:
            return None
        uniq_codes: list[str] = []
        for e in non_normal:
            if e["code"] not in uniq_codes:
                uniq_codes.append(e["code"])
        # Plotly: pro Code eine "Serie" mit einzelnen Linien-Segmenten
        palette = ["#d62728", "#ff7f0e", "#bcbd22", "#9467bd",
                   "#17becf", "#e377c2", "#8c564b"]
        traces = []
        for i, code in enumerate(uniq_codes):
            color = palette[i % len(palette)]
            xs: list = []
            ys: list = []
            hover: list = []
            for e in non_normal:
                if e["code"] != code:
                    continue
                xs += [e["start"].isoformat(sep=" ", timespec="seconds"),
                       e["ende"].isoformat(sep=" ", timespec="seconds"),
                       None]
                ys += [code, code, None]
                hover += [
                    f"{code}<br>{_decode_code(code, kind)}<br>"
                    f"{_fmt_ts(e['start'])} - {_fmt_ts(e['ende'])}<br>"
                    f"Dauer: {_fmt_duration(e['dauer_min']*60)}"
                ] * 2 + [""]
            traces.append({
                "type": "scatter", "mode": "lines",
                "x": xs, "y": ys, "name": code,
                "line": {"color": color, "width": 16},
                "hovertext": hover,
                "hoverinfo": "text",
                "connectgaps": False,
            })
        layout = {
            "title": ("Error-Timeline" if kind == "error" else "Warning-Timeline"),
            "height": 90 + 40 * len(uniq_codes),
            "margin": {"l": 240, "r": 20, "t": 45, "b": 40},
            "xaxis": {"type": "date", "title": "Zeit"},
            "yaxis": {"type": "category", "automargin": True},
            "template": "plotly_white",
            "showlegend": False,
        }
        return {"data": traces, "layout": layout}

    err_tl  = _build_timeline(err_eps,  "error")
    warn_tl = _build_timeline(warn_eps, "warning")
    if err_tl:
        plots_out["plot-events-errors"] = err_tl
    if warn_tl:
        plots_out["plot-events-warnings"] = warn_tl

    # ---- aktueller Zustand ------------------------------------------
    last = df.iloc[-1]
    last_err  = str(last.get("error_code",   "") or "")
    last_warn = str(last.get("warning_code", "") or "")
    status_bits: list[str] = []
    if last_err and last_err not in NORMAL_CODES:
        status_bits.append(
            f'<div class="kpi"><div class="v sev-error">{last_err}</div>'
            f'<div class="l">aktueller Fehler ({_decode_code(last_err, "error")})</div></div>'
        )
    else:
        status_bits.append(
            '<div class="kpi"><div class="v sev-ok">OK</div>'
            '<div class="l">kein aktiver Fehler</div></div>'
        )
    if last_warn and last_warn not in NORMAL_CODES:
        status_bits.append(
            f'<div class="kpi"><div class="v sev-warn">{last_warn}</div>'
            f'<div class="l">aktuelle Warnung ({_decode_code(last_warn, "warning")})</div></div>'
        )
    else:
        status_bits.append(
            '<div class="kpi"><div class="v sev-ok">OK</div>'
            '<div class="l">keine aktive Warnung</div></div>'
        )
    status_block = '<div class="kpis">' + "".join(status_bits) + '</div>'

    tl_html_err  = (_plot_div("plot-events-errors")
                    if err_tl else "")
    tl_html_warn = (_plot_div("plot-events-warnings")
                    if warn_tl else "")

    return (
        '<section class="panel" id="panel-events">'
        '<h2>Ereignisse (Fehler &amp; Warnungen)</h2>'
        '<p class="hint">Quelle: Spalten <code>error_code</code> / '
        '<code>warning_code</code> der samples-Tabelle. '
        'Angezeigt werden nur Codes, die im gewaehlten Zeitraum '
        'tatsaechlich aufgetreten sind.</p>'
        + status_block +
        '<h3>Fehler</h3>' + err_table + (tl_html_err if err_tl else "") +
        '<h3>Warnungen</h3>' + warn_table + (tl_html_warn if warn_tl else "") +
        '</section>'
    )


# ---------------------------------------------------------------------------
# Info-Panel (Geraete- und Systeminfos)
# ---------------------------------------------------------------------------

def _load_device_info() -> dict | None:
    """Holt den juengsten device_info-Eintrag inkl. raw_info_json."""
    if not DB_FILE.exists():
        return None
    try:
        with sqlite3.connect(str(DB_FILE)) as conn:
            row = conn.execute(
                "SELECT ts_utc, serial_number, device_name, model_type, "
                "sw_version, hw_version, raw_info_json "
                "FROM device_info ORDER BY ts_utc DESC LIMIT 1"
            ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    raw = {}
    if row[6]:
        try:
            raw = json.loads(row[6])
        except Exception:
            pass
    return {
        "ts_utc": row[0],
        "serial_number": row[1],
        "device_name": row[2],
        "model_type": row[3],
        "sw_version": row[4],
        "hw_version": row[5],
        "raw": raw,
    }


def _db_stats() -> dict:
    """Ein paar Eckdaten der DB fuer den Info-Tab."""
    out: dict = {"db_file": str(DB_FILE)}
    if not DB_FILE.exists():
        return out
    try:
        out["db_size_mb"] = DB_FILE.stat().st_size / (1024 * 1024)
    except Exception:
        pass
    try:
        with sqlite3.connect(str(DB_FILE)) as conn:
            out["samples"] = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
            r = conn.execute(
                "SELECT MIN(ts_local), MAX(ts_local) FROM samples"
            ).fetchone()
            out["first_sample"] = r[0]
            out["last_sample"]  = r[1]
            # samples_kv ist optional
            try:
                out["kv_rows"]  = conn.execute("SELECT COUNT(*) FROM samples_kv").fetchone()[0]
                out["kv_paths"] = conn.execute("SELECT COUNT(DISTINCT path) FROM samples_kv").fetchone()[0]
            except sqlite3.OperationalError:
                pass
            try:
                out["n_enums"] = conn.execute("SELECT COUNT(*) FROM code_enums").fetchone()[0]
            except sqlite3.OperationalError:
                pass
    except Exception:
        pass
    return out


def _load_enums_by_kind() -> dict[str, list[tuple[str, str, str]]]:
    """Liefert die komplette code_enums-Tabelle, gruppiert nach kind."""
    out: dict[str, list[tuple[str, str, str]]] = {}
    if not DB_FILE.exists():
        return out
    try:
        with sqlite3.connect(str(DB_FILE)) as conn:
            rows = conn.execute(
                "SELECT kind, code, description, severity FROM code_enums "
                "ORDER BY kind, code"
            ).fetchall()
    except sqlite3.OperationalError:
        return out
    for kind, code, desc, sev in rows:
        out.setdefault(kind, []).append((code, desc or "", sev or "info"))
    return out


def _info_table(title: str, rows: list[tuple[str, str]]) -> str:
    """HTML-Tabelle mit Label/Value-Paaren."""
    rows = [(k, v) for k, v in rows if v not in (None, "")]
    if not rows:
        return ""
    body = "".join(
        f'<tr><th>{k}</th><td>{v}</td></tr>' for k, v in rows
    )
    return (f'<div class="info-card">'
            f'<h3>{title}</h3>'
            f'<table class="info-table">{body}</table>'
            f'</div>')


def _nested_get(node: dict, *path: str):
    cur = node
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _first_present(node: dict, paths: list[tuple[str, ...]]):
    for path in paths:
        val = _nested_get(node, *path)
        if val not in (None, ""):
            return val
    return None


def _gps_info(raw: dict) -> dict:
    lat = _first_present(raw, [
        ("gps", "latitude"),
        ("gps", "lat"),
        ("location", "latitude"),
        ("location", "lat"),
        ("network", "gps", "latitude"),
        ("network", "gps", "lat"),
    ])
    lon = _first_present(raw, [
        ("gps", "longitude"),
        ("gps", "lon"),
        ("gps", "lng"),
        ("location", "longitude"),
        ("location", "lon"),
        ("location", "lng"),
        ("network", "gps", "longitude"),
        ("network", "gps", "lon"),
        ("network", "gps", "lng"),
    ])
    if lat is None or lon is None:
        return {}
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return {}
    return {
        "lat": lat_f,
        "lon": lon_f,
        "fix": _first_present(raw, [
            ("gps", "fix"),
            ("gps", "fix_type"),
            ("location", "fix"),
            ("network", "gps", "fix"),
        ]),
        "accuracy": _first_present(raw, [
            ("gps", "accuracy"),
            ("gps", "accuracy_m"),
            ("location", "accuracy"),
            ("location", "accuracy_m"),
        ]),
    }


def _cellular_info(raw: dict) -> dict:
    return {
        "operator": _first_present(raw, [
            ("cellular", "operator"),
            ("cellular", "provider"),
            ("mobile", "operator"),
            ("modem", "operator"),
            ("network", "cellular", "operator"),
        ]),
        "technology": _first_present(raw, [
            ("cellular", "technology"),
            ("cellular", "network_type"),
            ("mobile", "technology"),
            ("modem", "technology"),
            ("network", "cellular", "technology"),
        ]),
        "signal": _first_present(raw, [
            ("cellular", "signal"),
            ("cellular", "rssi"),
            ("mobile", "signal"),
            ("modem", "rssi"),
            ("network", "cellular", "signal"),
            ("network", "cellular", "rssi"),
        ]),
        "imei": _first_present(raw, [
            ("cellular", "imei"),
            ("mobile", "imei"),
            ("modem", "imei"),
            ("network", "cellular", "imei"),
        ]),
        "iccid": _first_present(raw, [
            ("cellular", "iccid"),
            ("mobile", "iccid"),
            ("modem", "iccid"),
            ("network", "cellular", "iccid"),
        ]),
    }


def _code_table_html(kind: str, rows: list[tuple[str, str, str]]) -> str:
    sev_class_map = {"ok": "sev-ok", "warn": "sev-warn", "error": "sev-error"}
    tr_rows = []
    for code, desc, sev in rows:
        cls = sev_class_map.get(sev, "")
        tr_rows.append(
            f'<tr><td class="code {cls}">{code}</td>'
            f'<td>{desc}</td><td>{sev}</td></tr>'
        )
    return (
        '<table class="events">'
        '<thead><tr><th>Code</th><th>Bedeutung</th><th>Schwere</th></tr></thead>'
        f'<tbody>{"".join(tr_rows)}</tbody></table>'
    )


def build_info_panel() -> str:
    dev = _load_device_info()
    stats = _db_stats()
    enums = _load_enums_by_kind()

    cards: list[str] = []
    if dev:
        raw = dev.get("raw") or {}
        gen = raw.get("general", {})
        con = raw.get("connector", {})
        grid = raw.get("grid", {})
        net = raw.get("network", {})
        ver = raw.get("versions", {})
        gps = _gps_info(raw)
        cellular = _cellular_info(raw)

        cards.append(_info_table("Geraet", [
            ("Name",          dev.get("device_name") or gen.get("device_name")),
            ("Modell",        dev.get("model_type")  or gen.get("model_type")),
            ("Seriennummer",  dev.get("serial_number") or gen.get("serial_number")),
            ("Nennstrom",     f"{gen.get('rated_current')} A" if gen.get("rated_current") else None),
            ("Info-Stand",    dev.get("ts_utc")),
        ]))

        # Angesteckter Adapter/Stecker
        conn_type = con.get("type") or ""
        conn_desc = _decode_code(conn_type, "connector_type") if conn_type else ""
        con_display = f"{conn_type} ({conn_desc})" if conn_desc and conn_desc != "(unbekannter Code)" else conn_type
        cards.append(_info_table("Angesteckter Adapter", [
            ("Typ",           con_display),
            ("Seriennummer",  con.get("serial")),
            ("Max. Strom",    f"{con.get('max_current')} A" if con.get("max_current") is not None else None),
            ("Phasen",        con.get("phase_count")),
        ]))

        cards.append(_info_table("Netz (Grid)", [
            ("Spannung",      f"{grid.get('voltage')} V"   if grid.get("voltage")   is not None else None),
            ("Frequenz",      f"{grid.get('frequency')} Hz" if grid.get("frequency") is not None else None),
            ("Phasen",        grid.get("phases")),
        ]))

        rssi = net.get("rssi")
        rssi_text = None
        if rssi is not None:
            # RSSI-Qualitaet: >-50 super, -50..-60 gut, -60..-70 ok, -70..-80 schwach, <-80 kritisch
            if rssi >= -50:
                q = "sehr gut"
            elif rssi >= -60:
                q = "gut"
            elif rssi >= -70:
                q = "ausreichend"
            elif rssi >= -80:
                q = "schwach"
            else:
                q = "kritisch"
            rssi_text = f"{rssi} dBm ({q})"
        cards.append(_info_table("Netzwerk", [
            ("IP-Adresse",    net.get("ip_address")),
            ("MAC-Adresse",   net.get("mac_address")),
            ("WLAN-SSID",     net.get("ssid")),
            ("WLAN-Signal",   rssi_text),
        ]))

        cell_signal = cellular.get("signal")
        if cell_signal not in (None, ""):
            try:
                cell_signal = f"{float(cell_signal):.0f} dBm"
            except Exception:
                cell_signal = str(cell_signal)
        cards.append(_info_table("Mobilfunk", [
            ("Netz",          cellular.get("technology")),
            ("Anbieter",      cellular.get("operator")),
            ("Signal",        cell_signal),
            ("IMEI",          cellular.get("imei")),
            ("ICCID",         cellular.get("iccid")),
        ]))

        gps_rows: list[tuple[str, str]] = []
        if gps:
            gps_rows.append(("Koordinaten", f'{gps["lat"]:.6f}, {gps["lon"]:.6f}'))
            maps_url = f'https://www.openstreetmap.org/?mlat={gps["lat"]:.6f}&mlon={gps["lon"]:.6f}#map=16/{gps["lat"]:.6f}/{gps["lon"]:.6f}'
            gps_rows.append(("Karte", f'<a href="{maps_url}" target="_blank" rel="noopener noreferrer">OpenStreetMap</a>'))
            if gps.get("fix") not in (None, ""):
                gps_rows.append(("Fix", str(gps["fix"])))
            if gps.get("accuracy") not in (None, ""):
                try:
                    gps_rows.append(("Genauigkeit", f'{float(gps["accuracy"]):.1f} m'))
                except Exception:
                    gps_rows.append(("Genauigkeit", str(gps["accuracy"])))
        cards.append(_info_table("GPS", gps_rows))

        # Firmware-Versionen (alle Microcontroller einzeln)
        # NRGkick Gen2 hat: sm, ma, to, st - jeweils sw+hw
        # sm = ... ma = ... to = ... st = ...
        fw_module_labels = {
            "sm": "SmartModule (Hauptsteuerung)",
            "ma": "Master (Leistungselektronik)",
            "to": "Top (Taster/LEDs)",
            "st": "Stecker/Adapter",
        }
        fw_rows: list[tuple[str, str]] = []
        for mod, label in fw_module_labels.items():
            sw = ver.get(f"sw_{mod}")
            hw = ver.get(f"hw_{mod}")
            if sw or hw:
                parts = []
                if sw:
                    parts.append(f"SW {sw}")
                if hw:
                    parts.append(f"HW {hw}")
                fw_rows.append((label, " &middot; ".join(parts)))
        cards.append(_info_table("Firmware & Hardware", fw_rows))
    else:
        cards.append(
            '<div class="info-card"><h3>Geraet</h3>'
            '<p><i>Noch keine Geraete-Info in der DB. '
            'Laeuft der Logger? Einmal starten, damit <code>/info</code> abgefragt wird.</i></p>'
            '</div>'
        )

    # DB-Statistiken
    db_rows: list[tuple[str, str]] = []
    if stats.get("db_file"):
        db_rows.append(("DB-Datei", stats["db_file"]))
    if "db_size_mb" in stats:
        db_rows.append(("DB-Groesse", f"{stats['db_size_mb']:.2f} MB"))
    if "samples" in stats:
        db_rows.append(("Samples (samples)", str(stats['samples'])))
    if "kv_rows" in stats:
        db_rows.append(("KV-Zeilen (samples_kv)", str(stats['kv_rows'])))
    if "kv_paths" in stats:
        db_rows.append(("eindeutige API-Pfade", str(stats['kv_paths'])))
    if "n_enums" in stats:
        db_rows.append(("Code-Enums", str(stats['n_enums'])))
    if stats.get("first_sample"):
        db_rows.append(("erstes Sample",  stats['first_sample']))
    if stats.get("last_sample"):
        db_rows.append(("letztes Sample", stats['last_sample']))
    cards.append(_info_table("Datenbank", db_rows))

    # Enums
    enum_blocks: list[str] = []
    enum_labels = {
        "status":         "Status-Codes (general.status)",
        "error":          "Error-Codes",
        "warning":        "Warning-Codes",
        "relay":          "Relay-Zustaende",
        "rcd":            "RCD/FI-Zustaende",
        "connector_type": "Adapter-Typen",
    }
    for kind in ("status", "error", "warning", "relay", "rcd", "connector_type"):
        rows = enums.get(kind, [])
        if not rows:
            continue
        label = enum_labels.get(kind, kind)
        enum_blocks.append(
            f'<details><summary><b>{label}</b> ({len(rows)} Eintraege)</summary>'
            f'{_code_table_html(kind, rows)}</details>'
        )

    enums_section = ""
    if enum_blocks:
        enums_section = (
            '<h3 style="margin-top:2rem">Code-Nachschlagewerk</h3>'
            '<p class="hint">Diese Tabellen sind in der DB gespeichert '
            '(<code>code_enums</code>) und stehen auch fuer SQL-Joins zur Verfuegung.</p>'
            + "".join(enum_blocks)
        )

    return (
        '<section class="panel" id="panel-info">'
        '<h2>Info</h2>'
        '<p class="hint">Statische Geraete- und Systeminformationen. '
        'Quelle: <code>/info</code> der NRGkick (zuletzt abgefragt siehe "Info-Stand") '
        'sowie lokale DB-Tabellen.</p>'
        '<div class="info-grid">' + "".join(cards) + '</div>'
        + enums_section +
        '</section>'
    )


# ---------------------------------------------------------------------------
# Kabel-Analyse: Strom vs. Temperatur
# ---------------------------------------------------------------------------

def _prepare_cable_df(df: pd.DataFrame) -> pd.DataFrame:
    """Nur CHARGING-Samples mit I>0. Fuegt Spalten i_max und t_max hinzu."""
    if df.empty or "charging_state" not in df.columns:
        return pd.DataFrame()
    d = df[df["charging_state"] == "CHARGING"].copy()
    if d.empty:
        return d

    phase_cols = [c for c in ["current_l1_a", "current_l2_a", "current_l3_a"]
                  if c in d.columns]
    if not phase_cols:
        return pd.DataFrame()
    d["i_max"] = d[phase_cols].max(axis=1)

    temp_cols = [c for c in [
        "temp_connector_l1", "temp_connector_l2", "temp_connector_l3",
        "temp_domestic_plug", "temp_domestic_plug_1", "temp_domestic_plug_2",
        "temp_housing",
    ] if c in d.columns and d[c].notna().any()]
    if not temp_cols:
        return pd.DataFrame()
    d["t_max"] = d[temp_cols].max(axis=1)

    d = d.dropna(subset=["i_max", "t_max"])
    d = d[d["i_max"] > 0.5]   # Anlaufphase/Schein-Charging rausfiltern
    return d


def _filter_cable_by_amp(cable: pd.DataFrame, amp: int | None) -> pd.DataFrame:
    if cable.empty or amp is None:
        return cable
    d = cable.copy()
    d["i_bin"] = d["i_max"].round().astype(int)
    return d[d["i_bin"] == int(amp)].copy()


def fig_cable_scatter(cable: pd.DataFrame, amp: int | None = None) -> dict | None:
    """Streudiagramm Ist-Strom (x) vs. waermster Sensor (y).
    Farbcodiert nach Zeit - so sieht man die Entwicklung ueber den Zeitraum."""
    cable = _filter_cable_by_amp(cable, amp)
    if cable.empty or len(cable) < 5:
        return None

    # Zeit normalisiert 0..1 fuer Colorscale
    ts_num = cable.index.astype("int64").astype(float)
    t_min, t_max = ts_num.min(), ts_num.max()
    color = ((ts_num - t_min) / max(t_max - t_min, 1.0)).tolist() if t_max > t_min else [0.0] * len(cable)

    # Lineare Regression fuer Trendlinie (ohne scipy)
    x = cable["i_max"].astype(float).values
    y = cable["t_max"].astype(float).values
    if len(x) >= 2 and x.std() > 0:
        m = float(((x - x.mean()) * (y - y.mean())).sum() / ((x - x.mean()) ** 2).sum())
        b = float(y.mean() - m * x.mean())
        # Trend-Linie ueber den gesamten Stromsbereich zeichnen
        x_line = [float(x.min()), float(x.max())]
        y_line = [m * xv + b for xv in x_line]
        # Korrelationskoeffizient
        try:
            r = float(pd.Series(x).corr(pd.Series(y)))
        except Exception:
            r = float("nan")
    else:
        m, b, x_line, y_line, r = 0.0, 0.0, [], [], float("nan")

    hover_texts = [
        f"{ts.strftime('%d.%m. %H:%M')}<br>I = {ix:.2f} A<br>T = {it:.1f} °C"
        for ts, ix, it in zip(cable.index, x, y)
    ]

    scatter = {
        "type": "scattergl", "mode": "markers",
        "x": [float(v) for v in x.tolist()],
        "y": [float(v) for v in y.tolist()],
        "marker": {
            "size": 8, "opacity": 0.75,
            "color": color,
            "colorscale": "Plasma",
            "showscale": True,
            "colorbar": {
                "title": "Zeit",
                "tickmode": "array",
                "tickvals": [0.0, 1.0],
                "ticktext": [cable.index[0].strftime("%d.%m. %H:%M"),
                             cable.index[-1].strftime("%d.%m. %H:%M")],
                "x": 1.02,
            },
        },
        "hovertext": hover_texts,
        "hoverinfo": "text",
        "name": "Messpunkt",
    }

    traces = [scatter]
    annotations = []
    if x_line:
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x_line, "y": y_line,
            "line": {"color": "#d62728", "width": 2, "dash": "dash"},
            "name": f"Trend: +{m:.2f} °C/A",
            "hoverinfo": "skip",
        })
        annotations.append({
            "xref": "paper", "yref": "paper",
            "x": 0.02, "y": 0.98, "xanchor": "left", "yanchor": "top",
            "text": (f"<b>Trend:</b> pro +1 A Strom ≈ +{m:.2f} °C<br>"
                     f"<b>Korrelation r:</b> {r:+.2f}"),
            "showarrow": False,
            "bgcolor": "rgba(255,255,255,0.85)",
            "bordercolor": "rgba(0,0,0,0.2)",
            "borderwidth": 1, "borderpad": 6,
            "font": {"size": 12},
        })

    layout = {
        "title": ("Strom vs. waermster Sensor"
                  + (f" ({amp} A)" if amp is not None else " (alle CHARGING-Samples)")),
        "height": 480,
        "margin": {"l": 60, "r": 120, "t": 55, "b": 60},
        "xaxis": {"title": "Ist-Strom I (A)"},
        "yaxis": {"title": "Temperatur (°C) - waermster Sensor"},
        "template": "plotly_white",
        "annotations": annotations,
        "legend": {"orientation": "h", "y": -0.15},
    }
    return {"data": traces, "layout": layout}


def fig_cable_socket_scatter(cable: pd.DataFrame, amp: int | None = None) -> dict | None:
    """Strom (x) vs. Steckdosen-/Schuko-Temperatur (y)."""
    cable = _filter_cable_by_amp(cable, amp)
    if cable.empty or len(cable) < 5:
        return None

    socket_cols = [c for c in ["temp_domestic_plug", "temp_domestic_plug_1", "temp_domestic_plug_2"]
                   if c in cable.columns and cable[c].notna().any()]
    if not socket_cols:
        return None

    d = cable.copy()
    d["t_socket"] = d[socket_cols].max(axis=1)
    d = d.dropna(subset=["i_max", "t_socket"])
    if len(d) < 5:
        return None

    ts_num = d.index.astype("int64").astype(float)
    t_min, t_max = ts_num.min(), ts_num.max()
    color = ((ts_num - t_min) / max(t_max - t_min, 1.0)).tolist() if t_max > t_min else [0.0] * len(d)

    x = d["i_max"].astype(float).values
    y = d["t_socket"].astype(float).values
    if len(x) >= 2 and x.std() > 0:
        m = float(((x - x.mean()) * (y - y.mean())).sum() / ((x - x.mean()) ** 2).sum())
        b = float(y.mean() - m * x.mean())
        x_line = [float(x.min()), float(x.max())]
        y_line = [m * xv + b for xv in x_line]
        try:
            r = float(pd.Series(x).corr(pd.Series(y)))
        except Exception:
            r = float("nan")
    else:
        m, b, x_line, y_line, r = 0.0, 0.0, [], [], float("nan")

    hover_texts = [
        f"{ts.strftime('%d.%m. %H:%M')}<br>I = {ix:.2f} A<br>T Steckdose = {it:.1f} °C"
        for ts, ix, it in zip(d.index, x, y)
    ]

    traces = [{
        "type": "scattergl", "mode": "markers",
        "x": [float(v) for v in x.tolist()],
        "y": [float(v) for v in y.tolist()],
        "marker": {
            "size": 8, "opacity": 0.75,
            "color": color,
            "colorscale": "Plasma",
            "showscale": True,
            "colorbar": {
                "title": "Zeit",
                "tickmode": "array",
                "tickvals": [0.0, 1.0],
                "ticktext": [d.index[0].strftime("%d.%m. %H:%M"),
                             d.index[-1].strftime("%d.%m. %H:%M")],
                "x": 1.02,
            },
        },
        "hovertext": hover_texts,
        "hoverinfo": "text",
        "name": "Messpunkt",
    }]

    annotations = []
    if x_line:
        traces.append({
            "type": "scatter", "mode": "lines",
            "x": x_line, "y": y_line,
            "line": {"color": "#d62728", "width": 2, "dash": "dash"},
            "name": f"Trend: +{m:.2f} °C/A",
            "hoverinfo": "skip",
        })
        annotations.append({
            "xref": "paper", "yref": "paper",
            "x": 0.02, "y": 0.98, "xanchor": "left", "yanchor": "top",
            "text": (f"<b>Trend Steckdose:</b> pro +1 A Strom ≈ +{m:.2f} °C<br>"
                     f"<b>Korrelation r:</b> {r:+.2f}"),
            "showarrow": False,
            "bgcolor": "rgba(255,255,255,0.85)",
            "bordercolor": "rgba(0,0,0,0.2)",
            "borderwidth": 1, "borderpad": 6,
            "font": {"size": 12},
        })

    layout = {
        "title": ("Strom vs. Steckdose / Schuko-Adapter"
                  + (f" ({amp} A)" if amp is not None else "")),
        "height": 480,
        "margin": {"l": 60, "r": 120, "t": 55, "b": 60},
        "xaxis": {"title": "Ist-Strom I (A)"},
        "yaxis": {"title": "Temperatur (°C) - Steckdose / Schuko"},
        "template": "plotly_white",
        "annotations": annotations,
        "legend": {"orientation": "h", "y": -0.15},
    }
    return {"data": traces, "layout": layout}


def fig_cable_boxplot(cable: pd.DataFrame) -> dict | None:
    """Boxplot: Temperatur-Verteilung je Strom-Bin (ganzzahlige Ampere).
    Zeigt direkt 'bei X A werden typischerweise Y-Z Grad erreicht'."""
    if cable.empty or len(cable) < 10:
        return None
    d = cable.copy()
    d["i_bin"] = d["i_max"].round().astype(int)
    # Bins mit < 3 Samples ignorieren
    bin_counts = d["i_bin"].value_counts()
    good_bins = sorted(b for b in bin_counts.index if bin_counts[b] >= 3)
    if len(good_bins) < 2:
        return None

    traces = []
    for b in good_bins:
        sub = d[d["i_bin"] == b]["t_max"]
        traces.append({
            "type": "box",
            "name": f"{b} A",
            "y": [float(v) for v in sub.tolist()],
            "boxmean": True,          # zeigt Mittelwert als gestrichelte Linie
            "marker": {"color": "#1f77b4"},
            "line": {"color": "#1f77b4"},
            "hovertemplate": "%{y:.1f} °C<extra>" + f"{b} A</extra>",
        })

    layout = {
        "title": "Temperatur-Verteilung pro Ampere-Stufe",
        "height": 420,
        "margin": {"l": 60, "r": 20, "t": 50, "b": 60},
        "xaxis": {"title": "Ist-Strom (gerundet)", "type": "category"},
        "yaxis": {"title": "Temperatur (°C) - waermster Sensor"},
        "template": "plotly_white",
        "showlegend": False,
    }
    return {"data": traces, "layout": layout}


def _cable_recommendation(cable: pd.DataFrame, adapter_max_a: float | None) -> str:
    """Gibt einen knappen Empfehlungstext aus den Daten."""
    if cable.empty or len(cable) < 10:
        return ("<p><i>Noch zu wenig CHARGING-Daten fuer eine belastbare "
                "Empfehlung. Nach einigen Ladevorgaengen wird hier eine "
                "Einschaetzung stehen.</i></p>")

    i_max = float(cable["i_max"].quantile(0.95))   # robust: 95%-Percentil
    t_med_at_max = float(
        cable.loc[cable["i_max"] >= (i_max - 1.0), "t_max"].median()
    )

    parts: list[str] = []
    parts.append(
        f'<p><b>Messbasis:</b> {len(cable)} Samples unter Ladung. '
        f'Max. beobachteter Strom: <b>{cable["i_max"].max():.1f} A</b>. '
        f'95%-Percentil Strom: <b>{i_max:.1f} A</b> bei '
        f'Temperatur <b>{t_med_at_max:.1f} °C</b> (Median).</p>'
    )

    # Groesster beobachteter Temperatur-Wert
    t_peak = float(cable["t_max"].max())

    # Schwellen aus Config (konfigurierbar in thresholds.temperature_*)
    t_cool  = float(_cfg_get("thresholds.temperature_cool",     40.0))
    t_warm  = float(_cfg_get("thresholds.temperature_warm",     60.0))
    t_hot   = float(_cfg_get("thresholds.temperature_hot",      75.0))
    if t_peak < t_cool:
        color = "#2ca02c"
        label = "unkritisch"
        hint = ("Du hast deutlich Reserve. Ein etwas hoeheres Limit "
                "waere wahrscheinlich unproblematisch - wenn dein Adapter das zulaesst. ")
    elif t_peak < t_warm:
        color = "#ff7f0e"
        label = "beobachtbar"
        hint = (f"Moderate Erwaermung. Du liegst noch weit von typischen "
                f"Abschaltschwellen (~{t_hot:.0f}-{t_hot+10:.0f} °C), aber eine "
                f"Erhoehung des Stroms wuerde die Temperatur deutlich steigern. ")
    elif t_peak < t_hot:
        color = "#d62728"
        label = "grenzwertig"
        hint = ("Deutliche Erwaermung, naehert sich Derating-Bereichen. "
                "Ein niedrigeres Limit (1-2 A weniger) koennte das Kabel schonen. ")
    else:
        color = "#7c1a1a"
        label = "kritisch"
        hint = ("Temperatur bereits im kritischen Bereich - Derating ist wahrscheinlich. "
                "Strom-Limit reduzieren oder fuer bessere Belueftung sorgen. ")

    parts.append(
        f'<p><b>Max. gemessene Temperatur:</b> '
        f'<span style="color:{color}; font-weight:600">{t_peak:.1f} °C ({label})</span></p>'
    )
    parts.append(f'<p>{hint}')

    # Adapter-Info mit einbeziehen
    if adapter_max_a is not None:
        parts.append(
            f'Dein angeschlossener Adapter ist fuer bis zu '
            f'<b>{adapter_max_a:.0f} A</b> zugelassen. '
        )
        if cable["i_max"].max() < adapter_max_a - 0.5:
            parts.append(
                f'Du laedst aktuell mit max. {cable["i_max"].max():.1f} A - '
                f'unterhalb der Adapter-Grenze.'
            )

    parts.append('</p>')

    # Trend-Extrapolation
    x = cable["i_max"].astype(float).values
    y = cable["t_max"].astype(float).values
    if len(x) >= 10 and x.std() > 0:
        m = float(((x - x.mean()) * (y - y.mean())).sum() / ((x - x.mean()) ** 2).sum())
        current_max = float(x.max())
        if adapter_max_a is not None and adapter_max_a > current_max + 0.5:
            delta = adapter_max_a - current_max
            est_t_at_max = t_peak + m * delta
            parts.append(
                f'<p><b>Abschaetzung:</b> Bei maximaler Adapter-Ausnutzung '
                f'({adapter_max_a:.0f} A) waere die Temperatur vermutlich bei ca. '
                f'<b>{est_t_at_max:.1f} °C</b> '
                f'(auf Basis Steigung {m:+.2f} °C/A).</p>'
            )

    return "".join(parts)


def _get_adapter_max_a() -> float | None:
    """Liest den max. Strom des aktuell angesteckten Adapters aus device_info."""
    dev = _load_device_info()
    if not dev:
        return None
    raw = dev.get("raw") or {}
    con = raw.get("connector") or {}
    mc = con.get("max_current")
    try:
        return float(mc) if mc is not None else None
    except Exception:
        return None


def build_cable_panel(df: pd.DataFrame, plots_out: dict) -> str:
    cable = _prepare_cable_df(df)
    if cable.empty:
        return ('<section class="panel" id="panel-cable">'
                '<h2>Kabel-Analyse</h2>'
                '<p><i>Keine Lade-Daten im gewaehlten Zeitraum. '
                'Mit ersten Ladevorgaengen entsteht hier automatisch eine '
                'Analyse des Zusammenhangs zwischen Ladestrom und Temperatur.</i></p>'
                '</section>')

    adapter_max_a = _get_adapter_max_a()
    amp_bins = sorted(int(v) for v in cable["i_max"].round().dropna().astype(int).unique().tolist())

    fig_scatter = fig_cable_scatter(cable)
    fig_socket  = fig_cable_socket_scatter(cable)
    fig_box     = fig_cable_boxplot(cable)

    if fig_scatter:
        plots_out["plot-cable-scatter"] = fig_scatter
    if fig_socket:
        plots_out["plot-cable-socket"] = fig_socket
    if fig_box:
        plots_out["plot-cable-box"] = fig_box

    cable_views: list[str] = []
    if fig_scatter:
        cable_views.append('<div class="cable-view" id="cable-view-warmest-all">' + _plot_div("plot-cable-scatter") + '</div>')
    if fig_socket:
        cable_views.append('<div class="cable-view hidden" id="cable-view-socket-all">' + _plot_div("plot-cable-socket") + '</div>')
    if fig_box:
        cable_views.append('<div class="cable-view hidden" id="cable-view-box-all">' + _plot_div("plot-cable-box") + '</div>')

    for amp in amp_bins:
        fig_scatter_amp = fig_cable_scatter(cable, amp)
        fig_socket_amp = fig_cable_socket_scatter(cable, amp)
        if fig_scatter_amp:
            pid = f"plot-cable-scatter-{amp}a"
            plots_out[pid] = fig_scatter_amp
            cable_views.append(f'<div class="cable-view hidden" id="cable-view-warmest-{amp}a">' + _plot_div(pid) + '</div>')
        if fig_socket_amp:
            pid = f"plot-cable-socket-{amp}a"
            plots_out[pid] = fig_socket_amp
            cable_views.append(f'<div class="cable-view hidden" id="cable-view-socket-{amp}a">' + _plot_div(pid) + '</div>')

    rec = _cable_recommendation(cable, adapter_max_a)

    intro = (
        '<p class="hint">'
        'Die folgenden Plots zeigen, wie sich die Temperatur des waermsten '
        'Sensors (Stecker, Schuko-Adapter oder Gehaeuse - je nachdem was am '
        'heissesten ist) mit dem Ist-Ladestrom veraendert. Damit laesst sich '
        'abschaetzen, bei welcher Stromstaerke thermisches Derating droht - '
        'und wo ein sinnvolles Strom-Limit liegt.'
        '</p>'
    )

    cable_select = (
        '<div class="session-selector">'
        '<label for="cable-plot-select">Grafik: </label>'
        '<select id="cable-plot-select" onchange="selectCableView()">'
        '<option value="warmest" selected>waermster Sensor</option>'
        '<option value="socket">Steckdose / Schuko</option>'
        '<option value="box">Verteilung je Ampere</option>'
        '</select>'
        '<label for="cable-amp-select">Strom: </label>'
        '<select id="cable-amp-select" onchange="selectCableView()">'
        '<option value="all" selected>alle Punkte</option>'
        + ''.join(f'<option value="{amp}a">{amp} A</option>' for amp in amp_bins)
        + '</select>'
        '</div>'
    )

    return (
        '<section class="panel" id="panel-cable">'
        '<h2>Kabel-Analyse</h2>'
        + intro
        + '<div class="info-card"><h3>Empfehlung</h3>'
        + rec + '</div>'
        + cable_select
        + ''.join(cable_views)
        + '</section>'
    )


# ---------------------------------------------------------------------------
# HTML / Report
# ---------------------------------------------------------------------------

SECTIONS: list[tuple[str, str]] = [
    ("dashboard", "Dashboard"),
    ("info",      "Info"),
    ("current",   "Aktuelle Session"),
    ("analysis",  "Ladevorgang-Analyse"),
    ("events",    "Ereignisse"),
    ("temps",     "Temperaturen"),
    ("cable",     "Kabel-Analyse"),
    ("power",     "Leistung & Strom"),
    ("energy",    "Energie/Tag"),
    ("sessions",  "Ladesitzungen"),
    ("heatmap",   "Ladeaktivitaet"),
]

# Klartext-Beschreibungen fuer Codes werden primaer aus der DB-Tabelle
# `code_enums` geladen (vom Logger beim Start befuellt). Dieses Dict ist nur
# Minimal-Fallback, falls die Tabelle nicht existiert oder noch nicht
# synchronisiert wurde.
_FALLBACK_DESCRIPTIONS: dict[tuple[str, str], str] = {
    ("error",   "NO_ERROR"):    "kein Fehler",
    ("warning", "NO_WARNING"):  "keine Warnung",
}

_code_cache: dict[tuple[str, str], tuple[str, str]] | None = None


def _load_code_enums() -> dict[tuple[str, str], tuple[str, str]]:
    """Laedt die Enum-Tabelle aus der DB einmalig. Schluessel: (kind, code),
    Wert: (description, severity)."""
    global _code_cache
    if _code_cache is not None:
        return _code_cache
    out: dict[tuple[str, str], tuple[str, str]] = {}
    try:
        if DB_FILE.exists():
            with sqlite3.connect(str(DB_FILE)) as conn:
                rows = conn.execute(
                    "SELECT kind, code, description, severity FROM code_enums"
                ).fetchall()
                for kind, code, desc, sev in rows:
                    out[(kind, code)] = (desc or "", sev or "info")
    except Exception:
        pass
    _code_cache = out
    return out


def _decode_code(code: str, kind: str) -> str:
    if code is None:
        return "-"
    code_str = str(code)
    row = _load_code_enums().get((kind, code_str))
    if row:
        return row[0] or code_str
    fb = _FALLBACK_DESCRIPTIONS.get((kind, code_str))
    if fb:
        return fb
    return "(unbekannter Code)"


VALID_DEFAULTS = {sid for sid, _ in SECTIONS}


# Default-Plotly-Config fuer alle Plots: dt. Oberflaeche + nuetzliche Modes.
PLOTLY_CONFIG = {
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,  # Mausrad = Zoom
    "modeBarButtonsToAdd": ["drawline", "drawrect", "eraseshape"],
    "toImageButtonOptions": {"format": "png", "filename": "nrgkick", "scale": 2},
    "locale": "de",
}


HTML_TEMPLATE = """<!doctype html>
<html lang="de"><head>
<meta charset="utf-8">
<title>NRGkick Report - {title}</title>
<script src="{plotly_cdn}" charset="utf-8"></script>
<style>
  :root {{
    color-scheme: light dark;
    --bg: #ffffff; --fg: #222; --muted: #666;
    --card: #f2f4f7; --border: #e5e7eb;
    --accent: #2563eb; --accent-fg: #fff;
    --tab: #eef2f7; --tab-hover: #dde4ee;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg: #0f1115; --fg: #e6e6e6; --muted: #aaa;
      --card: #1a1d23; --border: #2a2f37;
      --accent: #3b82f6; --accent-fg: #fff;
      --tab: #1a1d23; --tab-hover: #262a33;
    }}
  }}
  html, body {{ background: var(--bg); color: var(--fg); }}
  body {{
    font-family: -apple-system, Segoe UI, Roboto, sans-serif;
    margin: 2rem auto; max-width: 1280px; padding: 0 1rem;
    line-height: 1.45;
  }}
  h1 {{ margin-bottom: 0.2rem; }}
  h2 {{ margin-top: 0; }}
  .sub {{ color: var(--muted); margin-top: 0; }}

  .kpis {{ display: flex; flex-wrap: wrap; gap: 1rem; margin: 1.4rem 0 1.8rem; }}
  .kpi {{
    flex: 1 1 160px; padding: 0.9rem 1rem; border-radius: 10px;
    background: var(--card); border: 1px solid var(--border);
  }}
  .kpi .v {{ font-size: 1.6rem; font-weight: 600; }}
  .kpi .l {{ font-size: 0.85rem; color: var(--muted); margin-top: 0.15rem; }}

  nav.tabs {{
    display: flex; flex-wrap: wrap; gap: 0.4rem;
    margin: 1.2rem 0 1.4rem; padding: 0.6rem 0 0.8rem;
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; background: var(--bg); z-index: 10;
  }}
  nav.tabs button {{
    border: 1px solid var(--border); background: var(--tab); color: var(--fg);
    padding: 0.45rem 0.9rem; border-radius: 999px; cursor: pointer;
    font-size: 0.92rem; font-weight: 500;
    transition: background 0.1s ease, color 0.1s ease;
  }}
  nav.tabs button:hover {{ background: var(--tab-hover); }}
  nav.tabs button.active {{
    background: var(--accent); color: var(--accent-fg);
    border-color: var(--accent);
  }}

  section.panel {{ margin: 1.6rem 0 2.5rem; }}
  section.panel.hidden {{ display: none; }}

  .plot {{ width: 100%; min-height: 300px; border-radius: 8px; }}
  .plot-wrap {{ background: var(--card); border: 1px solid var(--border);
                border-radius: 10px; padding: 0.6rem 0.4rem; margin-bottom: 1rem; }}
  .plot-title {{ font-weight: 600; padding: 0 0.6rem; color: var(--muted); font-size: 0.9rem; }}
  .hint {{ color: var(--muted); font-size: 0.85rem; margin: 0 0 0.8rem 0; }}

  .dash-grid {{ display: grid; grid-template-columns: 1fr; gap: 1rem; }}
  @media (min-width: 1100px) {{
    .dash-grid {{ grid-template-columns: 1fr 1fr; }}
    .dash-full {{ grid-column: 1 / -1; }}
  }}

  table.sessions {{ border-collapse: collapse; width: 100%; font-size: 0.92rem; }}
  table.sessions th, table.sessions td {{
    padding: 0.4rem 0.6rem; border-bottom: 1px solid var(--border); text-align: left;
  }}
  table.sessions th {{ background: var(--card); }}

  .current-teaser {{
    background: linear-gradient(135deg, rgba(34,197,94,0.10), rgba(59,130,246,0.10));
    border: 1px solid var(--border); border-radius: 12px;
    padding: 0.8rem 1rem; margin: 0 0 1.4rem;
  }}
  .current-teaser .ct-head {{
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 0.5rem; font-size: 1rem;
  }}
  .current-teaser .ct-head a {{ color: var(--accent); text-decoration: none; font-size: 0.9rem; }}
  .current-teaser .ct-head a:hover {{ text-decoration: underline; }}
  .current-teaser .kpis {{ margin: 0; }}

  .session-selector {{
    display: flex; align-items: center; gap: 0.6rem;
    margin: 1rem 0 1.2rem;
    padding: 0.6rem 0.8rem;
    background: var(--card); border: 1px solid var(--border); border-radius: 8px;
  }}
  .session-selector select {{
    flex: 1; padding: 0.4rem 0.6rem; border-radius: 6px;
    border: 1px solid var(--border); background: var(--bg); color: var(--fg);
    font-size: 0.95rem;
  }}
  .analysis-session {{ margin: 1rem 0; }}
  .analysis-session.hidden {{ display: none; }}
  .analysis-scatter-view.hidden {{ display: none; }}
  .cable-view.hidden {{ display: none; }}

  .limit-card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 0.8rem 1rem;
    margin: 0 0 1.4rem;
  }}
  .limit-head {{ display: flex; justify-content: space-between;
                 flex-wrap: wrap; gap: 0.4rem; margin-bottom: 0.5rem;
                 font-size: 0.95rem; }}
  .limit-stats {{ color: var(--muted); }}
  .limit-bar-bg {{ background: rgba(128,128,128,0.15); height: 16px;
                   border-radius: 999px; overflow: hidden;
                   border: 1px solid var(--border); }}
  .limit-bar-fg {{ height: 100%; transition: width 0.3s ease; }}

  table.events {{ border-collapse: collapse; width: 100%; font-size: 0.92rem; }}
  table.events th, table.events td {{
    padding: 0.4rem 0.6rem; border-bottom: 1px solid var(--border); text-align: left;
    vertical-align: top;
  }}
  table.events th {{ background: var(--card); }}
  table.events td.code {{ font-family: Consolas, 'Courier New', monospace;
                          font-size: 0.88rem; }}
  .sev-error   {{ color: #d62728; font-weight: 600; }}
  .sev-warn    {{ color: #ff7f0e; font-weight: 600; }}
  .sev-ok      {{ color: #2ca02c; }}

  .info-grid {{
    display: grid; gap: 1rem;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    margin: 1rem 0 1.4rem;
  }}
  .info-card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 0.8rem 1.1rem;
  }}
  .info-card h3 {{ margin: 0 0 0.5rem; font-size: 1.02rem; color: var(--muted); }}
  table.info-table {{ width: 100%; border-collapse: collapse; font-size: 0.92rem; }}
  table.info-table th {{
    text-align: left; font-weight: 500; color: var(--muted);
    padding: 0.35rem 0; width: 45%; vertical-align: top;
  }}
  table.info-table td {{
    padding: 0.35rem 0; font-family: Consolas, 'Courier New', monospace; font-size: 0.88rem;
    word-break: break-word;
  }}
  details summary {{ cursor: pointer; padding: 0.4rem 0; color: var(--fg); }}
  details[open] summary {{ margin-bottom: 0.4rem; }}

  .temp-tiles {{
    display: grid; gap: 0.7rem;
    grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    margin: 0.4rem 0 1.4rem;
  }}
  .temp-tile {{
    border: 1px solid var(--border); border-radius: 10px;
    padding: 0.6rem 0.8rem;
  }}
  .temp-tile .tt-label {{ font-size: 0.82rem; color: var(--muted); margin-bottom: 0.2rem; }}
  .temp-tile .tt-value {{ font-size: 1.6rem; font-weight: 600; line-height: 1; }}
  .temp-tile .tt-value span {{ font-size: 0.85rem; font-weight: 400; color: var(--muted); }}
  .temp-tile .tt-range {{ font-size: 0.78rem; color: var(--muted); margin-top: 0.35rem; }}

  .badges {{ margin: 0.6rem 0 1.2rem; }}
  .badges a {{ text-decoration: none; color: inherit; }}
  .status-badge {{
    display: inline-block; padding: 0.25rem 0.6rem; border-radius: 999px;
    font-size: 0.85rem; font-weight: 600; margin-right: 0.4rem;
  }}
  .status-badge.err {{ background: rgba(214,39,40,0.15); color: #d62728;
                       border: 1px solid rgba(214,39,40,0.4); }}
  .status-badge.warn {{ background: rgba(255,127,14,0.15); color: #ff7f0e;
                        border: 1px solid rgba(255,127,14,0.4); }}

  footer {{ color: var(--muted); font-size: 0.8rem; margin-top: 3rem; }}
</style>
</head><body>
<h1>NRGkick Report</h1>
<p class="sub">{title} &middot; {start_str} &ndash; {end_str} &middot; {samples} Messpunkte</p>
{kpi_html}
<nav class="tabs" id="tabs">{tabs_html}</nav>
<main id="panels">
{sections_html}
</main>
<footer>
  Interaktion: Bereich mit Maus aufziehen = Zoom &middot; Doppelklick = Reset
  &middot; Legenden-Eintrag klicken = Serie aus/an &middot;
  Mausrad = Zoom in Achsenrichtung.
  <br>erzeugt: {generated} &middot; Quelle: {db}
</footer>

<script>
const PLOTS = {plots_json};
const PLOTLY_CONFIG = {plotly_config_json};

function renderPlot(id) {{
  const spec = PLOTS[id];
  if (!spec) return;
  const el = document.getElementById(id);
  if (!el || el.dataset.rendered === "1") return;
  Plotly.newPlot(el, spec.data, spec.layout, PLOTLY_CONFIG);
  el.dataset.rendered = "1";
}}

function activate(id, push) {{
  document.querySelectorAll('section.panel').forEach(p => {{
    p.classList.toggle('hidden', p.id !== 'panel-' + id);
  }});
  document.querySelectorAll('nav.tabs button').forEach(t => {{
    t.classList.toggle('active', t.dataset.target === id);
  }});
  // Plots erst rendern, wenn Panel sichtbar wird (Resize korrekt)
  const panel = document.getElementById('panel-' + id);
  if (panel) {{
    panel.querySelectorAll('.plot').forEach(el => {{
      renderPlot(el.id);
      // nach kurzem Delay ggf. Resize antriggern
      requestAnimationFrame(() => Plotly.Plots.resize(el));
    }});
  }}
  if (push) history.replaceState(null, '', '#' + id);
}}

document.querySelectorAll('nav.tabs button').forEach(t => {{
  t.addEventListener('click', () => activate(t.dataset.target, true));
}});

// Session-Switcher fuer "Ladevorgang-Analyse"
function selectAnalysisSession(id) {{
  document.querySelectorAll('.analysis-session').forEach(el => {{
    el.classList.toggle('hidden', el.id !== id);
  }});
  // Plots in dieser Session rendern / resizen
  const sec = document.getElementById(id);
  if (sec) {{
    sec.querySelectorAll('.plot').forEach(el => {{
      renderPlot(el.id);
      requestAnimationFrame(() => Plotly.Plots.resize(el));
    }});
  }}
  selectAnalysisScatter(id);
}}

function selectAnalysisScatter(id) {{
  const typeSel = document.getElementById(id + '-scatter-type');
  const ampSel = document.getElementById(id + '-scatter-amp');
  if (!typeSel || !ampSel) return;
  const plotType = typeSel.value || 'warmest';
  const amp = ampSel.value || 'all';
  const targetId = id + '-scatter-' + plotType + '-' + amp;

  document.querySelectorAll('#' + id + ' .analysis-scatter-view').forEach(el => {{
    el.classList.toggle('hidden', el.id !== targetId);
  }});

  const sec = document.getElementById(targetId);
  if (sec) {{
    sec.querySelectorAll('.plot').forEach(el => {{
      renderPlot(el.id);
      requestAnimationFrame(() => Plotly.Plots.resize(el));
    }});
  }}
}}

function selectCableView() {{
  const plotSel = document.getElementById('cable-plot-select');
  const ampSel = document.getElementById('cable-amp-select');
  if (!plotSel || !ampSel) return;
  const plotType = plotSel.value || 'warmest';
  const amp = (plotType === 'box') ? 'all' : (ampSel.value || 'all');
  ampSel.disabled = (plotType === 'box');
  const targetId = 'cable-view-' + plotType + '-' + amp;

  document.querySelectorAll('.cable-view').forEach(el => {{
    el.classList.toggle('hidden', el.id !== targetId);
  }});

  const sec = document.getElementById(targetId);
  if (sec) {{
    sec.querySelectorAll('.plot').forEach(el => {{
      renderPlot(el.id);
      requestAnimationFrame(() => Plotly.Plots.resize(el));
    }});
  }}
}}

window.addEventListener('resize', () => {{
  document.querySelectorAll('.plot').forEach(el => {{
    if (el.dataset.rendered === "1" && el.offsetParent !== null) {{
      Plotly.Plots.resize(el);
    }}
  }});
}});

const initial = (location.hash || '').replace('#', '') || "{default_tab}";
activate(initial, false);
selectCableView();
</script>
</body></html>
"""


def _plot_div(plot_id: str, title: str | None = None,
              wrap_class: str = "") -> str:
    t = f'<div class="plot-title">{title}</div>' if title else ""
    extra = f" {wrap_class}" if wrap_class else ""
    return (f'<div class="plot-wrap{extra}">{t}'
            f'<div class="plot" id="{plot_id}"></div></div>')


def build_report(df: pd.DataFrame, default_tab: str) -> tuple[str, str, dict, pd.DataFrame]:
    """Erzeugt tabs_html, sections_html, plots-dict (JSON-serialisierbar)
    und das sessions-DataFrame."""
    sess_df = display_sessions(df)
    current_df, start_from_counter = find_current_session(df)

    # alle Plots vorausbauen (werden per JS gerendert, Panels sind schon im DOM)
    plots: dict[str, dict] = {}
    temps_fig     = fig_temperatures(df)
    temps_all_fig = fig_temperatures_all(df)
    power_fig     = fig_power_current(df)
    energy_fig, _daily = fig_energy_per_day(df)
    heatmap_fig   = fig_power_heatmap(df)

    if temps_fig:
        plots["plot-temps"] = temps_fig
    if temps_all_fig:
        plots["plot-temps-all"] = temps_all_fig
    if power_fig:
        plots["plot-power"] = power_fig
    if energy_fig:
        plots["plot-energy"] = energy_fig
    if heatmap_fig:
        plots["plot-heatmap"] = heatmap_fig

    # Dashboard-Klone (eigene IDs, damit jede Seite ihren eigenen Plot-Container hat)
    if temps_fig:
        plots["plot-dash-temps"] = temps_fig
    if power_fig:
        plots["plot-dash-power"] = power_fig
    if energy_fig:
        plots["plot-dash-energy"] = energy_fig
    if heatmap_fig:
        plots["plot-dash-heatmap"] = heatmap_fig

    # --- Panels ---------------------------------------------------------
    last_button_hint = "Alles" if REPORT_RANGE_NAME == "all" else "Zeitraum"
    hint_zoom = (f'<p class="hint">Tipp: Bereich mit der Maus aufziehen '
                 f'= hineinzoomen &middot; Doppelklick = Reset &middot; '
                 f'Shortcuts 1h/6h/24h/7T/30T/{last_button_hint} oben links im Plot.</p>')
    hint_legend = ('<p class="hint">Klick auf Legenden-Eintraege blendet '
                   'einzelne Kurven aus/ein.</p>')

    def _panel_plot(sid: str, plot_id: str, title: str, extra_hint: str = "") -> str:
        if plot_id not in plots:
            body = "<p><i>Keine Daten im Zeitraum.</i></p>"
        else:
            body = hint_zoom + extra_hint + _plot_div(plot_id)
        return (f'<section class="panel" id="panel-{sid}">'
                f'<h2>{title}</h2>{body}</section>')

    # Temperaturen-Panel: Kacheln + Gesamt-Plot + kompakter Plot
    temp_tiles = temperature_tiles_html(df)
    if "plot-temps-all" not in plots and "plot-temps" not in plots:
        temp_body = "<p><i>Keine Temperaturdaten im Zeitraum.</i></p>"
    else:
        temp_body = (
            temp_tiles
            + hint_zoom + hint_legend
            + (_plot_div("plot-temps-all", "Alle Sensoren")
               if "plot-temps-all" in plots else "")
            + '<h3 style="margin-top:2rem">Kompakt-Ansicht</h3>'
            + '<p class="hint">Ohne Schuko-Einzelsensoren - uebersichtlicher fuer den '
              'schnellen Blick auf Gehaeuse, 3 Phasen und Schuko-Mittelwert.</p>'
            + (_plot_div("plot-temps") if "plot-temps" in plots else "")
        )
    panel_temps = (
        f'<section class="panel" id="panel-temps">'
        f'<h2>Temperaturen</h2>{temp_body}</section>'
    )
    panel_power = _panel_plot(
        "power", "plot-power", "Leistung & Strom je Phase", hint_legend,
    )
    panel_energy = _panel_plot(
        "energy", "plot-energy", "Lademenge je Tag",
        '<p class="hint">abgeleitet aus der Differenz des Lifetime-Zaehlers.</p>',
    )
    panel_heatmap = _panel_plot(
        "heatmap", "plot-heatmap", "Ladeaktivitaet nach Tag & Stunde",
    )
    panel_sessions = (
        f'<section class="panel" id="panel-sessions">'
        f'<h2>Ladesitzungen</h2>{sessions_table_html(sess_df)}</section>'
    )

    panel_current = current_session_html(current_df, plots, start_from_counter=start_from_counter)
    panel_analysis = build_analysis_section(df, plots)
    panel_events   = build_events_panel(df, plots)
    panel_info     = build_info_panel()
    panel_cable    = build_cable_panel(df, plots)

    # Dashboard mit Grid
    dash_tiles: list[str] = []
    if "plot-dash-temps" in plots:
        dash_tiles.append(_plot_div("plot-dash-temps", "Temperaturen"))
    if "plot-dash-power" in plots:
        dash_tiles.append(_plot_div("plot-dash-power", "Leistung & Strom"))
    if "plot-dash-energy" in plots:
        dash_tiles.append(_plot_div("plot-dash-energy", "Lademenge je Tag"))
    if "plot-dash-heatmap" in plots:
        dash_tiles.append(_plot_div("plot-dash-heatmap", "Ladeaktivitaet nach Tag & Stunde", wrap_class="dash-full"))
    dash_body = "".join(dash_tiles) if dash_tiles else "<p><i>Keine Daten im Zeitraum.</i></p>"

    # Teaser fuer aktuelle Session oben im Dashboard
    current_teaser = ""
    if not current_df.empty:
        items = current_session_kpis(current_df)
        if items:
            chips = "".join(
                f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div></div>'
                for value, label in items[:5]
            )
            limit_bar = _energy_limit_progress_html(current_df)
            current_teaser = (
                '<div class="current-teaser">'
                '<div class="ct-head"><b>Aktuelle Session</b> '
                '<a href="#current" onclick="activate(\'current\', true); return false;">Details &rarr;</a></div>'
                f'<div class="kpis">{chips}</div>'
                f'{limit_bar}'
                '</div>'
            )

    # Status-Badge, wenn aktuell ein Fehler/Warnung aktiv
    last = df.iloc[-1] if not df.empty else None
    badges: list[str] = []
    if last is not None:
        le = str(last.get("error_code",   "") or "")
        lw = str(last.get("warning_code", "") or "")
        if le and le not in NORMAL_CODES:
            badges.append(
                f'<span class="status-badge err">Fehler: {le}</span>'
            )
        if lw and lw not in NORMAL_CODES:
            badges.append(
                f'<span class="status-badge warn">Warnung: {lw}</span>'
            )
    badge_block = (
        '<div class="badges"><a href="#events" '
        'onclick="activate(\'events\', true); return false;">'
        + " ".join(badges) + ' &rarr; Ereignisse</a></div>'
    ) if badges else ""

    panel_dashboard = (
        f'<section class="panel" id="panel-dashboard">'
        f'<h2>Dashboard</h2>'
        f'{badge_block}'
        f'{current_teaser}'
        f'<p class="hint">Uebersicht aller Plots. Jeder Plot ist interaktiv: '
        f'Zoom, Pan, Legenden-Toggle.</p>'
        f'<div class="dash-grid">{dash_body}</div>'
        f'<h3 style="margin-top:2rem">Ladesitzungen</h3>{sessions_table_html(sess_df)}'
        f'</section>'
    )

    # Tabs
    tab_parts: list[str] = []
    for sid, label in SECTIONS:
        active = ' class="active"' if sid == default_tab else ""
        tab_parts.append(f'<button type="button" data-target="{sid}"{active}>{label}</button>')
    tabs_html = "".join(tab_parts)

    # Reihenfolge der Panels
    panel_map = {
        "dashboard": panel_dashboard,
        "current":   panel_current,
        "analysis":  panel_analysis,
        "events":    panel_events,
        "temps":     panel_temps,
        "cable":     panel_cable,
        "power":     panel_power,
        "energy":    panel_energy,
        "sessions":  panel_sessions,
        "heatmap":   panel_heatmap,
        "info":      panel_info,
    }
    sections_html = "".join(
        panel_map[sid].replace('class="panel"', 'class="panel hidden"', 1)
        if sid != default_tab else panel_map[sid]
        for sid, _ in SECTIONS
    )

    return tabs_html, sections_html, plots, sess_df


def render_html(title: str, df: pd.DataFrame, tabs_html: str, sections_html: str,
                plots: dict, default_tab: str,
                start: datetime | None, end: datetime) -> str:
    if not df.empty:
        start_str = df.index.min().strftime("%Y-%m-%d %H:%M")
        end_str   = df.index.max().strftime("%Y-%m-%d %H:%M")
    else:
        start_str = start.strftime("%Y-%m-%d %H:%M") if start else "Anfang"
        end_str   = end.strftime("%Y-%m-%d %H:%M")
    return HTML_TEMPLATE.format(
        title=title,
        start_str=start_str,
        end_str=end_str,
        samples=len(df),
        kpi_html=kpi_html(df, display_sessions(df)),
        tabs_html=tabs_html,
        sections_html=sections_html,
        plots_json=json.dumps(plots, ensure_ascii=False, default=str),
        plotly_config_json=json.dumps(PLOTLY_CONFIG, ensure_ascii=False),
        plotly_cdn=PLOTLY_CDN,
        default_tab=default_tab,
        generated=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        db=str(DB_FILE),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    global CFG, DATA_DIR, DB_FILE, REPORT_DIR, REPORT_RANGE_NAME

    argv = argv if argv is not None else sys.argv[1:]

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config")
    pre_args, _ = pre_parser.parse_known_args(argv)

    # Defaults aus Config auflesen, sodass argparse sie als --help zeigen kann
    _preview_cfg = _load_cfg(pre_args.config) if pre_args.config else _load_cfg()
    report_cfg = _preview_cfg.get("report", {}) or {}
    default_range = report_cfg.get("default_range", "24h")
    default_tab   = report_cfg.get("default_tab",   "dashboard")
    default_open  = bool(report_cfg.get("auto_open", True))

    parser = argparse.ArgumentParser(
        description="Erzeugt einen interaktiven HTML-Report aus der NRGkick-Logger DB. "
                    "Alle Grafiken sind zoombar (Plotly).",
    )
    parser.add_argument(
        "--range", default=default_range,
        choices=list(RANGES.keys()),
        help=f"Zeitraum-Preset (default aus Config: {default_range})",
    )
    parser.add_argument(
        "--default",
        default=default_tab if default_tab in VALID_DEFAULTS else "dashboard",
        choices=sorted(VALID_DEFAULTS),
        help=f"welcher Tab beim Oeffnen aktiv ist (default: {default_tab})",
    )
    parser.add_argument("--out", help="Ausgabedatei (Default: reports/report_<range>_<zeit>.html)")
    parser.add_argument(
        "--open", dest="open", action="store_true",  default=None,
        help="Report im Browser oeffnen",
    )
    parser.add_argument(
        "--no-open", dest="open", action="store_false",
        help="Report NICHT automatisch im Browser oeffnen",
    )
    parser.add_argument(
        "--config", help="alternativer Pfad zur config.json",
    )
    args = parser.parse_args(argv)
    REPORT_RANGE_NAME = args.range

    # Ggf. andere Config laden (falls --config uebergeben)
    CFG = _load_cfg(args.config) if args.config else _preview_cfg
    DATA_DIR   = _db_file().parent
    DB_FILE    = _db_file()
    REPORT_DIR = _report_dir()

    open_in_browser = default_open if args.open is None else args.open

    start, end = resolve_range(args.range)
    df = load_samples(start, end)
    if df.empty:
        print(f"Keine Daten im Zeitraum {args.range}.", file=sys.stderr)
        return 1

    title_suffix = {
        "today": "heute", "24h": "letzte 24h",
        "7d": "letzte 7 Tage", "30d": "letzte 30 Tage", "all": "gesamt",
    }[args.range]

    tabs_html, sections_html, plots, _sess = build_report(df, default_tab=args.default)
    html = render_html(
        title_suffix, df, tabs_html, sections_html, plots,
        default_tab=args.default, start=start, end=end,
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else (
        REPORT_DIR / f"report_{args.range}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    )
    out_path.write_text(html, encoding="utf-8")
    # Zusaetzlich immer 'latest.html' schreiben (fester Pfad)
    latest_name = (CFG.get("report") or {}).get("report_filename", "latest.html")
    if latest_name:
        (REPORT_DIR / latest_name).write_text(html, encoding="utf-8")
    print(f"Report: {out_path}  ({out_path.stat().st_size // 1024} KB, {len(df)} samples)")

    if open_in_browser:
        webbrowser.open(out_path.resolve().as_uri())
    return 0


if __name__ == "__main__":
    sys.exit(main())
