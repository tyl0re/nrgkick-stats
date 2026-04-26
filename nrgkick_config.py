"""
Zentrale Konfiguration fuer NRGkick Logger + Stats.

Die Defaults sind so gewaehlt, dass das Tool ohne viel Aufwand funktioniert.
Nutzer ueberschreiben Einzelwerte in ihrer `config.json`; unangegebene
Einstellungen fallen auf die Defaults zurueck.

Standard-Speicherort der User-Config:
    %LOCALAPPDATA%\\NRGkickLogger\\config.json   (Windows)
    ~/.local/share/nrgkick-logger/config.json    (Linux/Mac)

Pfad-Platzhalter in data_dir:
    ${LOCALAPPDATA}, ${HOME}, ${APPNAME}  -> werden zur Laufzeit ersetzt.
"""

from __future__ import annotations

import copy
import json
import os
import string
import sys
from pathlib import Path
from typing import Any


APP_NAME = "NRGkickLogger"


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULTS: dict[str, Any] = {
    "connection": {
        "host":          "192.168.1.100",
        "username":      "",
        "password":      "",
        "use_https":     False,
        "verify_tls":    True,
        "http_timeout":  10,
    },
    "polling": {
        "interval_seconds":      360,    # 6 Minuten
        "jitter_seconds":        0,      # 0 = exakt auf die Minute ausrichten
        "info_refresh_minutes":  60,     # /info jede Stunde neu abfragen
        "poll_on_start":         True,   # direkt beim Start einmal pollen
    },
    "data": {
        "data_dir":        "${LOCALAPPDATA}/${APPNAME}",
        "db_filename":     "nrgkick.db",
        "log_filename":    "nrgkick.log",
        "reports_dir":     "reports",          # relativ zu data_dir
        "store_raw_json":  True,               # raw_values_json, raw_control_json in 'samples'
        "store_kv":        True,               # Flache KV-Tabelle mit allen Feldern
        "keep_kv_days":    0,                  # 0 = nie loeschen; >0 = aelter als X Tage aufraeumen
        "log_max_bytes":   2_000_000,
        "log_backup_count": 5,
    },
    "thresholds": {
        "temperature_cool":      40.0,   # unter diesem Wert: gruen
        "temperature_warm":      60.0,   # zwischen cool..warm: gelb
        "temperature_hot":       75.0,   # zwischen warm..hot: orange
        "temperature_critical":  90.0,   # darueber: rot
        "standby_power_w":       50.0,   # < X W zaehlt nicht als Ladung
        "session_gap_minutes":   15.0,   # Messluecke > X min = neue Session
    },
    "derating": {
        "min_delta_a":           1.0,    # Strom-Aenderung >= X A gilt als Event
        "window_minutes":        2.0,    # Fenster zur Temp-Analyse (+/- X min)
        "temp_quantile":         0.75,   # Schwelle = Session-Quantil
        "recovery_cooldown_c":   3.0,    # °C unter Derating-Peak = Recovery
    },
    "costs": {
        "electricity_price_eur_per_kwh": 0.265,  # default cost per kWh (EUR)
        "co2_g_per_kwh":                 None,  # z.B. 380 fuer CO2-Schaetzung
    },
    "report": {
        "default_range":   "all",        # today | 24h | 7d | 30d | all
        "default_tab":     "dashboard",
        "auto_open":       True,
        "report_filename": "latest.html",  # feste Datei + zusaetzlich Zeitstempel
        "keep_history":    True,           # alte Reports behalten
    },
    "ui": {
        "locale":          "de",
        "timezone":        None,           # None = lokale TZ des Systems
        "heatmap_colorscale": [
            [0.00, "#2c3e8f"],
            [0.25, "#3b9cff"],
            [0.50, "#2ca02c"],
            [0.75, "#ff7f0e"],
            [1.00, "#d62728"],
        ],
    },
    "service": {
        "service_name":   "NRGkickLogger",
        "display_name":   "NRGkick Logger",
        "description":    "Zeichnet NRGkick-Daten periodisch in eine SQLite-DB auf.",
        "nssm_path":      "",            # leer = tools/nssm.exe im Projektverzeichnis
        "nssm_download":  "https://nssm.cc/release/nssm-2.24.zip",
    },
}


# Alte Flat-Config (nur 'host', 'username', ... auf oberster Ebene) wird nach
# DEFAULTS[.connection.*] migriert, damit Bestandskonfigs weiter funktionieren.
LEGACY_TOP_LEVEL_KEYS = {
    "host":             ("connection", "host"),
    "username":         ("connection", "username"),
    "password":         ("connection", "password"),
    "use_https":        ("connection", "use_https"),
    "verify_tls":       ("connection", "verify_tls"),
    "interval_seconds": ("polling",    "interval_seconds"),
    "store_raw_json":   ("data",       "store_raw_json"),
}


# ---------------------------------------------------------------------------
# Pfad-Aufloesung
# ---------------------------------------------------------------------------

def _expand_vars(path: str) -> str:
    """${LOCALAPPDATA} / ${HOME} / ${APPNAME} in Pfad ersetzen."""
    mapping = {
        "LOCALAPPDATA": os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local"),
        "HOME":         str(Path.home()),
        "APPNAME":      APP_NAME,
    }
    return string.Template(path).safe_substitute(mapping)


def _resolve_data_dir(data_dir_template: str) -> Path:
    return Path(_expand_vars(data_dir_template))


def default_data_dir() -> Path:
    """Public: Wo liegen Config/DB/Logs by default."""
    env_dir = os.environ.get("NRGKICK_DATA_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    return _resolve_data_dir(DEFAULTS["data"]["data_dir"])


def locate_config_file(explicit: str | None = None) -> Path:
    """Bestimmt den Pfad zur config.json.
    Priorisierung:
      1) explizit uebergebener Pfad
      2) Env-Var NRGKICK_CONFIG
      3) Datenordner/config.json (default_data_dir)
    """
    if explicit:
        return Path(explicit).expanduser().resolve()
    env = os.environ.get("NRGKICK_CONFIG")
    if env:
        return Path(env).expanduser().resolve()
    return default_data_dir() / "config.json"


# ---------------------------------------------------------------------------
# Laden / Mergen
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _migrate_legacy(user_cfg: dict) -> dict:
    """Flach geschriebene alte Keys ("host", "interval_seconds", ...) werden in
    die neue verschachtelte Struktur uebertragen (ohne die Originale zu
    veraendern). So laeuft auch eine aeltere config.json ohne Anpassung."""
    out = copy.deepcopy(user_cfg)
    for key, (section, sub) in LEGACY_TOP_LEVEL_KEYS.items():
        if key in out and not isinstance(out[key], dict):
            out.setdefault(section, {})
            out[section].setdefault(sub, out[key])
            del out[key]
    return out


def write_example_config(path: Path) -> None:
    """Schreibt eine kommentierte Beispielkonfig (pures JSON, daher nur
    "_help"-Schluessel als Kommentar-Ersatz)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(DEFAULTS, f, indent=2, ensure_ascii=False)


def load_config(explicit_path: str | None = None,
                *, strict: bool = False) -> dict:
    """Laedt die User-Config und merged sie auf DEFAULTS.
    Erstellt die Config-Datei mit Default-Werten, wenn sie noch nicht existiert.

    Returns:
        Vollstaendige Konfiguration (alle Sections immer vorhanden).
    """
    cfg_path = locate_config_file(explicit_path)

    if not cfg_path.exists():
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        with cfg_path.open("w", encoding="utf-8") as f:
            json.dump(DEFAULTS, f, indent=2, ensure_ascii=False)
        if strict:
            raise SystemExit(
                f"Config wurde neu angelegt unter {cfg_path}. "
                "Bitte host (und ggf. Auth) eintragen und erneut starten."
            )
        return copy.deepcopy(DEFAULTS)

    with cfg_path.open("r", encoding="utf-8") as f:
        user_cfg = json.load(f)

    user_cfg = _migrate_legacy(user_cfg)
    merged = _deep_merge(DEFAULTS, user_cfg)

    # Pfade aufloesen
    merged["_config_file"] = str(cfg_path)
    env_dir = os.environ.get("NRGKICK_DATA_DIR")
    merged["_data_dir"] = str(
        Path(env_dir).expanduser().resolve()
        if env_dir else _resolve_data_dir(merged["data"]["data_dir"])
    )
    return merged


def data_dir_from(cfg: dict) -> Path:
    return Path(cfg.get("_data_dir") or _resolve_data_dir(cfg["data"]["data_dir"]))


def db_path(cfg: dict) -> Path:
    return data_dir_from(cfg) / cfg["data"]["db_filename"]


def log_path(cfg: dict) -> Path:
    return data_dir_from(cfg) / cfg["data"]["log_filename"]


def reports_dir(cfg: dict) -> Path:
    rp = cfg["data"]["reports_dir"]
    p = Path(rp)
    if p.is_absolute():
        return p
    return data_dir_from(cfg) / rp


# ---------------------------------------------------------------------------
# CLI (zum Testen)
# ---------------------------------------------------------------------------

def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(
        description="Zeigt / verwaltet die NRGkick-Config.",
    )
    ap.add_argument("action", nargs="?", default="show",
                    choices=["show", "path", "init", "example"],
                    help="show = aktuelle Config ausgeben, "
                         "path = Pfad zur Config-Datei, "
                         "init = Config-Datei anlegen (wenn nicht vorhanden), "
                         "example = Defaults als JSON ausgeben")
    ap.add_argument("--config", help="alternativer Pfad zur Config-Datei")
    args = ap.parse_args()

    if args.action == "path":
        print(locate_config_file(args.config))
        return 0
    if args.action == "example":
        json.dump(DEFAULTS, sys.stdout, indent=2, ensure_ascii=False)
        print()
        return 0
    if args.action == "init":
        path = locate_config_file(args.config)
        if path.exists():
            print(f"Config existiert bereits: {path}")
            return 0
        write_example_config(path)
        print(f"Config angelegt: {path}")
        return 0
    # show
    cfg = load_config(args.config)
    json.dump(cfg, sys.stdout, indent=2, ensure_ascii=False, default=str)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
