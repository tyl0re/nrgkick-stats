"""
NRGkick Gen2 Logger
-------------------
Fragt die lokale JSON-API einer NRGkick-Wallbox periodisch ab und schreibt
die Messwerte in eine lokale SQLite-Datenbank.

- Endpunkte: /info, /control, /values  (HTTP, Port 80, optional Basic Auth)
- Scheduling: Endlos-Loop mit driftfreiem Sleep auf volle Minute
- Einstellungen kommen aus config.json (siehe nrgkick_config.py fuer Defaults)

Zum Aktivieren in der NRGkick App: Einstellungen -> Lokale API -> JSON API an.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import random
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from requests.auth import HTTPBasicAuth

from nrgkick_config import (
    APP_NAME,
    load_config as _load_cfg,
    db_path  as _db_path,
    log_path as _log_path,
)


# ---------------------------------------------------------------------------
# Logging wird erst nach Config-Laden konfiguriert (wegen Pfaden)
# ---------------------------------------------------------------------------
log: logging.Logger = logging.getLogger(APP_NAME)


def _setup_logging(cfg: dict) -> None:
    log.setLevel(logging.INFO)
    # doppelte Handler vermeiden (bei Reloads)
    for h in list(log.handlers):
        log.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    log_file = _log_path(cfg)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    fh = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=int(cfg["data"].get("log_max_bytes") or 2_000_000),
        backupCount=int(cfg["data"].get("log_backup_count") or 5),
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    log.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)


def load_config() -> dict[str, Any]:
    """Laedt die Config und initialisiert Logging. Prueft auch auf 'host'."""
    cfg = _load_cfg()
    _setup_logging(cfg)
    host = (cfg.get("connection") or {}).get("host")
    if not host or host == "192.168.1.100":
        log.error(
            "Config unvollstaendig: bitte 'connection.host' in %s setzen.",
            cfg.get("_config_file"),
        )
        sys.exit(2)
    return cfg


# ---------------------------------------------------------------------------
# NRGkick API Client
# ---------------------------------------------------------------------------

class NRGkickClient:
    def __init__(self, cfg: dict[str, Any]):
        conn = cfg.get("connection", {}) or {}
        scheme = "https" if conn.get("use_https") else "http"
        self.base_url = f"{scheme}://{conn['host']}"
        self.verify = bool(conn.get("verify_tls", True))
        self.timeout = float(conn.get("http_timeout", 10))
        self.auth = None
        if conn.get("username"):
            self.auth = HTTPBasicAuth(conn["username"], conn.get("password", ""))
        self.session = requests.Session()

    def _get(self, path: str) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, auth=self.auth, timeout=self.timeout, verify=self.verify)
        r.raise_for_status()
        data = r.json()
        # Falls API deaktiviert ist, liefert NRGkick so was:
        if isinstance(data, dict) and "Response" in data and len(data) == 1:
            raise RuntimeError(f"NRGkick antwortete: {data['Response']}")
        return data

    def get_values(self) -> dict[str, Any]:
        return self._get("/values")

    def get_info(self) -> dict[str, Any]:
        return self._get("/info")

    def get_control(self) -> dict[str, Any]:
        return self._get("/control")


# ---------------------------------------------------------------------------
# Datenbank
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    ts_utc              TEXT    NOT NULL,        -- ISO 8601, UTC
    ts_local            TEXT    NOT NULL,        -- ISO 8601, lokale Zeit

    -- Status (API liefert Strings: "CHARGING", "NO_ERROR", ...)
    charging_state      TEXT,
    charging_rate       REAL,                    -- kW laut API (general.charging_rate)
    connector_state     TEXT,                    -- relay_state, z.B. "N, L1, L2, L3"
    error_code          TEXT,
    warning_code        TEXT,

    -- Leistung / Energie
    power_w             REAL,                    -- aktuelle Wirkleistung gesamt (W)
    power_l1_w          REAL,
    power_l2_w          REAL,
    power_l3_w          REAL,
    current_l1_a        REAL,
    current_l2_a        REAL,
    current_l3_a        REAL,
    current_n_a         REAL,
    voltage_l1_v        REAL,
    voltage_l2_v        REAL,
    voltage_l3_v        REAL,
    frequency_hz        REAL,
    power_factor        REAL,

    energy_session_wh   REAL,                    -- aktuelle Ladesitzung
    energy_total_wh     REAL,                    -- Lebensdauerzaehler

    -- Temperaturen (°C)
    temp_housing         REAL,
    temp_connector_l1    REAL,
    temp_connector_l2    REAL,
    temp_connector_l3    REAL,
    temp_connector_n     REAL,
    temp_domestic_plug   REAL,       -- Mittelwert plug_1/plug_2 (legacy)
    temp_domestic_plug_1 REAL,
    temp_domestic_plug_2 REAL,

    -- Control
    set_current_a       REAL,
    charge_pause        INTEGER,                 -- 0/1
    energy_limit_wh     REAL,
    phase_count         INTEGER,

    -- Session-Kontext (aus /values.general)
    vehicle_connect_time  INTEGER,               -- Sekunden seit Fahrzeug eingesteckt
    vehicle_charging_time INTEGER,               -- Sekunden aktive Ladezeit
    charge_permitted      INTEGER,               -- 0/1

    raw_values_json     TEXT,
    raw_control_json    TEXT,
    PRIMARY KEY (ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_samples_local ON samples(ts_local);

CREATE TABLE IF NOT EXISTS device_info (
    ts_utc        TEXT PRIMARY KEY,
    serial_number TEXT,
    device_name   TEXT,
    model_type    TEXT,
    sw_version    TEXT,
    hw_version    TEXT,
    raw_info_json TEXT
);

-- Generische Key-Value-Tabelle: speichert JEDES Feld aus /values und /control
-- als einzelne Zeile. So koennen auch zukuenftige Firmware-Felder ohne
-- Code-Aenderungen historisch ausgewertet werden.
-- source: 'values' oder 'control'
-- path:   Punkt-separierter JSON-Pfad, z.B. 'powerflow.l1.active_power'
-- value_num / value_text : typisiert, genau eins ist NULL
CREATE TABLE IF NOT EXISTS samples_kv (
    ts_utc      TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    path        TEXT    NOT NULL,
    value_num   REAL,
    value_text  TEXT,
    PRIMARY KEY (ts_utc, source, path)
);
CREATE INDEX IF NOT EXISTS idx_kv_path    ON samples_kv(path);
CREATE INDEX IF NOT EXISTS idx_kv_ts_path ON samples_kv(ts_utc, path);

-- Code-Beschreibungen (werden vom Logger beim Start synchronisiert)
CREATE TABLE IF NOT EXISTS code_enums (
    kind        TEXT NOT NULL,     -- 'error' | 'warning' | 'status' | 'relay' | 'rcd' | 'connector_type'
    code        TEXT NOT NULL,     -- z.B. 'NO_ERROR', 'CHARGING', ...
    description TEXT,
    severity    TEXT,              -- 'ok' | 'info' | 'warn' | 'error'
    PRIMARY KEY (kind, code)
);
"""


# Lookup-Tabellen fuer NRGkick-Codes. Werden beim Start in die DB gespiegelt,
# damit man auch in SQL joinen kann und die Codes *historisch* dokumentiert sind,
# falls spaeter Firmware-Updates Codes umbenennen.
CODE_ENUMS: dict[str, list[tuple[str, str, str]]] = {
    "status": [
        ("UNKNOWN",             "unbekannt",                                     "info"),
        ("STANDBY",             "Bereit, kein Fahrzeug angesteckt",              "ok"),
        ("CONNECTED",           "Fahrzeug angesteckt, noch nicht ladend",        "info"),
        ("CHARGING",            "laedt aktiv",                                   "ok"),
        ("PAUSED",              "Ladung pausiert (durch Nutzer/App)",            "info"),
        ("SIGNALED",            "Ladung wird signalisiert (Anlaufphase)",        "info"),
        ("WAITING",             "wartet (Fahrzeug hat noch nicht quittiert)",    "info"),
        ("ERROR",               "Fehlerzustand - Laden nicht moeglich",          "error"),
        ("FINISHED",            "Ladung beendet (Fahrzeug voll/Limit)",          "ok"),
    ],
    "error": [
        ("NO_ERROR",                  "kein Fehler",                                      "ok"),
        ("RCD_FAULT",                 "Fehlerstromschutzschalter hat ausgeloest",         "error"),
        ("CABLE_OVERCURRENT",         "Ueberstrom am Ladekabel",                          "error"),
        ("CABLE_OVERTEMPERATURE",     "Ladekabel ueberhitzt",                             "error"),
        ("CONNECTOR_OVERTEMPERATURE", "Stecker ueberhitzt (thermische Abschaltung)",      "error"),
        ("HOUSING_OVERTEMPERATURE",   "Gehaeuse ueberhitzt",                              "error"),
        ("PROXIMITY_FAULT",           "Proximity-Kontakt (Kabelerkennung) fehlerhaft",    "error"),
        ("PILOT_FAULT",               "Pilotsignal (Ladekommunikation) fehlerhaft",       "error"),
        ("RELAY_WELDED",              "Relais verschweisst",                              "error"),
        ("FI_TEST_FAILED",            "interner FI-Test fehlgeschlagen",                  "error"),
        ("GROUND_FAULT",              "Fehler am Schutzleiter",                           "error"),
        ("VOLTAGE_TOO_LOW",           "Netzspannung zu niedrig",                          "error"),
        ("VOLTAGE_TOO_HIGH",          "Netzspannung zu hoch",                             "error"),
        ("PHASE_LOSS",                "Phasenausfall",                                    "error"),
        ("UNBALANCED_LOAD",           "unsymmetrische Phasenbelastung",                   "warn"),
        ("FIRMWARE_ERROR",            "Firmware-Fehler",                                  "error"),
        ("COMMUNICATION_ERROR",       "Kommunikationsfehler",                             "error"),
        ("VEHICLE_COMM_ERROR",        "Kommunikationsfehler mit dem Fahrzeug",            "error"),
        ("EMERGENCY_STOP",            "Notabschaltung ausgeloest",                        "error"),
    ],
    "warning": [
        ("NO_WARNING",                 "keine Warnung",                                         "ok"),
        ("TEMPERATURE_HIGH",           "Temperatur hoch",                                       "warn"),
        ("CONNECTOR_TEMPERATURE_HIGH", "Stecker-Temperatur hoch (nahe Derating-Grenze)",        "warn"),
        ("HOUSING_TEMPERATURE_HIGH",   "Gehaeuse-Temperatur hoch",                              "warn"),
        ("CABLE_TEMPERATURE_HIGH",     "Kabel-Temperatur hoch",                                 "warn"),
        ("DERATED_DUE_TO_TEMPERATURE", "Ladestrom wegen Temperatur reduziert",                  "warn"),
        ("VOLTAGE_DROP",               "Spannungseinbruch erkannt",                             "warn"),
        ("CURRENT_IMBALANCE",          "Stromasymmetrie zwischen Phasen",                       "warn"),
        ("VEHICLE_REQUESTS_LESS",      "Fahrzeug fordert weniger Strom als vorgegeben",         "info"),
        ("LIMITED_BY_CONFIG",          "durch Konfiguration begrenzt",                          "info"),
        ("SIGNAL_WEAK",                "schwaches Funksignal (WLAN/RSSI)",                      "warn"),
    ],
    "relay": [
        ("-",               "alle Phasen getrennt",              "ok"),
        ("NO_RELAY",        "kein Relais geschaltet (Standby)",  "ok"),
        ("L1",              "nur L1 geschaltet",                 "info"),
        ("L1, L2",          "L1 und L2 geschaltet",              "info"),
        ("L1, L2, L3",      "alle drei Phasen geschaltet",       "info"),
        ("N",               "nur Neutralleiter geschaltet",      "info"),
        ("N, L1",           "N + L1 geschaltet",                 "info"),
        ("N, L1, L2",       "N + L1 + L2 geschaltet",            "info"),
        ("N, L1, L2, L3",   "alle Leiter geschaltet (Vollload)", "ok"),
    ],
    "rcd": [
        ("NO_FAULT",        "kein FI-Fehler",                    "ok"),
        ("FAULT_DETECTED",  "FI-Fehlerstrom erkannt",            "error"),
        ("TEST_FAILED",     "FI-Selbsttest fehlgeschlagen",      "error"),
    ],
    "connector_type": [
        ("DOMESTIC",        "Schuko-Adapter (1-phasig, 16 A max.)",  "info"),
        ("CEE",             "CEE-Adapter (rot/blau, 16-32 A)",       "info"),
        ("CEE16",           "CEE-Stecker 16 A",                      "info"),
        ("CEE32",           "CEE-Stecker 32 A",                      "info"),
        ("SWISS",           "Schweizer Haushaltssteckdose",          "info"),
    ],
}


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Fuegt fehlende Spalten zu bestehender samples-Tabelle hinzu."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(samples)").fetchall()}
    additions = [
        ("vehicle_connect_time",  "INTEGER"),
        ("vehicle_charging_time", "INTEGER"),
        ("charge_permitted",      "INTEGER"),
        ("temp_domestic_plug_1",  "REAL"),
        ("temp_domestic_plug_2",  "REAL"),
    ]
    for name, typ in additions:
        if name not in existing:
            conn.execute(f"ALTER TABLE samples ADD COLUMN {name} {typ}")
    conn.commit()


def _sync_code_enums(conn: sqlite3.Connection) -> None:
    """Spiegelt das CODE_ENUMS-Dict in die Tabelle code_enums.
    So ist die Code-Dokumentation IN der DB und waechst mit jedem Start mit."""
    rows = [
        (kind, code, description, severity)
        for kind, items in CODE_ENUMS.items()
        for (code, description, severity) in items
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO code_enums (kind, code, description, severity) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def open_db(cfg: dict[str, Any]) -> sqlite3.Connection:
    db_file = _db_path(cfg)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA)
    _migrate_schema(conn)
    _sync_code_enums(conn)
    return conn


# ---------------------------------------------------------------------------
# JSON -> flache Key-Value-Paare
# ---------------------------------------------------------------------------

def _flatten(obj: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    """Rekursives Flatten: nested dict/list -> Pfad.zu.feld = wert.
    Listen werden indiziert (z.B. 'values[0]')."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            sub = f"{prefix}.{k}" if prefix else str(k)
            yield from _flatten(v, sub)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            sub = f"{prefix}[{i}]"
            yield from _flatten(v, sub)
    else:
        yield prefix, obj


def kv_rows(ts_utc: str, values: dict[str, Any], control: dict[str, Any]
            ) -> list[tuple[str, str, str, float | None, str | None]]:
    """Liefert (ts_utc, source, path, value_num, value_text)-Tupel fuer jedes
    einzelne Blatt in values und control."""
    out: list[tuple[str, str, str, float | None, str | None]] = []
    for source, data in (("values", values), ("control", control)):
        if not isinstance(data, dict):
            continue
        for path, raw in _flatten(data):
            if raw is None:
                continue
            vnum: float | None = None
            vtext: str | None = None
            if isinstance(raw, bool):
                vnum = 1.0 if raw else 0.0
            elif isinstance(raw, (int, float)):
                vnum = float(raw)
            else:
                vtext = str(raw)
            out.append((ts_utc, source, path, vnum, vtext))
    return out


# ---------------------------------------------------------------------------
# Mapping Helpers
# ---------------------------------------------------------------------------

def g(d: Any, *path: str, default=None):
    """Sicherer dict.get ueber einen Pfad."""
    cur = d
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def _num(v: Any):
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        return float(v)
    except (TypeError, ValueError):
        return None


def extract_sample(values: dict[str, Any], control: dict[str, Any]) -> dict[str, Any]:
    """Extrahiert ein flaches Sample aus /values und /control.

    Mapping an die tatsaechliche NRGkick Gen2 JSON-API (Firmware 4.x):
      values.general.{status, charging_rate, error_code, warning_code, ...}
      values.powerflow.{total_active_power, grid_frequency, total_power_factor, l1/l2/l3/n.*}
      values.energy.{charged_energy, total_charged_energy}
      values.temperatures.{housing, connector_l1/l2/l3, domestic_plug_1/2}
      control.{current_set, charge_pause, energy_limit, phase_count}
    """

    # ---- status (in "general") ---------------------------------------------
    gen = values.get("general") or {}
    charging_state  = gen.get("status")          # String, z.B. "CHARGING"
    charging_rate   = _num(gen.get("charging_rate"))
    connector_state = gen.get("relay_state")
    error_code      = gen.get("error_code")      # String, z.B. "NO_ERROR"
    warning_code    = gen.get("warning_code")
    vehicle_ct      = gen.get("vehicle_connect_time")
    vehicle_cht     = gen.get("vehicle_charging_time")
    charge_permit   = gen.get("charge_permitted")
    # int-cast (kann None sein)
    vehicle_ct      = int(vehicle_ct)    if isinstance(vehicle_ct,    (int, float)) else None
    vehicle_cht     = int(vehicle_cht)   if isinstance(vehicle_cht,   (int, float)) else None
    charge_permit   = int(charge_permit) if isinstance(charge_permit, (int, float)) else None

    # ---- powerflow ---------------------------------------------------------
    pf = values.get("powerflow") or {}
    power_w       = _num(pf.get("total_active_power"))
    frequency_hz  = _num(pf.get("grid_frequency"))
    power_factor  = _num(pf.get("total_power_factor"))

    def ph(key: str, sub: str):
        return _num(g(pf, key, sub))

    current_l1 = ph("l1", "current")
    current_l2 = ph("l2", "current")
    current_l3 = ph("l3", "current")
    voltage_l1 = ph("l1", "voltage")
    voltage_l2 = ph("l2", "voltage")
    voltage_l3 = ph("l3", "voltage")
    power_l1   = ph("l1", "active_power")
    power_l2   = ph("l2", "active_power")
    power_l3   = ph("l3", "active_power")
    current_n  = ph("n",  "current")

    # ---- energie -----------------------------------------------------------
    energy_session = _num(g(values, "energy", "charged_energy"))
    energy_total   = _num(g(values, "energy", "total_charged_energy"))

    # ---- temperaturen ------------------------------------------------------
    t = values.get("temperatures") or {}
    temp_housing = _num(t.get("housing"))
    temp_con_l1  = _num(t.get("connector_l1"))
    temp_con_l2  = _num(t.get("connector_l2"))
    temp_con_l3  = _num(t.get("connector_l3"))
    temp_con_n   = _num(t.get("connector_n"))
    # "domestic_plug_1" und "_2" einzeln + Mittelwert (legacy)
    dp1 = _num(t.get("domestic_plug_1"))
    dp2 = _num(t.get("domestic_plug_2"))
    if dp1 is not None and dp2 is not None:
        temp_plug = (dp1 + dp2) / 2
    else:
        temp_plug = dp1 if dp1 is not None else dp2
    if temp_plug is None:
        temp_plug = _num(t.get("domestic_plug"))

    # ---- control -----------------------------------------------------------
    set_current_a   = _num(control.get("current_set") or control.get("set_current"))
    charge_pause    = control.get("charge_pause")
    if isinstance(charge_pause, bool):
        charge_pause = int(charge_pause)
    energy_limit_wh = _num(control.get("energy_limit"))
    phase_count     = control.get("phase_count")

    now = datetime.now(timezone.utc)
    return {
        "ts_utc":   now.isoformat(timespec="seconds"),
        "ts_local": now.astimezone().isoformat(timespec="seconds"),
        "charging_state":  charging_state,
        "charging_rate":   charging_rate,
        "connector_state": connector_state,
        "error_code":      error_code,
        "warning_code":    warning_code,
        "power_w":     power_w,
        "power_l1_w":  power_l1,
        "power_l2_w":  power_l2,
        "power_l3_w":  power_l3,
        "current_l1_a": current_l1,
        "current_l2_a": current_l2,
        "current_l3_a": current_l3,
        "current_n_a":  current_n,
        "voltage_l1_v": voltage_l1,
        "voltage_l2_v": voltage_l2,
        "voltage_l3_v": voltage_l3,
        "frequency_hz": frequency_hz,
        "power_factor": power_factor,
        "energy_session_wh": energy_session,
        "energy_total_wh":   energy_total,
        "temp_housing":         temp_housing,
        "temp_connector_l1":    temp_con_l1,
        "temp_connector_l2":    temp_con_l2,
        "temp_connector_l3":    temp_con_l3,
        "temp_connector_n":     temp_con_n,
        "temp_domestic_plug":   temp_plug,
        "temp_domestic_plug_1": dp1,
        "temp_domestic_plug_2": dp2,
        "set_current_a":   set_current_a,
        "charge_pause":    charge_pause,
        "energy_limit_wh": energy_limit_wh,
        "phase_count":     phase_count,
        "vehicle_connect_time":  vehicle_ct,
        "vehicle_charging_time": vehicle_cht,
        "charge_permitted":      charge_permit,
    }


def insert_sample(
    conn: sqlite3.Connection,
    sample: dict[str, Any],
    values_raw: dict[str, Any],
    control_raw: dict[str, Any],
    store_raw: bool,
) -> None:
    sample = dict(sample)
    sample["raw_values_json"]  = json.dumps(values_raw,  ensure_ascii=False) if store_raw else None
    sample["raw_control_json"] = json.dumps(control_raw, ensure_ascii=False) if store_raw else None

    cols = list(sample.keys())
    placeholders = ",".join(f":{c}" for c in cols)
    sql = f"INSERT OR REPLACE INTO samples ({','.join(cols)}) VALUES ({placeholders})"
    conn.execute(sql, sample)

    # Alle API-Felder zusaetzlich 1:1 in die KV-Tabelle. Dank INSERT OR REPLACE
    # ist das idempotent (erneutes Einlesen ueberschreibt).
    ts = sample.get("ts_utc")
    if ts:
        rows = kv_rows(ts, values_raw, control_raw)
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO samples_kv "
                "(ts_utc, source, path, value_num, value_text) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
    conn.commit()


def maybe_insert_device_info(conn: sqlite3.Connection, info: dict[str, Any]) -> None:
    serial = g(info, "general", "serial_number") or info.get("serial_number")
    existing = conn.execute(
        "SELECT serial_number FROM device_info ORDER BY ts_utc DESC LIMIT 1"
    ).fetchone()
    if existing and existing[0] == serial:
        return

    versions = info.get("versions") or {}
    sw_version = versions.get("sw_sm") or versions.get("sw_fw") or g(info, "general", "sw_version")
    hw_version = versions.get("hw_sm") or versions.get("hw")    or g(info, "general", "hw_version")
    row = {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "serial_number": serial,
        "device_name":   g(info, "general", "device_name"),
        "model_type":    g(info, "general", "model_type") or g(info, "general", "model"),
        "sw_version":    sw_version,
        "hw_version":    hw_version,
        "raw_info_json": json.dumps(info, ensure_ascii=False),
    }
    conn.execute(
        """INSERT OR REPLACE INTO device_info
           (ts_utc, serial_number, device_name, model_type, sw_version, hw_version, raw_info_json)
           VALUES (:ts_utc, :serial_number, :device_name, :model_type, :sw_version, :hw_version, :raw_info_json)""",
        row,
    )
    conn.commit()
    log.info("device_info aktualisiert: serial=%s sw=%s", serial, row["sw_version"])


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

_running = True


def _stop(signum, _frame):
    global _running
    log.info("Signal %s empfangen, beende...", signum)
    _running = False


def poll_once(client: NRGkickClient, conn: sqlite3.Connection, store_raw: bool) -> None:
    try:
        values  = client.get_values()
        control = client.get_control()
    except Exception as e:
        log.error("Abruf fehlgeschlagen: %s", e)
        return

    try:
        sample = extract_sample(values, control)
        insert_sample(conn, sample, values, control, store_raw)
        log.info(
            "OK  P=%.0fW  I_set=%s  state=%s  E_sess=%s",
            sample.get("power_w") or 0,
            sample.get("set_current_a"),
            sample.get("charging_state"),
            sample.get("energy_session_wh"),
        )
    except Exception as e:
        log.exception("Verarbeitung fehlgeschlagen: %s", e)


def sleep_until_next_tick(interval: int, jitter_seconds: int = 0) -> None:
    """Schlaeft bis zum naechsten vollen interval-Zeitpunkt (driftfrei).
    Optional mit Zufalls-Jitter (+/- jitter_seconds), falls mehrere Instanzen
    parallel laufen und die Wallbox nicht zeitgleich bombardiert werden soll."""
    now = time.time()
    next_tick = (int(now) // interval + 1) * interval
    if jitter_seconds > 0:
        next_tick += random.randint(-jitter_seconds, jitter_seconds)
    remaining = max(0.0, next_tick - now)
    end = time.time() + remaining
    while _running and time.time() < end:
        time.sleep(min(0.5, max(0.05, end - time.time())))


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    cfg = load_config()
    polling = cfg.get("polling", {}) or {}
    interval = int(polling.get("interval_seconds", 360))
    jitter   = max(0, int(polling.get("jitter_seconds", 0)))
    info_refresh_minutes = max(1, int(polling.get("info_refresh_minutes", 60)))
    poll_on_start = bool(polling.get("poll_on_start", True))
    store_raw     = bool((cfg.get("data") or {}).get("store_raw_json", True))

    db_file = _db_path(cfg)
    host = cfg["connection"]["host"]
    log.info("Starte %s  host=%s  interval=%ds  db=%s",
             APP_NAME, host, interval, db_file)

    client = NRGkickClient(cfg)
    conn = open_db(cfg)

    # Einmal /info lesen und ablegen (und bei Aenderungen erneut)
    try:
        info = client.get_info()
        maybe_insert_device_info(conn, info)
    except Exception as e:
        log.warning("get_info() fehlgeschlagen (weiter mit /values): %s", e)

    if poll_on_start:
        poll_once(client, conn, store_raw)

    info_refresh_every = max(1, (info_refresh_minutes * 60) // interval)
    counter = 0
    while _running:
        sleep_until_next_tick(interval, jitter)
        if not _running:
            break
        counter += 1
        poll_once(client, conn, store_raw)
        if counter % info_refresh_every == 0:
            try:
                info = client.get_info()
                maybe_insert_device_info(conn, info)
            except Exception as e:
                log.debug("get_info() refresh fehlgeschlagen: %s", e)

    conn.close()
    log.info("beendet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
