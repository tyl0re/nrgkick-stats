# NRGkick Logger – Agenten-Handbuch

**Was ist das?**  
Windows-Logger + HTML-Analyse für **NRGkick Gen2**-Wallboxen. Python 3.10+, SQLite, Plotly.

## Sofortiges Verständnis

| Datei | Zweck |
|---|---|
| `nrgkick_logger.py` | Hauptprogramm: Pollt alle 6 Min (default), schreibt DB |
| `nrgkick_stats.py` | Erzeugt interaktiven HTML-Report mit 11 Tabs |
| `nrgkick_config.py` | Zentrale Konfiguration (Deep-Merge, Pfad-Templates) |
| `service.ps1` | PowerShell-Control für NSSM-Dienst (install/start/stop/status/logs) |

**Datenbank:** `%LOCALAPPDATA%\NRGkickLogger\nrgkick.db`  
**Config:** `%LOCALAPPDATA%\NRGkickLogger\config.json`  
**Reports:** `%LOCALAPPTAITATA%\NRGkickLogger\reports\latest.html`

## Wichtige Commands (nicht raten!)

```powershell
# Dienst-Status (ohne Admin)
.\service.ps1 status

# Live-Logs tailen (ohne Admin)
.\service.ps1 logs

# Report erzeugen
.\stats.bat  # Doppelklick oder
python nrgkick_stats.py --range 7d --default current

# Dienst installieren/deinstallieren (Admin nötig, Script stuft sich hoch)
.\service.ps1 install
.\service.ps1 uninstall
```

## Konfiguration (config.json)

- **`connection.host`** → IP der Wallbox (**muss gesetzt werden!**)
- **`polling.interval_seconds`** → 360 default (6 Min)
- **`data.data_dir`** → `%LOCALAPPDATA%\NRGkickLogger` standardmäßig
- **Template-Variablen:** `${LOCALAPPDATA}`, `${HOME}`, `${APPNAME}`

Mehrere Wallboxen: Config mit anderem `service.service_name` und per `-ConfigFile` übergeben.

## Datenbank-Schema (wichtig für Queries)

| Tabelle | Inhalt |
|---|---|
| `samples` | Zeitreihe aller Messpunkte |
| `samples_kv` | Alle API-Felder als Key-Value-Paare (zukunftssicher!) |
| `device_info` | Gerätedaten aus `/info` (nur bei Änderung) |
| `code_enums` | Klartext für Error/Warning/Status-Codes |

**Query-Tipp:** Neue API-Felder automatisch finden:  
```sql
SELECT DISTINCT path FROM samples_kv ORDER BY path;
```

## Session-Erkennung (wichtig!)

- **Startzeitpunkt:** Erster `CHARGING` im Log oder `energy_session_wh`-Reset
- **Grenzen:** Lücken > 15 Min trennen Sessions (`thresholds.session_gap_minutes`)
- **Problem:** Wenn Logger inaktiv war, fehlt der exakte Start – Code sucht nach Energie-Sprüngen

## Typische Fehlerquellen

| Symptom | Ursache | Lösung |
|---|---|---|
| `"Config unvollstaendig"` | `connection.host` nicht gesetzt | IP der Wallbox eintragen |
| **"API must be enabled"** | Lokale API in App nicht aktiviert | In NRGkick-App aktivieren |
| **401/403** | Username/Passwort falsch | In Config anpassen oder App prüfen |
| **Timeouts** | Netzwerk-Firewall, falsche IP | `Test-NetConnection <ip> -Port 80` |

## Code-Qualität

```powershell
# Linting (ruff)
python -m ruff check nrgkick_stats.py nrgkick_logger.py nrgkick_config.py

# Typcheck (mypy)
python -m mypy nrgkick_stats.py nrgkick_logger.py nrgkick_config.py
```

## Architektur-Notizen

- **`_cfg_get()`** → Zentrale Config-Zugriffe mit Deep-Merge und Default-Werten
- **`_report_tzinfo()`** → Lokale Zeitzone (meist CET/CEST) für Reports
- **Plotly-Daten:** Alle Zeitreihen als `dict` mit `x=ts_utc`, `y=value_num`
- **HTML-Tabs:** Per JS-Dropdown umschaltbar (`analysis-session.hidden`)

## Testing/Tests

Kein automatisiertes Test-Setup. Manuelle Tests:
1. Logger laufen lassen, DB prüfen
2. Report erzeugen und auf Plotly-Interaktivität testen
3. Dienst neu starten (UAC-Prompt bestätigen)
