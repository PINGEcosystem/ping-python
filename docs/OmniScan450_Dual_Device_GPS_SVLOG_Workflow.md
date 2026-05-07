# Dual OmniScan 450 + Network GPS + SVLOG Workflow

This runbook is for recording from two Cerulean OmniScan 450 devices with a networked GPS source, saving `.svlog` files, and processing those files with PINGVerter/PINGMapper.

It is based on:
- `ping-python/examples/omniscan450Example.py`
- `ping-python/generate/templates/omniscan450.py.in`
- `PINGVerter/pingverter/cerulean_class.py`
- `PINGVerter/pingverter/converter.py`
- `PINGMapper/pingmapper/main_readFiles.py`

## 1) High-level architecture

Use one `Omniscan450` object per physical sonar and write both streams into one shared `.svlog` file.

Recommended layout:
- Sonar A (port-facing): UDP/TCP endpoint A -> shared writer
- Sonar B (starboard-facing): UDP/TCP endpoint B -> shared writer
- Shared writer output: `line001_YYYY-MM-DD-HH-MM.svlog`
- GPS source (networked): feeds navigation to both sonars (preferred), or to your logger (advanced)

Why shared log:
- It keeps both sonar streams in one file for one-click downstream ingest.
- The new `dualOmniscan450Logger.py` writes all packets from both devices (sonar + nav) to one file with a thread-safe writer.
- Post-run QA is simpler because packet counts are summarized in one place.

## 2) Important behavior to know

### What gets logged in `.svlog`

In `Omniscan450`, `wait_message(...)` writes incoming packets when logging is enabled.

Typical packets used downstream:
- Sonar profile packet (`packet_id` 2198 in parser logic)
- JSON wrapper/navigation packet (`packet_id` 150 in parser logic)

PINGVerter (`cerulean_class.py`) interpolates nav fields onto pings if those nav packets contain values like:
- `time_boot_ms`
- `lat`, `lon`
- `hdg`, `alt` (if available)

If no valid `lat/lon` are present, conversion continues in sonar-only mode (non-georeferenced).

### Logging controls available in ping-python

The generated OmniScan class supports:
- `Omniscan450(logging=True, log_directory=...)`
- `start_logging(...)`
- `stop_logging()`
- `new_log(...)`

The example `omniscan450Example.py` already demonstrates basic connect/init/ping/log flow.

## 3) Prerequisites checklist

- Two OmniScan 450 units reachable via IP and port.
- Distinct endpoints for each unit.
- GPS feed available on the survey network.
- Confirm each OmniScan stream includes nav JSON packets before long surveys.
- Python environment with:
  - `bluerobotics-ping` (or local `brping` generated from source)
  - `pingverter`
  - `pingmapper`

If using this repository from `master`, generate protocol/device files first:

```bash
cd ping-python
pip install jinja2
python generate/generate-python.py --output-dir=brping
python -c "import brping"
```

## 4) Acquisition workflow (field ops)

1. Bench test each sonar independently with `omniscan450Example.py`.
2. Verify ping data arrives and range/gain settings are sane.
3. Start GPS source and verify nav appears in sonar stream.
4. Start dual logger script (below), one thread per sonar.
5. Record line(s), then cleanly disable pinging on both devices.
6. Confirm the shared `.svlog` is non-trivial size and readable.
7. Convert the shared `.svlog` with PINGVerter/PINGMapper.

## 5) Dual logger script

If you are using this repository directly, a ready-to-run example is now available at:

- `ping-python/examples/dualOmniscan450Logger.py`

Example launch:

```bash
python examples/dualOmniscan450Logger.py \
    --port-endpoint 192.168.2.92:51200 \
    --star-endpoint 192.168.2.93:51200 \
    --port-protocol udp \
    --star-protocol udp \
    --line-name line001 \
    --log-root logs/omniscan_dual \
    --start-mm 0 \
    --length-mm 5000
```

By default this script creates one shared `.svlog` under:

- `logs/omniscan_dual/<line-name>/<line-name>_<timestamp>.svlog`

## 6) GPS integration options

### Preferred (recommended)

Feed network GPS to each OmniScan device so nav packets are embedded in the shared `.svlog`.

Benefits:
- No custom packet writing code.
- Matches PINGVerter's expected Cerulean nav parsing path.

### Advanced (custom)

If sonar streams do not include nav packets, you can inject your own JSON wrapper packets into logs, but you must match Cerulean packet schema expected by `PINGVerter/pingverter/cerulean_class.py`.

Only use this if you are ready to validate packet structure carefully.

## 7) Convert `.svlog` -> PINGMapper inputs

### With PINGVerter directly

```python
from pingverter import cerul2pingmapper

in_file = r"Z:\survey\line001\line001_2026-05-07-14-30.svlog"
out_dir = r"Z:\survey\processed\line001"
obj = cerul2pingmapper(in_file, out_dir, nchunk=500, tempC=10)
print(obj.metaDir)
```

### With PINGMapper programmatic API

`PINGMapper` accepts `.svlog` directly and calls `cerul2pingmapper` internally.

```python
from pingmapper.doWork import doWork

params = {
    "project_mode": 1,
    "tempC": 10.0,
    "nchunk": 500,
    "rect_wcp": True,
}

result = doWork(
    in_file=r"Z:\survey\line001\line001_2026-05-07-14-30.svlog",
    out_dir=r"Z:\survey\pm_out",
    proj_name="line001",
    batch=False,
    params=params,
)

print(result)
```

Process the single shared file once per survey line.

## 8) Validation checks before long surveys

Run these checks on a short trial line:
- One shared `.svlog` grows during recording.
- PINGVerter output includes `meta/*ss_port*.csv` or `meta/*ss_star*.csv`.
- Converted metadata has plausible `time_s`, `ping_number`, `lat`, `lon` (if nav is enabled).
- PINGMapper completes without falling back to sonar-only mode (unless intended).

## 9) Common pitfalls

- Time alignment drift between independent devices.
  - Mitigation: start both recorders together and keep GPS source stable.
- Missing nav in logs.
  - Symptom: warnings about no `lat/lon` and non-georeferenced processing.
- Endpoint confusion (wrong IP/port).
  - Mitigation: test each sonar in isolation before dual runs.
- Log overwrites.
    - Mitigation: keep unique `line-name` values per run.

## 10) Suggested next step

After you validate one short dual-device survey, build a tiny post-run QA script that reads metadata outputs and reports:
- ping counts
- start/end timestamps
- nav coverage percentage
- average speed

That gives you a go/no-go quality gate before full processing.
