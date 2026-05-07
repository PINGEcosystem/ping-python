#!/usr/bin/env python3

"""
Dual OmniScan 450 logger (single shared SVLOG).

Records two OmniScan 450 streams at once (typically port and starboard), writes
all packets to one shared .svlog, and prints post-run QA including sonar/nav
packet counts and per-device packet distribution.

Optional: ingest NMEA over UDP and inject navigation JSON wrapper packets into
the same shared SVLOG so downstream tools can georeference without SonarView.
"""

import argparse
import json
import signal
import socket
import struct
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from brping import Omniscan450
from brping import definitions

RUNNING = True


class RawPacket:
    def __init__(self, msg_data: bytes):
        self.msg_data = msg_data


class SharedSvlogWriter:
    """Thread-safe writer for appending packets from multiple devices to one SVLOG."""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.bytes_written = 0

        # Create/truncate file at start of acquisition.
        self.log_path.write_bytes(b"")

    def write_message(self, msg) -> None:
        payload = getattr(msg, "msg_data", None)
        if not payload:
            return

        with self.lock:
            with open(self.log_path, "ab") as handle:
                handle.write(payload)
                self.bytes_written += len(payload)


class NavState:
    def __init__(self):
        self.lock = threading.Lock()
        self.lat_deg: Optional[float] = None
        self.lon_deg: Optional[float] = None
        self.alt_m: Optional[float] = None
        self.hdg_deg: Optional[float] = None
        self.last_update_monotonic: Optional[float] = None
        self.valid_sentences = 0
        self.total_sentences = 0


def nmea_checksum_ok(sentence: str) -> bool:
    if not sentence.startswith("$") or "*" not in sentence:
        return False

    body, chk = sentence[1:].split("*", 1)
    chk = chk.strip()
    if len(chk) < 2:
        return False

    calc = 0
    for char in body:
        calc ^= ord(char)

    try:
        expected = int(chk[:2], 16)
    except ValueError:
        return False

    return calc == expected


def parse_nmea_coord(raw: str, hemi: str, is_lat: bool) -> Optional[float]:
    if not raw or not hemi:
        return None

    try:
        value = float(raw)
    except ValueError:
        return None

    degrees = int(value / 100)
    minutes = value - (degrees * 100)
    dec_deg = degrees + (minutes / 60.0)

    hemi = hemi.upper()
    if is_lat and hemi not in ("N", "S"):
        return None
    if (not is_lat) and hemi not in ("E", "W"):
        return None

    if hemi in ("S", "W"):
        dec_deg *= -1.0

    return dec_deg


def parse_nmea_sentence(sentence: str, nav_state: NavState) -> None:
    line = sentence.strip()
    if not line.startswith("$"):
        return

    nav_state.total_sentences += 1

    if not nmea_checksum_ok(line):
        return

    body = line[1:].split("*", 1)[0]
    fields = body.split(",")
    if not fields:
        return

    tag = fields[0].upper()

    lat_deg = None
    lon_deg = None
    alt_m = None
    hdg_deg = None

    # RMC: $GPRMC,time,status,lat,NS,lon,EW,sog,cog,date,...
    if tag.endswith("RMC") and len(fields) >= 10:
        status = fields[2].upper() if fields[2] else ""
        if status == "A":
            lat_deg = parse_nmea_coord(fields[3], fields[4], is_lat=True)
            lon_deg = parse_nmea_coord(fields[5], fields[6], is_lat=False)
            if fields[8]:
                try:
                    hdg_deg = float(fields[8])
                except ValueError:
                    pass

    # GGA: $GPGGA,time,lat,NS,lon,EW,fix,numsat,hdop,alt,M,...
    elif tag.endswith("GGA") and len(fields) >= 10:
        fix = fields[6]
        if fix and fix != "0":
            lat_deg = parse_nmea_coord(fields[2], fields[3], is_lat=True)
            lon_deg = parse_nmea_coord(fields[4], fields[5], is_lat=False)
            if fields[9]:
                try:
                    alt_m = float(fields[9])
                except ValueError:
                    pass

    # HDT: $HEHDT,heading,T
    elif tag.endswith("HDT") and len(fields) >= 2:
        if fields[1]:
            try:
                hdg_deg = float(fields[1])
            except ValueError:
                pass

    # VTG: $GPVTG,cog,T,,M,sog_knots,N,sog_kmh,K
    elif tag.endswith("VTG") and len(fields) >= 2:
        if fields[1]:
            try:
                hdg_deg = float(fields[1])
            except ValueError:
                pass

    changed = False
    now = time.monotonic()

    with nav_state.lock:
        if lat_deg is not None and lon_deg is not None:
            nav_state.lat_deg = lat_deg
            nav_state.lon_deg = lon_deg
            nav_state.last_update_monotonic = now
            changed = True

        if alt_m is not None:
            nav_state.alt_m = alt_m
            changed = True

        if hdg_deg is not None:
            # Normalize into [0, 360)
            nav_state.hdg_deg = hdg_deg % 360.0
            changed = True

        if changed:
            nav_state.valid_sentences += 1


def build_json_wrapper_packet(payload_obj: Dict[str, object], src_device_id: int = 0) -> RawPacket:
    json_bytes = json.dumps(payload_obj, separators=(",", ":")).encode("utf-8")
    msg_id = definitions.OMNISCAN450_JSON_WRAPPER

    packet = bytearray()
    packet += b"BR"
    packet += int(len(json_bytes)).to_bytes(2, "little")
    packet += int(msg_id).to_bytes(2, "little")
    packet += int(0).to_bytes(1, "little")  # dst_device_id
    packet += int(src_device_id).to_bytes(1, "little")
    packet += json_bytes

    checksum = sum(packet) & 0xFFFF
    packet += struct.pack("<H", checksum)

    return RawPacket(bytes(packet))


def nmea_listener_worker(listen_host: str, listen_port: int, nav_state: NavState) -> None:
    global RUNNING

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((listen_host, listen_port))
    sock.settimeout(0.5)

    print(f"[nmea] listening on udp://{listen_host}:{listen_port}")

    try:
        while RUNNING:
            try:
                data, _ = sock.recvfrom(8192)
            except socket.timeout:
                continue
            except OSError:
                break

            text = data.decode("ascii", errors="ignore")
            for raw_line in text.replace("\r", "\n").split("\n"):
                if raw_line.strip():
                    parse_nmea_sentence(raw_line, nav_state)
    finally:
        try:
            sock.close()
        except Exception:
            pass


def nav_injector_worker(
    writer: SharedSvlogWriter,
    nav_state: NavState,
    nav_rate_hz: float,
    src_device_id: int,
) -> None:
    global RUNNING

    period_s = 1.0 / nav_rate_hz if nav_rate_hz > 0 else 0.2
    seq = 0
    boot_start = time.monotonic()

    while RUNNING:
        time.sleep(period_s)

        with nav_state.lock:
            lat = nav_state.lat_deg
            lon = nav_state.lon_deg
            alt = nav_state.alt_m
            hdg = nav_state.hdg_deg

        if lat is None or lon is None:
            continue

        time_boot_ms = int((time.monotonic() - boot_start) * 1000.0)

        message = {
            "time_boot_ms": time_boot_ms,
            "lat": int(round(lat * 1e7)),
            "lon": int(round(lon * 1e7)),
        }

        if alt is not None:
            message["alt"] = int(round(alt * 1000.0))
            message["relative_alt"] = int(round(alt * 1000.0))

        if hdg is not None:
            message["hdg"] = int(round(hdg * 100.0))

        payload_obj = {
            "header": {
                "system_id": 255,
                "component_id": 1,
                "sequence": seq,
                "type": "nmea_nav",
            },
            "message": message,
        }

        writer.write_message(build_json_wrapper_packet(payload_obj, src_device_id=src_device_id))
        seq = (seq + 1) % 65536


def parse_endpoint(endpoint: str) -> Tuple[str, int]:
    if ":" not in endpoint:
        raise ValueError(f"Endpoint must be HOST:PORT, got '{endpoint}'")

    host, port_str = endpoint.rsplit(":", 1)
    if not host:
        raise ValueError(f"Endpoint host is empty: '{endpoint}'")

    try:
        port = int(port_str)
    except ValueError as exc:
        raise ValueError(f"Endpoint port is not an integer: '{endpoint}'") from exc

    return host, port


def connect_device(dev: Omniscan450, protocol: str, endpoint: str, timeout_s: float) -> None:
    host, port = parse_endpoint(endpoint)

    if protocol == "udp":
        dev.connect_udp(host, port)
    elif protocol == "tcp":
        dev.connect_tcp(host, port, timeout=timeout_s)
    else:
        raise ValueError(f"Unsupported protocol: {protocol}")


def configure_ping(
    dev: Omniscan450,
    start_mm: int,
    length_mm: int,
    ping_rate_hz: Optional[float],
    pulse_percent: Optional[float],
    num_results: Optional[int],
) -> None:
    params: Dict[str, object] = {
        "start_mm": start_mm,
        "length_mm": length_mm,
        "enable": 1,
    }

    if ping_rate_hz is not None:
        params["msec_per_ping"] = Omniscan450.calc_msec_per_ping(ping_rate_hz)

    if pulse_percent is not None:
        params["pulse_len_percent"] = Omniscan450.calc_pulse_length_pc(pulse_percent)

    if num_results is not None:
        params["num_results"] = int(num_results)

    dev.control_os_ping_params(**params)


def stop_device(name: str, dev: Omniscan450) -> None:
    try:
        dev.control_os_ping_params(enable=0)
    except Exception as exc:
        print(f"[{name}] Failed to disable pinging: {exc}")

    try:
        if dev.iodev:
            dev.iodev.close()
    except Exception as exc:
        print(f"[{name}] Failed to close socket: {exc}")


def read_svlog_stats(svlog_path: Path) -> Dict[str, object]:
    stats: Dict[str, object] = {
        "path": str(svlog_path),
        "total_packets": 0,
        "sonar_packets": 0,
        "nav_packets": 0,
        "gps_fixes": 0,
        "first_ping_number": None,
        "last_ping_number": None,
        "packets_by_src": {},
    }

    if not svlog_path.exists():
        return stats

    with open(svlog_path, "rb") as handle:
        while True:
            msg = Omniscan450.read_packet(handle)
            if msg is None:
                break

            stats["total_packets"] += 1

            src_id = getattr(msg, "src_device_id", "unknown")
            by_src = stats["packets_by_src"]
            by_src[src_id] = by_src.get(src_id, 0) + 1

            if msg.message_id == definitions.OMNISCAN450_OS_MONO_PROFILE:
                stats["sonar_packets"] += 1
                ping_no = getattr(msg, "ping_number", None)
                if ping_no is not None:
                    if stats["first_ping_number"] is None:
                        stats["first_ping_number"] = ping_no
                    stats["last_ping_number"] = ping_no

            elif msg.message_id == definitions.OMNISCAN450_JSON_WRAPPER:
                stats["nav_packets"] += 1

                payload = getattr(msg, "payload", b"")
                if not payload:
                    continue

                try:
                    nav_obj = json.loads(payload.decode("utf-8"))
                except Exception:
                    continue

                msg_obj = nav_obj.get("message", {})
                lat = msg_obj.get("lat")
                lon = msg_obj.get("lon")
                if lat is not None and lon is not None:
                    stats["gps_fixes"] += 1

    sonar_packets = stats["sonar_packets"]
    nav_packets = stats["nav_packets"]
    gps_fixes = stats["gps_fixes"]

    stats["nav_per_sonar_pct"] = (100.0 * nav_packets / sonar_packets) if sonar_packets else 0.0
    stats["gps_fix_per_sonar_pct"] = (100.0 * gps_fixes / sonar_packets) if sonar_packets else 0.0

    return stats


def worker(
    name: str,
    dev: Omniscan450,
    writer: SharedSvlogWriter,
    status: Dict[str, object],
    print_every: int,
) -> None:
    global RUNNING

    while RUNNING:
        msg = dev.read()
        if msg is None:
            time.sleep(0.005)
            continue

        # Write every packet to preserve nav + sonar in the shared log.
        writer.write_message(msg)

        status["packets_live"] += 1

        if msg.message_id == definitions.OMNISCAN450_OS_MONO_PROFILE:
            status["sonar_packets_live"] += 1
            status["last_ping_number"] = getattr(msg, "ping_number", None)

            if print_every > 0 and (status["sonar_packets_live"] % print_every == 0):
                print(f"[{name}] sonar pings: {status['sonar_packets_live']}")

        elif msg.message_id == definitions.OMNISCAN450_JSON_WRAPPER:
            status["nav_packets_live"] += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record two OmniScan 450 streams into one shared SVLOG and summarize nav coverage."
    )

    parser.add_argument("--port-endpoint", required=True, help="Port sonar endpoint, HOST:PORT")
    parser.add_argument("--star-endpoint", required=True, help="Starboard sonar endpoint, HOST:PORT")

    parser.add_argument("--port-protocol", choices=["udp", "tcp"], default="udp")
    parser.add_argument("--star-protocol", choices=["udp", "tcp"], default="udp")
    parser.add_argument("--tcp-timeout", type=float, default=5.0, help="TCP connect timeout (seconds)")

    parser.add_argument("--line-name", default="line", help="Line/session name for combined log filename")
    parser.add_argument("--log-root", default="logs/omniscan_dual", help="Root log folder")

    parser.add_argument("--start-mm", type=int, default=0, help="Start range in mm")
    parser.add_argument("--length-mm", type=int, default=5000, help="Range length in mm")
    parser.add_argument("--ping-rate-hz", type=float, default=None, help="Optional ping rate in Hz")
    parser.add_argument("--pulse-percent", type=float, default=None, help="Optional pulse length percent")
    parser.add_argument("--num-results", type=int, default=None, help="Optional sample count per ping")

    parser.add_argument(
        "--print-every",
        type=int,
        default=100,
        help="Print live sonar count every N pings per device (0 disables)",
    )

    parser.add_argument(
        "--nmea-udp-listen",
        default=None,
        help="Optional NMEA UDP listen endpoint HOST:PORT (example: 0.0.0.0:10110)",
    )
    parser.add_argument(
        "--nav-rate-hz",
        type=float,
        default=5.0,
        help="Injected nav packet rate (Hz) when --nmea-udp-listen is enabled",
    )
    parser.add_argument(
        "--nav-src-device-id",
        type=int,
        default=250,
        help="src_device_id used for injected nav packets",
    )

    return parser.parse_args()


def main() -> int:
    global RUNNING

    args = parse_args()

    log_root = Path(args.log_root).resolve()
    stamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    combined_log = log_root / args.line_name / f"{args.line_name}_{stamp}.svlog"
    writer = SharedSvlogWriter(combined_log)
    nav_state = NavState()

    # Internal logger disabled because we write to one shared file ourselves.
    port_dev = Omniscan450(logging=False)
    star_dev = Omniscan450(logging=False)

    devices = {
        "port": {
            "dev": port_dev,
            "protocol": args.port_protocol,
            "endpoint": args.port_endpoint,
        },
        "star": {
            "dev": star_dev,
            "protocol": args.star_protocol,
            "endpoint": args.star_endpoint,
        },
    }

    def sigint_handler(_sig, _frame):
        global RUNNING
        print("\nStopping acquisition...")
        RUNNING = False

    signal.signal(signal.SIGINT, sigint_handler)

    status = {
        "port": {
            "packets_live": 0,
            "sonar_packets_live": 0,
            "nav_packets_live": 0,
            "last_ping_number": None,
        },
        "star": {
            "packets_live": 0,
            "sonar_packets_live": 0,
            "nav_packets_live": 0,
            "last_ping_number": None,
        },
        "nav_injected": {
            "enabled": bool(args.nmea_udp_listen),
            "valid_sentences": 0,
            "total_sentences": 0,
        },
    }

    try:
        for name, cfg in devices.items():
            connect_device(cfg["dev"], cfg["protocol"], cfg["endpoint"], args.tcp_timeout)
            if not cfg["dev"].initialize():
                raise RuntimeError(f"[{name}] initialize() failed")

            info = cfg["dev"].readDeviceInformation()
            print(f"[{name}] connected, device_type={getattr(info, 'device_type', 'unknown')}")

            configure_ping(
                cfg["dev"],
                start_mm=args.start_mm,
                length_mm=args.length_mm,
                ping_rate_hz=args.ping_rate_hz,
                pulse_percent=args.pulse_percent,
                num_results=args.num_results,
            )

        # Write one metadata packet from each device at start.
        writer.write_message(port_dev.build_metadata_packet())
        writer.write_message(star_dev.build_metadata_packet())

        aux_threads = []

        if args.nmea_udp_listen:
            nmea_host, nmea_port = parse_endpoint(args.nmea_udp_listen)
            nmea_thread = threading.Thread(
                target=nmea_listener_worker,
                args=(nmea_host, nmea_port, nav_state),
                daemon=True,
            )
            nav_thread = threading.Thread(
                target=nav_injector_worker,
                args=(writer, nav_state, args.nav_rate_hz, args.nav_src_device_id),
                daemon=True,
            )
            nmea_thread.start()
            nav_thread.start()
            aux_threads.extend([nmea_thread, nav_thread])

        print("Recording dual OmniScan streams to ONE shared SVLOG. Press Ctrl+C to stop.")
        print(f"Combined svlog: {combined_log}")
        if args.nmea_udp_listen:
            print(f"[nmea] nav injection enabled from udp://{args.nmea_udp_listen}")

        threads = [
            threading.Thread(
                target=worker,
                args=("port", port_dev, writer, status["port"], args.print_every),
                daemon=True,
            ),
            threading.Thread(
                target=worker,
                args=("star", star_dev, writer, status["star"], args.print_every),
                daemon=True,
            ),
        ]

        for thread in threads:
            thread.start()

        while RUNNING:
            time.sleep(0.2)

    except KeyboardInterrupt:
        RUNNING = False
    except Exception as exc:
        print(f"Fatal error: {exc}")
        RUNNING = False
    finally:
        for name, cfg in devices.items():
            stop_device(name, cfg["dev"])

        with nav_state.lock:
            status["nav_injected"]["valid_sentences"] = nav_state.valid_sentences
            status["nav_injected"]["total_sentences"] = nav_state.total_sentences

        merged_stats = read_svlog_stats(combined_log)

        print("\n=== Acquisition Summary ===")
        print(
            "[port] packets={packets} sonar={sonar} nav={nav} last_ping={last}".format(
                packets=status.get("port", {}).get("packets_live", 0),
                sonar=status.get("port", {}).get("sonar_packets_live", 0),
                nav=status.get("port", {}).get("nav_packets_live", 0),
                last=status.get("port", {}).get("last_ping_number"),
            )
        )
        print(
            "[star] packets={packets} sonar={sonar} nav={nav} last_ping={last}".format(
                packets=status.get("star", {}).get("packets_live", 0),
                sonar=status.get("star", {}).get("sonar_packets_live", 0),
                nav=status.get("star", {}).get("nav_packets_live", 0),
                last=status.get("star", {}).get("last_ping_number"),
            )
        )
        if status["nav_injected"]["enabled"]:
            print(
                "[nmea] valid_sentences={valid} total_sentences={total}".format(
                    valid=status["nav_injected"].get("valid_sentences", 0),
                    total=status["nav_injected"].get("total_sentences", 0),
                )
            )

        print("\n[combined] post-run svlog QA")
        print(
            "  file={path}\n"
            "  bytes_written={bytes_written}\n"
            "  total_packets={total_packets}\n"
            "  sonar_packets={sonar_packets}\n"
            "  nav_packets={nav_packets}\n"
            "  gps_fixes={gps_fixes}\n"
            "  nav_per_sonar_pct={nav_pct:.2f}\n"
            "  gps_fix_per_sonar_pct={fix_pct:.2f}\n"
            "  first_ping={first_ping_number}\n"
            "  last_ping={last_ping_number}".format(
                path=merged_stats.get("path"),
                bytes_written=writer.bytes_written,
                total_packets=merged_stats.get("total_packets", 0),
                sonar_packets=merged_stats.get("sonar_packets", 0),
                nav_packets=merged_stats.get("nav_packets", 0),
                gps_fixes=merged_stats.get("gps_fixes", 0),
                nav_pct=merged_stats.get("nav_per_sonar_pct", 0.0),
                fix_pct=merged_stats.get("gps_fix_per_sonar_pct", 0.0),
                first_ping_number=merged_stats.get("first_ping_number"),
                last_ping_number=merged_stats.get("last_ping_number"),
            )
        )

        packets_by_src = merged_stats.get("packets_by_src", {})
        if packets_by_src:
            print("  packets_by_src_device_id:")
            for src_id, count in sorted(packets_by_src.items(), key=lambda kv: str(kv[0])):
                print(f"    src={src_id}: {count}")

        print(
            "\nNOTE: Shared-log conversion assumes both devices have compatible time bases and distinct channel usage "
            "for clean beam separation downstream."
        )
        if status["nav_injected"]["enabled"]:
            print(
                "NOTE: Injected nav packets are written with message.time_boot_ms/lat/lon (and optional hdg/alt) "
                "for PINGVerter compatibility."
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
