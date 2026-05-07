#!/usr/bin/env python3

"""
Dual OmniScan 450 logger (single shared SVLOG).

Records two OmniScan 450 streams at once (typically port and starboard), writes
all packets to one shared .svlog, and prints post-run QA including sonar/nav
packet counts and per-device packet distribution.
"""

import argparse
import json
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from brping import Omniscan450
from brping import definitions

RUNNING = True


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

    return parser.parse_args()


def main() -> int:
    global RUNNING

    args = parse_args()

    log_root = Path(args.log_root).resolve()
    stamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    combined_log = log_root / args.line_name / f"{args.line_name}_{stamp}.svlog"
    writer = SharedSvlogWriter(combined_log)

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

        print("Recording dual OmniScan streams to ONE shared SVLOG. Press Ctrl+C to stop.")
        print(f"Combined svlog: {combined_log}")

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

    return 0


if __name__ == "__main__":
    sys.exit(main())
