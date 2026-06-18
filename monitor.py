#!/usr/bin/env python3
"""Sprint 4: coleta métricas da farm e envia via TLS TCP ao supervisor a cada SUPERVISOR_INTERVAL."""

import json
import os
import shutil
import socket
import ssl
import time
import uuid

from config import (
    FARM_ID,
    FARM_HOSTNAME,
    SUPERVISOR_HOST,
    SUPERVISOR_PORT,
    SUPERVISOR_SNI,
    SUPERVISOR_INTERVAL,
    SUPERVISOR_TLS,
)

try:
    import psutil as _psutil
    _PSUTIL = True
    _psutil.cpu_percent(interval=None)  # prime: first call always returns 0.0
except ImportError:
    _PSUTIL = False


def _load_average():
    """Retorna (load_1m, load_5m). Retorna (0.0, 0.0) em sistemas sem suporte (Windows)."""
    try:
        la = os.getloadavg()
        return round(float(la[0]), 2), round(float(la[1]), 2)
    except AttributeError:
        return 0.0, 0.0


def _iso_ts(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def collect_system_metrics(start_time):
    """Coleta métricas do sistema operacional. Usa psutil se disponível; zeros como fallback."""
    la1, la5 = _load_average()

    if _PSUTIL:
        cpu_pct = round(_psutil.cpu_percent(interval=None), 2)
        cpu_logical = _psutil.cpu_count(logical=True) or os.cpu_count() or 1
        cpu_physical = _psutil.cpu_count(logical=False) or max(1, (os.cpu_count() or 2) // 2)
        vm = _psutil.virtual_memory()
        mem_total = int(vm.total >> 20)
        mem_avail = int(vm.available >> 20)
        mem_pct = round(vm.percent, 2)
        mem_used = int(vm.used >> 20)
    else:
        cpu_pct = 0.0
        cpu_logical = os.cpu_count() or 1
        cpu_physical = max(1, cpu_logical // 2)
        mem_total = mem_avail = mem_used = 0
        mem_pct = 0.0

    try:
        du = shutil.disk_usage(".")
        disk_total = round(du.total / 1e9, 1)
        disk_free = round(du.free / 1e9, 1)
        disk_pct = round(100.0 * du.used / du.total, 1) if du.total else 0.0
    except OSError:
        disk_total = disk_free = 0.0
        disk_pct = 0.0

    return {
        "uptime_seconds": int(time.time() - start_time),
        "load_average_1m": la1,
        "load_average_5m": la5,
        "cpu": {
            "usage_percent": cpu_pct,
            "count_logical": cpu_logical,
            "count_physical": cpu_physical,
        },
        "memory": {
            "total_mb": mem_total,
            "available_mb": mem_avail,
            "percent_used": mem_pct,
            "memory_used": mem_used,
        },
        "disk": {
            "total_gb": disk_total,
            "free_gb": disk_free,
            "percent_used": disk_pct,
        },
    }


def build_performance_report(farm_state):
    """Monta o payload completo conforme schema sprint4-monitor."""
    system_metrics = collect_system_metrics(farm_state["start_time"])
    return {
        "server_uuid": FARM_ID,
        "hostname": FARM_HOSTNAME,
        "role": "master",
        "task": "performance_report",
        "timestamp": _iso_ts(time.time()),
        "message_id": str(uuid.uuid4()),
        "payload_version": "sprint4-monitor",
        "performance": {
            "system": system_metrics,
            "farm_state": {
                "workers": farm_state["workers"],
                "tasks": farm_state["tasks"],
            },
            "config_thresholds": farm_state["config_thresholds"],
            "neighbors": farm_state["neighbors"],
        },
    }


def send_to_supervisor(payload):
    """Conecta via TCP (TLS opcional), envia JSON + newline, fecha sem aguardar resposta."""
    data = (json.dumps(payload) + "\n").encode("utf-8")
    raw = socket.create_connection((SUPERVISOR_HOST, SUPERVISOR_PORT), timeout=10.0)
    try:
        if SUPERVISOR_TLS:
            ctx = ssl.create_default_context()
            conn = ctx.wrap_socket(raw, server_hostname=SUPERVISOR_SNI)
        else:
            conn = raw
        try:
            conn.sendall(data)
            print(
                f"[MONITOR] Relatorio enviado | server_uuid={payload['server_uuid']}"
                f" | msg_id={payload['message_id'][:8]}"
            )
        finally:
            conn.close()
    except Exception:
        try:
            raw.close()
        except OSError:
            pass
        raise


def monitor_loop(farm_state_provider):
    """Thread daemon: coleta estado, monta relatório e envia ao supervisor a cada SUPERVISOR_INTERVAL."""
    print(
        f"[MONITOR] Iniciando | supervisor={SUPERVISOR_HOST}:{SUPERVISOR_PORT}"
        f" | farm_id={FARM_ID} | intervalo={SUPERVISOR_INTERVAL}s"
    )
    while True:
        try:
            state = farm_state_provider()
            report = build_performance_report(state)
            send_to_supervisor(report)
        except Exception as exc:
            print(f"[MONITOR] Falha ao enviar relatorio: {exc}")
        time.sleep(SUPERVISOR_INTERVAL)
