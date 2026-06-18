import os
import socket as _socket
import uuid

SERVER_UUID = str(uuid.uuid4())

# MASTER_HOST is the address workers will try after discovery/handshake.
MASTER_HOST = "10.62.206.219"
MASTER_BIND_HOST = "0.0.0.0"
MASTER_PORT = 5000
MASTER_NAME = "MASTER_77"

# UDP discovery uses the same logical port as the TCP master by default.
DISCOVERY_PORT = int(os.getenv("DISCOVERY_PORT", str(MASTER_PORT)))
DISCOVERY_BROADCAST_ADDRESS = os.getenv("DISCOVERY_BROADCAST_ADDRESS", "255.255.255.255")
DISCOVERY_TIMEOUT = float(os.getenv("DISCOVERY_TIMEOUT", "3.0"))
DISCOVERY_RETRY_DELAY = float(os.getenv("DISCOVERY_RETRY_DELAY", "2.0"))

LOAD_THRESHOLD = int(os.getenv("LOAD_THRESHOLD", "5"))
RELEASE_THRESHOLD = int(os.getenv("RELEASE_THRESHOLD", "3"))
TASK_DURATION = 3
REQUEST_INTERVAL = 1.0
HEARTBEAT_INTERVAL = 30.0
HEARTBEAT_TIMEOUT = 5.0
# Sprint 2 ativa o fluxo completo de apresentação, fila e processamento.
SPRINT1_HEARTBEAT_ONLY = False
CONNECTION_ERROR_THRESHOLD = 4
ELECTION_RETRY_INTERVAL = 2.0
SPRINT3_HELP_TIMEOUT = float(os.getenv("SPRINT3_HELP_TIMEOUT", "5.0"))
SPRINT3_DEFAULT_WORKERS_TO_BORROW = int(os.getenv("SPRINT3_DEFAULT_WORKERS_TO_BORROW", "1"))


def _parse_neighbor_masters(raw_value):
	masters = []
	if not raw_value:
		return masters

	for item in raw_value.split(","):
		candidate = item.strip()
		if not candidate:
			continue
		host, separator, port_text = candidate.partition(":")
		if not separator:
			continue
		try:
			masters.append((host.strip(), int(port_text.strip())))
		except ValueError:
			continue
	return masters

# Legacy neighbor settings remain for compatibility with the existing code path.
ELECTION_PORT = 5100
ELECTION_CANDIDATES = [MASTER_HOST]
NEIGHBOR_MASTERS = [("10.62.206.48", 8000)]

# Set GENERATE_TASKS=false to run as a passive helper master (no task load generator).
GENERATE_TASKS = False

# Sprint 4 — Supervisor de métricas
SUPERVISOR_HOST = os.getenv("SUPERVISOR_HOST", "10.62.206.206")
SUPERVISOR_PORT = int(os.getenv("SUPERVISOR_PORT", "8000"))
SUPERVISOR_SNI = os.getenv("SUPERVISOR_SNI", "10.62.206.206")
SUPERVISOR_TLS = os.getenv("SUPERVISOR_TLS", "false").lower() == "true"
SUPERVISOR_INTERVAL = float(os.getenv("SUPERVISOR_INTERVAL", "10.0"))
FARM_ID = os.getenv("FARM_ID", MASTER_NAME)
FARM_HOSTNAME = os.getenv("FARM_HOSTNAME", f"{FARM_ID}.farm.local")