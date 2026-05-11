import os
import uuid

SERVER_UUID = str(uuid.uuid4())

# MASTER_HOST is the address workers will try after discovery/handshake.
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_BIND_HOST = os.getenv("MASTER_BIND_HOST", "0.0.0.0")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
MASTER_NAME = os.getenv("MASTER_NAME", "MASTER_1")

# UDP discovery uses the same logical port as the TCP master by default.
DISCOVERY_PORT = int(os.getenv("DISCOVERY_PORT", str(MASTER_PORT)))
DISCOVERY_BROADCAST_ADDRESS = os.getenv("DISCOVERY_BROADCAST_ADDRESS", "255.255.255.255")
DISCOVERY_TIMEOUT = float(os.getenv("DISCOVERY_TIMEOUT", "3.0"))
DISCOVERY_RETRY_DELAY = float(os.getenv("DISCOVERY_RETRY_DELAY", "2.0"))

LOAD_THRESHOLD = 5
TASK_DURATION = 3
REQUEST_INTERVAL = 1.0
HEARTBEAT_INTERVAL = 30.0
HEARTBEAT_TIMEOUT = 5.0
# Sprint 2 ativa o fluxo completo de apresentação, fila e processamento.
SPRINT1_HEARTBEAT_ONLY = False
CONNECTION_ERROR_THRESHOLD = 4
ELECTION_RETRY_INTERVAL = 2.0

# Legacy neighbor settings remain for compatibility with the existing code path.
ELECTION_PORT = 5100
ELECTION_CANDIDATES = [MASTER_HOST]
NEIGHBOR_MASTERS = []