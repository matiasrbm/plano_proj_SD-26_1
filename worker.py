#!/usr/bin/env python3
# ============================================================
#  worker.py
#  - Conecta em um Master e RECEBE tarefas
#  - Descobre Masters por UDP quando nao recebe host/porta
#  - Elegera o mesmo Master por nome e confirma via TCP
#  - Processa (simula com sleep) e avisa que terminou
#  - Pode ser redirecionado para outro Master
# ============================================================
#  Como usar:
#    python worker.py 127.0.0.1 5000
#    python worker.py
# ============================================================

import json
import socket
import sys
import threading
import time
import uuid

from config import (
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT,
    MASTER_PORT,
    TASK_DURATION,
    CONNECTION_ERROR_THRESHOLD,
    ELECTION_RETRY_INTERVAL,
    DISCOVERY_PORT,
    DISCOVERY_TIMEOUT,
    DISCOVERY_BROADCAST_ADDRESS,
    DISCOVERY_RETRY_DELAY,
)

WORKER_UUID = str(uuid.uuid4())
master_target = {"host": None, "port": None}
master_target_lock = threading.Lock()
original_master_target = None
original_master_uuid = None
current_master_uuid = None
last_registration_master_uuid = None


# ── Envio de mensagem JSON ───────────────────────────────────
def send(sock, payload):
    try:
        sock.sendall((json.dumps(payload) + "\n").encode())
    except OSError:
        pass


# ── Recebimento de mensagem JSON ─────────────────────────────
def receive(sock):
    try:
        data = b""
        while b"\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
        return json.loads(data.split(b"\n")[0])
    except Exception:
        return None


def receive_with_timeout(sock, timeout_seconds):
    original_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout_seconds)
        return receive(sock)
    except socket.timeout:
        return None
    finally:
        sock.settimeout(original_timeout)


# ── Conecta no Master ───────────────────────────────────────
def connect(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(HEARTBEAT_TIMEOUT)
    sock.connect((host, port))
    print(f"[WORKER] Conectado em {host}:{port}")
    return sock


def build_presentation_payload():
    current_host, current_port = get_master_target()
    payload = {
        # Apresentacao inicial do Worker para o Master.
        "WORKER": "ALIVE",
        "WORKER_UUID": WORKER_UUID,
    }

    borrowed = (
        original_master_target is not None
        and original_master_uuid is not None
        and (current_host, current_port) != original_master_target
    )

    if borrowed:
        # Quando o Worker estiver emprestado, informa o Master original.
        payload["SERVER_UUID"] = original_master_uuid

    return payload


def process_task(sock, task_msg):
    task_id = task_msg.get("TASK_ID", "SEM_ID")
    user = task_msg.get("USER", "desconhecido")
    force_nok = bool(task_msg.get("FORCE_NOK", False))

    # Simula o processamento do trabalho recebido do Master.
    print(f"[WORKER] Processando tarefa {task_id} para {user}...")
    time.sleep(TASK_DURATION)

    status_payload = {
        # Reporte de status exigido na Sprint 2.
        "STATUS": "NOK" if force_nok else "OK",
        "TASK": "QUERY",
        "WORKER_UUID": WORKER_UUID,
        "TASK_ID": task_id,
    }
    send(sock, status_payload)

    ack = receive(sock)
    if ack and ack.get("STATUS") == "ACK":
        print(f"[WORKER] ACK recebido da tarefa {task_id}.")
    else:
        print(f"[WORKER] Sem ACK explicito para a tarefa {task_id}.")


def handle_master_message(sock, msg):
    if not isinstance(msg, dict):
        return

    if msg.get("TASK") == "QUERY":
        process_task(sock, msg)
    elif msg.get("TASK") == "NO_TASK":
        print("[WORKER] Master informou que nao ha tarefa na fila.")
    elif msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
        print("[WORKER] Heartbeat confirmado pelo Master.")


def set_master_target(host, port, reason=""):
    global original_master_target
    with master_target_lock:
        master_target["host"] = host
        master_target["port"] = port
        if original_master_target is None and host is not None and port is not None:
            original_master_target = (host, port)
    if reason and host is not None and port is not None:
        print(f"[WORKER] Novo master alvo: {host}:{port} ({reason})")


def get_master_target():
    with master_target_lock:
        return master_target["host"], master_target["port"]


def build_discovery_payload():
    # O worker manda apenas o necessario para a etapa 01.
    return {
        "TYPE": "DISCOVERY",
        "WORKER_UUID": WORKER_UUID,
    }


def valid_discovery_reply(msg):
    if not isinstance(msg, dict):
        return False
    if msg.get("TYPE") != "DISCOVERY_REPLY":
        return False

    master_name = msg.get("MASTER_NAME")
    master_ip = msg.get("MASTER_IP")
    master_port = msg.get("MASTER_PORT")
    status = msg.get("STATUS")

    if not isinstance(master_name, str) or not master_name.strip():
        return False
    if not isinstance(master_ip, str) or not master_ip.strip():
        return False
    if status != "AVAILABLE":
        return False
    try:
        int(master_port)
    except (TypeError, ValueError):
        return False
    return True


def send_discovery_probes(sock):
    payload = (json.dumps(build_discovery_payload()) + "\n").encode()
    targets = [
        (DISCOVERY_BROADCAST_ADDRESS, DISCOVERY_PORT),
        ("127.0.0.1", DISCOVERY_PORT),
    ]

    for target in targets:
        try:
            sock.sendto(payload, target)
            print(f"[WORKER][DISCOVERY] Probe enviado para {target[0]}:{target[1]}.")
        except OSError as error:
            print(f"[WORKER][DISCOVERY] Falha ao enviar probe para {target[0]}:{target[1]} - {error}")


def collect_discovery_replies():
    replies = []
    seen = set()
    discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    discovery_socket.bind(("", 0))
    discovery_socket.settimeout(0.5)

    send_discovery_probes(discovery_socket)
    deadline = time.time() + DISCOVERY_TIMEOUT

    while time.time() < deadline:
        remaining = deadline - time.time()
        discovery_socket.settimeout(max(0.05, min(0.5, remaining)))
        try:
            data, addr = discovery_socket.recvfrom(4096)
        except socket.timeout:
            continue
        except OSError:
            break

        try:
            msg = json.loads(data.decode().strip())
        except Exception:
            print(f"[WORKER][DISCOVERY] Resposta invalida recebida de {addr}.")
            continue

        if not valid_discovery_reply(msg):
            print(f"[WORKER][DISCOVERY] Resposta descoberta ignorada de {addr}.")
            continue

        master_name = msg["MASTER_NAME"].strip()
        master_host = msg["MASTER_IP"].strip()
        master_port = int(msg["MASTER_PORT"])
        key = (master_name, master_host, master_port)
        if key in seen:
            continue
        seen.add(key)
        replies.append({"name": master_name, "host": master_host, "port": master_port})
        print(f"[WORKER][DISCOVERY] Resposta valida de {master_name} em {master_host}:{master_port}.")

    discovery_socket.close()
    return replies


def choose_discovery_winner(replies):
    if not replies:
        return None
    return min(replies, key=lambda item: (item["name"], item["host"], item["port"]))


def discover_master():
    replies = collect_discovery_replies()
    if not replies:
        print("[WORKER][DISCOVERY] Nenhum Master respondeu dentro da janela de descoberta.")
        return None

    winner = choose_discovery_winner(replies)
    print(
        f"[WORKER][ELECTION] Vencedor escolhido por nome: {winner['name']} "
        f"({winner['host']}:{winner['port']})."
    )
    return winner


def perform_election_handshake(sock, selected_master_name):
    # Esta eh a ponte entre a eleicao e o fluxo de heartbeat ja existente.
    payload = {
        "TYPE": "ELECTION_ACK",
        "WORKER_UUID": WORKER_UUID,
        "SELECTED_MASTER": selected_master_name,
    }
    send(sock, payload)
    response = receive(sock)

    if (
        response
        and response.get("TYPE") == "ELECTION_ACK"
        and response.get("STATUS") == "ACCEPTED"
        and response.get("MASTER_NAME") == selected_master_name
    ):
        print(f"[WORKER][ELECTION] ACK confirmado por {selected_master_name}.")
        return True

    print(f"[WORKER][ELECTION] ACK negado ou ausente para {selected_master_name}.")
    return False


def register_with_master(sock):
    global original_master_uuid, current_master_uuid, last_registration_master_uuid

    payload = build_presentation_payload()
    send(sock, payload)

    response = receive(sock)
    if not response:
        return None

    response_server_uuid = response.get("SERVER_UUID")
    if isinstance(response_server_uuid, str) and response_server_uuid.strip():
        current_master_uuid = response_server_uuid
        if original_master_uuid is None:
            original_master_uuid = response_server_uuid

    print(f"[WORKER] Apresentacao enviada: {payload}")

    if response.get("TASK") == "QUERY":
        process_task(sock, response)
    elif response.get("TASK") == "NO_TASK":
        print("[WORKER] Master informou que nao havia tarefa na apresentacao.")

    last_registration_master_uuid = current_master_uuid
    return response


# ── Loop principal: heartbeat periodico com reconexao ────────
def run(host=None, port=None):
    global original_master_uuid, current_master_uuid, last_registration_master_uuid

    discovery_mode = host is None or port is None
    if discovery_mode:
        print("[WORKER][DISCOVERY] Iniciando sem host/porta configurados; usando descoberta UDP.")
    else:
        set_master_target(host, port, "inicial")

    sock = None
    consecutive_errors = 0
    selected_master = None

    while True:
        try:
            if discovery_mode and sock is None:
                selected_master = discover_master()
                if selected_master is None:
                    consecutive_errors += 1
                    print(
                        f"[WORKER] Status: OFFLINE - Nenhum Master encontrado "
                        f"({consecutive_errors}/{CONNECTION_ERROR_THRESHOLD})"
                    )
                    time.sleep(DISCOVERY_RETRY_DELAY)
                    continue
                set_master_target(
                    selected_master["host"],
                    selected_master["port"],
                    f"descoberta {selected_master['name']}",
                )

            target_host, target_port = get_master_target()
            if target_host is None or target_port is None:
                raise TimeoutError("Master alvo nao definido")

            if sock is None:
                sock = connect(target_host, target_port)
                if discovery_mode:
                    if not perform_election_handshake(sock, selected_master["name"]):
                        raise TimeoutError("Handshake de eleicao recusado")

            response = register_with_master(sock)
            if response is None:
                raise TimeoutError("Resposta invalida ou ausente do Master na solicitacao")

            consecutive_errors = 0
            time.sleep(HEARTBEAT_INTERVAL)

        except (socket.timeout, TimeoutError, OSError):
            consecutive_errors += 1
            print(
                f"[WORKER] Status: OFFLINE - Tentando Reconectar "
                f"({consecutive_errors}/{CONNECTION_ERROR_THRESHOLD})"
            )
            try:
                if sock is not None:
                    sock.close()
            except OSError:
                pass
            sock = None
            last_registration_master_uuid = None

            if discovery_mode:
                selected_master = None
                time.sleep(DISCOVERY_RETRY_DELAY)
                continue

            time.sleep(ELECTION_RETRY_INTERVAL)


# ── Entry point ──────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(f"[WORKER] UUID: {WORKER_UUID}")
        run()
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])

        print(f"[WORKER] UUID: {WORKER_UUID}")
        run(host, port)
    else:
        print("Uso: python worker.py [<host> <porta>]")
        print("Exemplos:")
        print("  python worker.py")
        print("  python worker.py 127.0.0.1 5000")
        sys.exit(1)
