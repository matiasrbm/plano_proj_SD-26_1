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
original_master_name = None
current_master_uuid = None
last_registration_master_uuid = None
pending_temporary_registration = False


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


def protocol_message_type(msg):
    if not isinstance(msg, dict):
        return None
    return msg.get("type") or msg.get("TYPE") or msg.get("TASK")


def protocol_payload(msg):
    if not isinstance(msg, dict):
        return {}
    payload = msg.get("payload")
    if isinstance(payload, dict):
        return payload
    return msg


def build_protocol_message(message_type, payload):
    return {
        "type": message_type,
        "request_id": str(uuid.uuid4()),
        "payload": payload,
    }


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


def build_temporary_registration_message():
    current_host, current_port = get_master_target()
    original_host, original_port = (None, None)
    if original_master_target is not None:
        original_host, original_port = original_master_target

    payload = {
        "WORKER_UUID": WORKER_UUID,
        "ORIGINAL_MASTER_UUID": original_master_uuid,
        "ORIGINAL_MASTER_NAME": original_master_name,
        "ORIGINAL_MASTER_HOST": original_host,
        "ORIGINAL_MASTER_PORT": original_port,
        "CURRENT_MASTER_HOST": current_host,
        "CURRENT_MASTER_PORT": current_port,
        "SERVER_UUID": original_master_uuid,
    }
    return build_protocol_message("register_temporary_worker", payload)


def build_registration_message():
    if pending_temporary_registration:
        return build_temporary_registration_message()
    return build_presentation_payload()


def handle_control_message(msg):
    global pending_temporary_registration, current_master_uuid, original_master_name

    message_type = protocol_message_type(msg)
    payload = protocol_payload(msg)

    if message_type == "command_redirect":
        new_host = payload.get("new_master_host") or msg.get("NEW_MASTER_HOST")
        new_port = payload.get("new_master_port") or msg.get("NEW_MASTER_PORT")
        new_master_name = payload.get("new_master_name") or msg.get("NEW_MASTER_NAME")
        if new_host is not None and new_port is not None:
            # Preserva o nome do master original para incluir no register_temporary_worker.
            orig_name = payload.get("original_master_name") or msg.get("ORIGINAL_MASTER_NAME")
            if orig_name is not None and original_master_name is None:
                original_master_name = orig_name
            set_master_target(new_host, int(new_port), f"redirecionado para {new_master_name or 'novo master'}")
            pending_temporary_registration = True
            print(
                f"[WORKER] Redirecionado para {new_master_name or new_host}:{new_port}. "
                "Reconectando..."
            )
            return "reconnect"

    if message_type == "command_release":
        return_host = payload.get("return_to_master_host") or msg.get("RETURN_TO_MASTER_HOST")
        return_port = payload.get("return_to_master_port") or msg.get("RETURN_TO_MASTER_PORT")
        return_master_name = payload.get("return_to_master_name") or msg.get("RETURN_TO_MASTER_NAME")
        if return_host is not None and return_port is not None:
            set_master_target(return_host, int(return_port), f"retorno para {return_master_name or 'master original'}")
            pending_temporary_registration = False
            print(
                f"[WORKER] Liberado pelo Master atual. Retornando para {return_master_name or return_host}:{return_port}."
            )
            return "reconnect"

    if message_type == "register_temporary_worker":
        print("[WORKER] Registro temporario confirmado pelo Master.")
        return None

    return None


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

    deadline = time.time() + HEARTBEAT_TIMEOUT
    while time.time() < deadline:
        remaining = max(0.05, min(0.5, deadline - time.time()))
        response = receive_with_timeout(sock, remaining)
        if response is None:
            continue

        action = handle_master_message(sock, response)
        if action == "reconnect":
            return "reconnect"

        if response.get("STATUS") == "ACK":
            print(f"[WORKER] ACK recebido da tarefa {task_id}.")
            return None

    print(f"[WORKER] Sem ACK explicito para a tarefa {task_id}.")
    return None


def handle_master_message(sock, msg):
    if not isinstance(msg, dict):
        return

    action = handle_control_message(msg)
    if action is not None:
        return action

    if msg.get("TASK") == "QUERY":
        return process_task(sock, msg)
    elif msg.get("TASK") == "NO_TASK":
        print("[WORKER] Master informou que nao ha tarefa na fila.")
    elif msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
        print("[WORKER] Heartbeat confirmado pelo Master.")

    if msg.get("STATUS") == "ACK":
        print("[WORKER] ACK confirmado pelo Master.")

    return None


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
    global original_master_uuid, current_master_uuid, last_registration_master_uuid, pending_temporary_registration

    temporary_registration = pending_temporary_registration
    payload = build_registration_message()
    send(sock, payload)

    response = receive_with_timeout(sock, HEARTBEAT_TIMEOUT)
    if not response:
        raise TimeoutError("Resposta invalida ou ausente do Master na solicitacao")

    if protocol_message_type(response) == "register_temporary_worker":
        return handle_master_message(sock, response)

    response_server_uuid = response.get("SERVER_UUID")
    if isinstance(response_server_uuid, str) and response_server_uuid.strip():
        current_master_uuid = response_server_uuid
        if original_master_uuid is None:
            original_master_uuid = response_server_uuid

    print(f"[WORKER] Apresentacao enviada: {payload}")

    action = handle_master_message(sock, response)
    if action == "reconnect":
        return "reconnect"

    if temporary_registration:
        pending_temporary_registration = False

    last_registration_master_uuid = current_master_uuid
    return "ok"


# ── Loop principal: heartbeat periodico com reconexao ────────
def run(host=None, port=None):
    global original_master_uuid, current_master_uuid, last_registration_master_uuid, original_master_name

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
                    # Registra o nome do master eleito para uso em register_temporary_worker.
                    if original_master_name is None:
                        original_master_name = selected_master["name"]

            action = register_with_master(sock)
            if action == "reconnect":
                try:
                    if sock is not None:
                        sock.close()
                except OSError:
                    pass
                sock = None
                consecutive_errors = 0
                continue

            if action == "ok":
                wait_deadline = time.time() + HEARTBEAT_INTERVAL
                while time.time() < wait_deadline:
                    remaining = max(0.05, min(0.5, wait_deadline - time.time()))
                    msg = receive_with_timeout(sock, remaining)
                    if msg is None:
                        continue

                    action = handle_master_message(sock, msg)
                    if action == "reconnect":
                        break

                if action == "reconnect":
                    try:
                        if sock is not None:
                            sock.close()
                    except OSError:
                        pass
                    sock = None
                    consecutive_errors = 0
                    continue

            if sock is None:
                raise TimeoutError("Resposta invalida ou ausente do Master na solicitacao")

            consecutive_errors = 0
            time.sleep(0.01)

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
