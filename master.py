#!/usr/bin/env python3
# ============================================================
#  master.py
#  - Aceita conexoes de Workers
#  - Responde a descoberta UDP com nome e porta do Master
#  - Confirma a eleicao via TCP antes do fluxo de heartbeat
#  - Gera requisicoes e ENVIA tarefas para os Workers
#  - Monitora saturacao e negocia com Master vizinho
# ============================================================

import json
import socket
import threading
import time
import uuid

from config import (
    SERVER_UUID,
    MASTER_NAME,
    MASTER_HOST,
    MASTER_BIND_HOST,
    MASTER_PORT,
    DISCOVERY_PORT,
    LOAD_THRESHOLD,
    TASK_DURATION,
    REQUEST_INTERVAL,
    NEIGHBOR_MASTERS,
    SPRINT1_HEARTBEAT_ONLY,
)

# ── Estado global ────────────────────────────────────────────
workers = {}          # { worker_uuid: socket }
pending = 0           # requisicoes ainda nao atribuidas
pending_lock = threading.Lock()
task_queue = []       # fila de tarefas aguardando worker
task_queue_lock = threading.Lock()


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


def decode_datagram(data):
    try:
        return json.loads(data.decode().strip())
    except Exception:
        return None


def valid_heartbeat(msg):
    if not isinstance(msg, dict):
        return False
    if msg.get("TASK") == "HEARTBEAT":
        server_uuid = msg.get("SERVER_UUID")
        if not isinstance(server_uuid, str) or not server_uuid.strip():
            return False
        return True
    if msg.get("WORKER") == "ALIVE":
        worker_uuid = msg.get("WORKER_UUID")
        if not isinstance(worker_uuid, str) or not worker_uuid.strip():
            return False
        return True
    return False


def build_alive_response():
    return {"SERVER_UUID": SERVER_UUID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}


def valid_discovery_request(msg):
    if not isinstance(msg, dict):
        return False
    if msg.get("TYPE") != "DISCOVERY":
        return False
    worker_uuid = msg.get("WORKER_UUID")
    return isinstance(worker_uuid, str) and worker_uuid.strip()


def build_discovery_reply():
    return {
        "TYPE": "DISCOVERY_REPLY",
        "MASTER_NAME": MASTER_NAME,
        "MASTER_IP": MASTER_HOST,
        "MASTER_PORT": MASTER_PORT,
        "STATUS": "AVAILABLE",
        "SERVER_UUID": SERVER_UUID,
    }


def valid_election_ack(msg):
    if not isinstance(msg, dict):
        return False
    if msg.get("TYPE") != "ELECTION_ACK":
        return False
    worker_uuid = msg.get("WORKER_UUID")
    selected_master = msg.get("SELECTED_MASTER")
    if not isinstance(worker_uuid, str) or not worker_uuid.strip():
        return False
    if not isinstance(selected_master, str) or not selected_master.strip():
        return False
    return True


def build_election_ack_response(worker_uuid):
    return {
        "TYPE": "ELECTION_ACK",
        "STATUS": "ACCEPTED",
        "MASTER_NAME": MASTER_NAME,
        "MASTER_IP": MASTER_HOST,
        "MASTER_PORT": MASTER_PORT,
        "SERVER_UUID": SERVER_UUID,
        "WORKER_UUID": worker_uuid,
    }


def borrowed_worker(msg):
    server_uuid = msg.get("SERVER_UUID")
    return isinstance(server_uuid, str) and server_uuid.strip() and server_uuid != SERVER_UUID


def enqueue_task(task_id, user, force_nok=False):
    # Fila da Sprint 2: cada item guarda a tarefa e metadados de simulacao.
    with task_queue_lock:
        task_queue.append({"TASK_ID": task_id, "USER": user, "FORCE_NOK": force_nok})


def dequeue_task():
    with task_queue_lock:
        if not task_queue:
            return None
        return task_queue.pop(0)


def dispatch_next_task(conn, worker_uuid):
    task = dequeue_task()
    if task is None:
        # Quando nao ha trabalho pendente, o Master responde explicitamente.
        send(conn, {"TASK": "NO_TASK", "SERVER_UUID": SERVER_UUID})
        print(f"[MASTER] Sem tarefa para o Worker {worker_uuid[:8]}.")
        return

    # Quando ha fila, o Master entrega uma QUERY para o Worker processar.
    send(
        conn,
        {
            "TASK": "QUERY",
            "USER": task["USER"],
            "TASK_ID": task["TASK_ID"],
            "FORCE_NOK": task["FORCE_NOK"],
            "SERVER_UUID": SERVER_UUID,
        },
    )
    print(f"[MASTER] Enviando {task['TASK_ID']} para Worker {worker_uuid[:8]} | USER={task['USER']}")


def valid_status_report(msg):
    if not isinstance(msg, dict):
        return False

    required = ("STATUS", "TASK", "WORKER_UUID")
    for key in required:
        if key not in msg:
            return False

    status = msg.get("STATUS")
    task = msg.get("TASK")
    worker_uuid = msg.get("WORKER_UUID")

    if status not in {"OK", "NOK"}:
        return False
    if task != "QUERY":
        return False
    if not isinstance(worker_uuid, str) or not worker_uuid.strip():
        return False

    return True


def handle_worker(worker_uuid, conn, first_msg=None):
    global pending
    msg = first_msg
    while True:
        if msg is None:
            msg = receive(conn)
        if msg is None:
            print(f"[MASTER] Worker {worker_uuid[:8]} desconectou.")
            workers.pop(worker_uuid, None)
            conn.close()
            return

        if msg.get("TYPE") == "ELECTION_ACK":
            if not valid_election_ack(msg):
                print(f"[MASTER] ELECTION_ACK invalido de {worker_uuid[:8]}. Encerrando conexao.")
                workers.pop(worker_uuid, None)
                conn.close()
                return
            if msg.get("SELECTED_MASTER") != MASTER_NAME:
                print(
                    f"[MASTER] Worker {worker_uuid[:8]} escolheu {msg.get('SELECTED_MASTER')} em vez de {MASTER_NAME}."
                )
                send(
                    conn,
                    {
                        "TYPE": "ELECTION_ACK",
                        "STATUS": "REJECTED",
                        "MASTER_NAME": MASTER_NAME,
                        "SERVER_UUID": SERVER_UUID,
                        "WORKER_UUID": worker_uuid,
                    },
                )
                workers.pop(worker_uuid, None)
                conn.close()
                return

            workers[worker_uuid] = conn
            print(f"[MASTER] ELECTION_ACK recebido de {worker_uuid[:8]} para {MASTER_NAME}.")
            send(conn, build_election_ack_response(worker_uuid))
            msg = None
            continue

        if msg.get("TASK") == "HEARTBEAT" or msg.get("WORKER") == "ALIVE":
            if not valid_heartbeat(msg):
                print("[MASTER] HEARTBEAT/APRESENTACAO invalido: campos obrigatorios ausentes.")
                workers.pop(worker_uuid, None)
                conn.close()
                return
            if msg.get("WORKER") == "ALIVE":
                origem = "emprestado" if borrowed_worker(msg) else "local"
                workers[worker_uuid] = conn
                print(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentado.")
                # Na apresentacao, o Master ja libera a primeira tarefa da fila.
                dispatch_next_task(conn, worker_uuid)
            else:
                send(conn, build_alive_response())

                # Apos o heartbeat, o Master pode liberar outra tarefa, se houver.
                dispatch_next_task(conn, worker_uuid)

        elif "STATUS" in msg or msg.get("TASK") == "QUERY":
            if not valid_status_report(msg):
                print(f"[MASTER] Status invalido de {worker_uuid[:8]}. Encerrando conexao.")
                workers.pop(worker_uuid, None)
                conn.close()
                return

            task_id = msg.get("TASK_ID", "SEM_ID")
            status = msg.get("STATUS")
            with pending_lock:
                pending = max(0, pending - 1)
            print(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]} com status {status}. Pendentes: {pending}")
            # O ACK final fecha o ciclo de confirmacao pedido na Sprint 2.
            send(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid, "TASK_ID": task_id})

        elif msg.get("TASK") == "task_done":
            task_id = msg.get("TASK_ID")
            with pending_lock:
                pending = max(0, pending - 1)
            print(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]}. Pendentes: {pending}")

        elif msg.get("TASK") == "register_worker" or msg.get("TASK") == "register_temporary_worker":
            # Worker temporario se registrou.
            wid = msg.get("WORKER_UUID", worker_uuid)
            workers[wid] = conn
            print(f"[MASTER] Worker {'temporario ' if 'temporary' in msg.get('TASK','') else ''}{wid[:8]} registrado.")

        msg = None


def discovery_loop():
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.bind((MASTER_BIND_HOST, DISCOVERY_PORT))
    print(f"[MASTER] Descoberta UDP ativa em {MASTER_BIND_HOST}:{DISCOVERY_PORT} | nome={MASTER_NAME}")

    while True:
        data, addr = udp_server.recvfrom(4096)
        msg = decode_datagram(data)
        if not valid_discovery_request(msg):
            print(f"[MASTER] Discovery invalido de {addr}. Ignorando.")
            continue

        reply = build_discovery_reply()
        sendto_payload = (json.dumps(reply) + "\n").encode()
        udp_server.sendto(sendto_payload, addr)
        print(f"[MASTER] Discovery reply enviado para {msg['WORKER_UUID'][:8]} em {addr[0]}:{addr[1]}.")


# ── Aceita conexoes de Workers ───────────────────────────────
def accept_loop():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MASTER_BIND_HOST, MASTER_PORT))
    server.listen(20)
    print(f"[MASTER] Escutando em {MASTER_BIND_HOST}:{MASTER_PORT} | anunciado como {MASTER_HOST}:{MASTER_PORT}")

    while True:
        conn, addr = server.accept()
        msg = receive(conn)
        if msg is None:
            conn.close()
            continue

        task = msg.get("TASK", "")
        worker_uuid = msg.get("WORKER_UUID") or msg.get("SERVER_UUID") or str(uuid.uuid4())

        # Heartbeat, apresentacao ou primeiro ACK da eleicao do Worker.
        if task == "HEARTBEAT" or msg.get("WORKER") == "ALIVE" or msg.get("TYPE") == "ELECTION_ACK":
            if msg.get("TYPE") == "ELECTION_ACK":
                print(f"[MASTER] Worker {worker_uuid[:8]} iniciou handshake de eleicao de {addr}.")
            elif msg.get("WORKER") == "ALIVE":
                origem = "emprestado" if borrowed_worker(msg) else "proprio"
                print(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentou-se de {addr}.")
            else:
                print(f"[MASTER] Heartbeat recebido de {worker_uuid[:8]}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, msg), daemon=True).start()

        # Worker se registrando.
        elif "register" in task:
            workers[worker_uuid] = conn
            kind = "temporario" if "temporary" in task else "proprio"
            print(f"[MASTER] Worker {kind} {worker_uuid[:8]} conectado de {addr}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, msg), daemon=True).start()

        # Master vizinho pedindo ajuda.
        elif task == "request_help":
            if SPRINT1_HEARTBEAT_ONLY:
                print("[MASTER] Modo Sprint 1: ignorando request_help.")
                conn.close()
                continue
            handle_help_request(conn, msg)

        # Master vizinho liberando workers emprestados.
        elif task == "command_release":
            if SPRINT1_HEARTBEAT_ONLY:
                print("[MASTER] Modo Sprint 1: ignorando command_release.")
                conn.close()
                continue
            print("[MASTER] Vizinho liberou os workers. Redirecionando de volta.")
            conn.close()


# ── Gera requisicoes e envia para workers disponiveis ────────
def load_generator():
    global pending
    users = ["User1", "User2", "User3", "User4"]
    count = 0
    while True:
        time.sleep(REQUEST_INTERVAL)

        task_id = f"TASK-{count:04d}"
        user = users[count % len(users)]
        force_nok = (count % 5 == 0)
        count += 1

        # Em vez de enviar direto, a Sprint 2 usa uma fila interna de tarefas.
        enqueue_task(task_id, user, force_nok)

        with pending_lock:
            pending += 1
            current_pending = pending

        print(f"[MASTER] Tarefa {task_id} enfileirada para {user}. Pendentes: {current_pending}")

        # Verifica saturacao da mesma forma, agora com base na fila.
        if current_pending > LOAD_THRESHOLD:
            print(f"[MASTER] SATURADO! ({current_pending} pendentes). Pedindo ajuda...")
            threading.Thread(target=ask_for_help, daemon=True).start()


# ── Pede ajuda a um Master vizinho ───────────────────────────
def ask_for_help():
    for (host, port) in NEIGHBOR_MASTERS:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            send(sock, {"SERVER_UUID": SERVER_UUID, "TASK": "request_help", "MASTER_PORT": MASTER_PORT})

            resp = receive(sock)
            if resp and resp.get("TASK") == "response_accepted":
                print(f"[MASTER] Vizinho {host}:{port} aceitou ajudar!")
                sock.close()
                return
            else:
                print(f"[MASTER] Vizinho {host}:{port} rejeitou.")
                sock.close()
        except OSError as e:
            print(f"[MASTER] Nao conectou ao vizinho {host}:{port} — {e}")


# ── Responde pedido de ajuda de outro Master ─────────────────
def handle_help_request(conn, msg):
    if len(workers) > 1:
        # Empresta 1 worker.
        w_uuid = list(workers.keys())[0]
        w_sock = workers[w_uuid]
        requester_host, _ = conn.getpeername()
        requester_port = msg.get("MASTER_PORT", MASTER_PORT)

        print(f"[MASTER] Aceitei ajudar. Redirecionando Worker {w_uuid[:8]}.")
        send(conn, {"SERVER_UUID": SERVER_UUID, "TASK": "response_accepted", "WORKERS_TO_SEND": 1})
        send(w_sock, {"TASK": "command_redirect", "NEW_MASTER_HOST": requester_host, "NEW_MASTER_PORT": requester_port})
        workers.pop(w_uuid, None)
    else:
        print("[MASTER] Sem workers para emprestar. Rejeitando.")
        send(conn, {"SERVER_UUID": SERVER_UUID, "TASK": "response_rejected"})
    conn.close()


# ── Entry point ──────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[MASTER] Iniciando | UUID: {SERVER_UUID} | nome: {MASTER_NAME}")
    print(f"[MASTER] Suba workers com: python worker.py 127.0.0.1 {MASTER_PORT}")
    threading.Thread(target=discovery_loop, daemon=True).start()
    if SPRINT1_HEARTBEAT_ONLY:
        print("[MASTER] Modo Sprint 1 ativo: apenas HEARTBEAT para demonstracao.")
        accept_loop()
    else:
        threading.Thread(target=accept_loop, daemon=True).start()
        load_generator()
