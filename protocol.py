import json

# tipos de mensagem definidos no protocolo
HEARTBEAT          = "HEARTBEAT"
REGISTER_WORKER    = "REGISTER_WORKER"
TASK_REQUEST       = "TASK_REQUEST"
TASK_RESPONSE      = "TASK_RESPONSE"
REQUEST_HELP       = "REQUEST_HELP"
RESPONSE_ACCEPTED  = "RESPONSE_ACCEPTED"
RESPONSE_REJECTED  = "RESPONSE_REJECTED"
COMMAND_REDIRECT   = "COMMAND_REDIRECT"
REGISTER_TEMP      = "REGISTER_TEMPORARY_WORKER"
COMMAND_RELEASE    = "COMMAND_RELEASE"
NOTIFY_RETURNED    = "NOTIFY_WORKER_RETURNED"


def montar_msg(task, server_id, **extras):
    """Monta um dicionário base com os campos obrigatórios do protocolo."""
    msg = {"SERVER_UUID": server_id, "TASK": task}
    msg.update(extras)
    return msg


# ── Heartbeat ────────────────────────────────────────────────────────────────

def heartbeat(sid):
    return montar_msg(HEARTBEAT, sid)

def heartbeat_ok(sid):
    # RESPONSE="ALIVE" conforme diagrama de sequência do plano
    return montar_msg(HEARTBEAT, sid, RESPONSE="ALIVE")


# ── Registro de workers ───────────────────────────────────────────────────────

def registrar_worker(sid, wid, host, porta):
    return montar_msg(REGISTER_WORKER, sid,
                      WORKER_ID=wid, WORKER_HOST=host, WORKER_PORT=porta)

def registrar_temp(sid, wid, master_original_host, master_original_porta, meu_host, minha_porta):
    """
    Worker avisa o master B que chegou temporariamente.
    Inclui WORKER_HOST/WORKER_PORT para que o master B saiba como contactar o worker.
    Inclui ORIGINAL_MASTER_HOST/PORT para que o master B saiba para onde devolver.
    """
    return montar_msg(REGISTER_TEMP, sid,
                      WORKER_ID=wid,
                      WORKER_HOST=meu_host,
                      WORKER_PORT=minha_porta,
                      ORIGINAL_MASTER_HOST=master_original_host,
                      ORIGINAL_MASTER_PORT=master_original_porta)


# ── Pedido de ajuda entre masters ─────────────────────────────────────────────

def pedir_ajuda(sid, pendentes, threshold, meu_host, minha_porta):
    return montar_msg(REQUEST_HELP, sid,
                      PENDING=pendentes, THRESHOLD=threshold,
                      MASTER_HOST=meu_host, MASTER_PORT=minha_porta)

def aceitar_ajuda(sid, workers):
    return montar_msg(RESPONSE_ACCEPTED, sid, WORKERS=workers)

def rejeitar_ajuda(sid, motivo="BUSY"):
    return montar_msg(RESPONSE_REJECTED, sid, REASON=motivo)


# ── Redirecionamento e devolução ──────────────────────────────────────────────

def redirecionar(sid, novo_host, nova_porta):
    return montar_msg(COMMAND_REDIRECT, sid,
                      NEW_MASTER_HOST=novo_host, NEW_MASTER_PORT=nova_porta)

def liberar_worker(sid, wid):
    """Master B manda worker voltar ao master A original."""
    return montar_msg(COMMAND_RELEASE, sid, WORKER_ID=wid)

def avisar_retorno(sid, wid):
    """Worker avisa o master A que voltou."""
    return montar_msg(NOTIFY_RETURNED, sid, WORKER_ID=wid)


# ── Transporte ────────────────────────────────────────────────────────────────

def enviar(sock, dados):
    """Serializa em JSON e envia com delimitador \\n conforme protocolo."""
    raw = json.dumps(dados) + "\n"
    sock.sendall(raw.encode())


def receber(conn):
    """Lê até encontrar o delimitador \\n e retorna o JSON decodificado."""
    buf = b""
    while True:
        pedaco = conn.recv(4096)
        if not pedaco:
            return None
        buf += pedaco
        if b"\n" in buf:
            linha, _ = buf.split(b"\n", 1)
            return json.loads(linha.decode())
