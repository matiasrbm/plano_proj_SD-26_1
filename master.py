import socket
import threading
import time
import random
import sys
import logging

import protocol as proto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [master %(name)s] %(message)s",
    datefmt="%H:%M:%S"
)

THRESHOLD = 5


class Master:
    def __init__(self, mid, host, porta, threshold=THRESHOLD, vizinhos=None):
        self.id        = mid
        self.host      = host
        self.porta     = porta
        self.threshold = threshold
        self.vizinhos  = vizinhos or []

        # farm própria: wid -> {host, porta}
        self.workers     = {}
        # workers que peguei emprestado de outro master: wid -> {host, porta, original_host, original_porta}
        self.emprestados = {}
        # workers que emprestei para outro master: wid -> {host, porta}
        self.emprestei   = {}

        self.tarefas_ativas = 0   # contador de tarefas em andamento
        self.lock    = threading.Lock()
        self.rodando = True
        self.log     = logging.getLogger(mid)

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((host, porta))
        self.server.listen(20)

    # ── Acesso à farm ─────────────────────────────────────────────────────────

    def todos_workers(self):
        """Retorna union dos workers próprios + emprestados."""
        with self.lock:
            return {**self.workers, **self.emprestados}

    # ── Tratamento de conexões de entrada ────────────────────────────────────

    def tratar_conexao(self, conn, addr):
        try:
            msg = proto.receber(conn)
            if not msg:
                return

            task = msg.get("TASK")
            self.log.info(f"recebi {task} de {addr}")

            if task == proto.HEARTBEAT:
                proto.enviar(conn, proto.heartbeat_ok(self.id))

            elif task == proto.REGISTER_WORKER:
                wid   = msg["WORKER_ID"]
                whost = msg["WORKER_HOST"]
                wport = int(msg["WORKER_PORT"])
                with self.lock:
                    self.workers[wid] = {"host": whost, "porta": wport}
                self.log.info(
                    f"worker {wid} entrou na farm [{whost}:{wport}] "
                    f"(total: {len(self.workers)})"
                )

            elif task == proto.REGISTER_TEMP:
                # worker chegou temporariamente vindo de outro master
                wid        = msg["WORKER_ID"]
                whost      = msg.get("WORKER_HOST")
                wport      = msg.get("WORKER_PORT")
                orig_host  = msg.get("ORIGINAL_MASTER_HOST")
                orig_porta = msg.get("ORIGINAL_MASTER_PORT")

                if not whost or not wport:
                    self.log.warning(
                        f"REGISTER_TEMP de {wid} sem WORKER_HOST/PORT — ignorando"
                    )
                    return

                with self.lock:
                    self.emprestados[wid] = {
                        "host":          whost,
                        "porta":         int(wport),
                        "original_host": orig_host,
                        "original_porta": orig_porta,
                    }
                self.log.info(
                    f"worker temporário {wid} chegou [{whost}:{wport}] "
                    f"(emprestados: {len(self.emprestados)})"
                )

            elif task == proto.REQUEST_HELP:
                self.responder_pedido_ajuda(conn, msg)
                return   # conexão já tratada dentro do método

            elif task == proto.RESPONSE_REJECTED:
                self.log.warning(f"vizinho recusou ajuda: {msg.get('REASON')}")

            elif task == proto.NOTIFY_RETURNED:
                # worker voltou ao nosso controle após ser emprestado
                wid = msg.get("WORKER_ID")
                with self.lock:
                    info = self.emprestei.pop(wid, None)
                    if wid not in self.workers:
                        # reinsere com as informações que tínhamos antes de emprestar
                        self.workers[wid] = info or {}
                self.log.info(f"worker {wid} voltou para a farm")

        except Exception as e:
            self.log.error(f"erro ao tratar mensagem de {addr}: {e}")
        finally:
            conn.close()

    # ── Protocolo de Conversa Consensual ─────────────────────────────────────

    def responder_pedido_ajuda(self, conn, msg):
        """
        Responde a um REQUEST_HELP de outro master.
        Só empresta se tivermos mais de 1 worker na farm própria.
        """
        host_solicitante  = msg.get("MASTER_HOST")
        porta_solicitante = msg.get("MASTER_PORT")

        with self.lock:
            posso_ajudar = len(self.workers) > 1

        if posso_ajudar:
            with self.lock:
                # empresta no máximo 2, mantendo pelo menos 1 na farm
                qtd = min(2, len(self.workers) - 1)
                ids = list(self.workers.keys())[:qtd]
                info_workers = []
                for wid in ids:
                    info = self.workers.pop(wid)
                    self.emprestei[wid] = info
                    info_workers.append({"WORKER_ID": wid, **info})

            proto.enviar(conn, proto.aceitar_ajuda(self.id, info_workers))
            conn.close()

            # redireciona fisicamente cada worker para o master solicitante
            for w in info_workers:
                self.redirecionar_worker(w, host_solicitante, porta_solicitante)
        else:
            proto.enviar(conn, proto.rejeitar_ajuda(self.id))
            conn.close()
            self.log.info("recusei ajuda — sem workers disponíveis para emprestar")

    def redirecionar_worker(self, winfo, novo_host, nova_porta):
        """Envia COMMAND_REDIRECT para o worker ir trabalhar em outro master."""
        whost = winfo.get("host")
        wport = winfo.get("porta")
        wid   = winfo.get("WORKER_ID")
        if not whost or not wport:
            self.log.warning(f"sem endereço para redirecionar worker {wid}")
            return
        try:
            conn = socket.create_connection((whost, wport), timeout=5)
            proto.enviar(conn, proto.redirecionar(self.id, novo_host, nova_porta))
            conn.close()
            self.log.info(f"redirecionei {wid} → {novo_host}:{nova_porta}")
        except Exception as e:
            self.log.error(f"erro ao redirecionar {wid}: {e}")

    # ── Despacho de tarefas ───────────────────────────────────────────────────

    def despachar_tarefa(self, tid, duracao):
        """Escolhe um worker aleatório e envia a tarefa. Retenta até conseguir."""
        with self.lock:
            self.tarefas_ativas += 1

        despachado = False
        while self.rodando and not despachado:
            todos = self.todos_workers()
            if todos:
                wid  = random.choice(list(todos.keys()))
                info = todos[wid]
                try:
                    conn = socket.create_connection((info["host"], info["porta"]), timeout=5)
                    proto.enviar(conn, proto.montar_msg(
                        proto.TASK_REQUEST, self.id,
                        TASK_ID=tid, DURATION=duracao
                    ))
                    resp = proto.receber(conn)
                    conn.close()
                    status = resp.get("STATUS", "?") if resp else "sem-resposta"
                    self.log.info(f"tarefa {tid} → worker {wid} [{status}]")
                    despachado = True
                except Exception as e:
                    self.log.error(f"erro ao enviar tarefa para {wid}: {e}")
            if not despachado:
                time.sleep(0.5)

        # aguarda o processamento para manter o contador de carga correto
        time.sleep(duracao)
        with self.lock:
            self.tarefas_ativas -= 1

    # ── Monitor de saturação ──────────────────────────────────────────────────

    def monitor_saturacao(self):
        """
        Verifica carga a cada 2 segundos.
        - Satura  (carga > threshold)           → pede ajuda ao vizinho
        - Normaliza (carga <= threshold // 2)   → devolve workers emprestados
        """
        pedindo_ajuda = False
        while self.rodando:
            time.sleep(2)
            with self.lock:
                carga = self.tarefas_ativas

            if carga > self.threshold and not pedindo_ajuda:
                pedindo_ajuda = True
                self.log.warning(
                    f"SATURADO! tarefas_ativas={carga} > threshold={self.threshold}"
                )
                self.pedir_ajuda()

            elif carga <= self.threshold // 2 and pedindo_ajuda:
                pedindo_ajuda = False
                self.log.info(
                    f"carga normalizada ({carga} ≤ {self.threshold // 2}), "
                    "devolvendo workers emprestados..."
                )
                self.devolver_workers()

    def pedir_ajuda(self):
        """Contata cada vizinho em ordem até alguém aceitar."""
        for host, porta in self.vizinhos:
            try:
                self.log.info(f"solicitando ajuda a {host}:{porta}")
                conn = socket.create_connection((host, porta), timeout=5)
                with self.lock:
                    carga = self.tarefas_ativas
                proto.enviar(conn, proto.pedir_ajuda(
                    self.id, carga, self.threshold, self.host, self.porta
                ))
                resp = proto.receber(conn)
                conn.close()
                if resp and resp.get("TASK") == proto.RESPONSE_ACCEPTED:
                    workers_vindo = resp.get("WORKERS", [])
                    self.log.info(
                        f"ajuda aceita por {host}:{porta} — "
                        f"{len(workers_vindo)} worker(s) a caminho"
                    )
                    return
                else:
                    self.log.info(f"{host}:{porta} recusou ajuda")
            except Exception as e:
                self.log.error(f"erro ao contatar vizinho {host}:{porta}: {e}")

    def devolver_workers(self):
        """
        Envia COMMAND_RELEASE para cada worker emprestado, instruindo-o a
        voltar ao seu master original.
        """
        with self.lock:
            lista = list(self.emprestados.items())

        for wid, info in lista:
            whost = info.get("host")
            wport = info.get("porta")
            if whost and wport:
                try:
                    conn = socket.create_connection((whost, wport), timeout=5)
                    proto.enviar(conn, proto.liberar_worker(self.id, wid))
                    conn.close()
                    self.log.info(f"liberado worker {wid} → retorna ao master original")
                except Exception as e:
                    self.log.error(f"erro ao liberar {wid}: {e}")
            with self.lock:
                self.emprestados.pop(wid, None)

    # ── Simulação de carga ────────────────────────────────────────────────────

    def simular_carga(self, rps=1.0):
        delay = 1.0 / rps
        self.log.info(f"simulando carga a {rps} req/s")
        n = 0
        while self.rodando:
            time.sleep(delay)
            n += 1
            tid     = f"{self.id}-T{n:04d}"
            duracao = random.uniform(2, 4)   # tarefas longas para acumular carga
            with self.lock:
                carga = self.tarefas_ativas
            self.log.info(f"nova requisição {tid} (tarefas_ativas={carga})")
            t = threading.Thread(
                target=self.despachar_tarefa, args=(tid, duracao), daemon=True
            )
            t.start()

    # ── Inicialização ─────────────────────────────────────────────────────────

    def loop_escuta(self):
        self.log.info(f"escutando em {self.host}:{self.porta}")
        while self.rodando:
            try:
                self.server.settimeout(1.0)
                conn, addr = self.server.accept()
                t = threading.Thread(
                    target=self.tratar_conexao, args=(conn, addr), daemon=True
                )
                t.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.rodando:
                    self.log.error(f"erro no accept: {e}")

    def iniciar(self, simular=True, rps=1.0):
        threading.Thread(target=self.loop_escuta, daemon=True).start()
        threading.Thread(target=self.monitor_saturacao, daemon=True).start()
        if simular:
            threading.Thread(target=self.simular_carga, args=(rps,), daemon=True).start()
        self.log.info(
            f"master '{self.id}' pronto | porta={self.porta} | threshold={self.threshold}"
        )


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("uso: python master.py <id> <host> <porta> [vizinho_host:vizinho_porta ...]")
        sys.exit(1)

    mid   = sys.argv[1]
    mhost = sys.argv[2]
    mport = int(sys.argv[3])

    vizinhos = []
    for arg in sys.argv[4:]:
        h, p = arg.split(":")
        vizinhos.append((h, int(p)))

    m = Master(mid, mhost, mport, vizinhos=vizinhos)
    m.iniciar(simular=True, rps=0.5)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        m.rodando = False
        print(f"\nmaster {mid} encerrado")
