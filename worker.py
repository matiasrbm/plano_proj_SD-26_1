import socket
import threading
import time
import random
import sys
import shutil
import logging

import protocol as proto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker %(name)s] %(message)s",
    datefmt="%H:%M:%S"
)

# Número de falhas consecutivas de heartbeat antes de iniciar eleição.
# Configurável em tempo de criação do worker (padrão = 4).
HEARTBEAT_FALHAS_LIMITE = 4


class Worker:
    def __init__(
        self,
        wid,
        master_host,
        master_porta,
        meu_ip="0.0.0.0",
        minha_porta=0,
        heartbeat_intervalo=30,
        heartbeat_falhas_limite=HEARTBEAT_FALHAS_LIMITE,
    ):
        self.id = wid
        self.master_host = master_host
        self.master_porta = master_porta
        # guarda endereço do master original para poder voltar após empréstimo
        self.master_original = (master_host, master_porta)
        self.meu_ip = meu_ip          # IP real reportado aos masters
        self.rodando = True
        self.log = logging.getLogger(wid)

        # ── Configuração de heartbeat / eleição ───────────────────────────────
        self.heartbeat_intervalo     = heartbeat_intervalo
        self.heartbeat_falhas_limite = heartbeat_falhas_limite
        self._falhas_heartbeat       = 0   # contador de falhas consecutivas
        self._eleicao_em_andamento   = False
        self._eleicao_lock           = threading.Lock()

        # Lista de peers (outros workers) conhecidos: [(host, porta), ...]
        # Preenchida externamente antes de iniciar, ou via ELECTION_START recebido.
        self.peers: list = []
        self._peers_lock = threading.Lock()

        # servidor próprio para receber comandos do master
        # bind em 0.0.0.0 aceita conexões de qualquer interface
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("0.0.0.0", minha_porta))
        self.minha_porta = self.server.getsockname()[1]   # porta real se 0 foi passado
        self.server.listen(5)

    # ── Registro ──────────────────────────────────────────────────────────────

    def registrar(self, host, porta):
        """Registra-se como worker permanente em (host, porta)."""
        try:
            conn = socket.create_connection((host, porta), timeout=5)
            msg = proto.registrar_worker(self.id, self.id, self.meu_ip, self.minha_porta)
            proto.enviar(conn, msg)
            conn.close()
            self.log.info(f"registrado (normal) em {host}:{porta}")
        except Exception as e:
            self.log.error(f"erro ao registrar em {host}:{porta}: {e}")

    def registrar_temporario(self, host, porta, master_orig_host, master_orig_porta):
        """
        Registra-se como worker temporário em (host, porta).
        Informa o endereço do master original para facilitar a devolução.
        """
        try:
            conn = socket.create_connection((host, porta), timeout=5)
            msg = proto.registrar_temp(
                self.id, self.id,
                master_orig_host, master_orig_porta,
                self.meu_ip, self.minha_porta
            )
            proto.enviar(conn, msg)
            conn.close()
            self.log.info(f"registrado (temporário) em {host}:{porta}")
        except Exception as e:
            self.log.error(f"erro ao registrar temporário em {host}:{porta}: {e}")

    # ── Processamento de tarefas ──────────────────────────────────────────────

    def processar_tarefa(self, tid, duracao=None):
        if duracao is None:
            duracao = random.uniform(1, 3)
        self.log.info(f"processando tarefa {tid} por {duracao:.1f}s...")
        time.sleep(duracao)
        self.log.info(f"tarefa {tid} concluída")

    # ── Tratamento de mensagens ───────────────────────────────────────────────

    def tratar_conexao(self, conn, addr):
        try:
            msg = proto.receber(conn)
            if not msg:
                return

            task = msg.get("TASK")

            if task == proto.HEARTBEAT:
                # responde ALIVE conforme diagrama de sequência do plano
                proto.enviar(conn, proto.heartbeat_ok(self.id))

            elif task == proto.TASK_REQUEST:
                tid = msg.get("TASK_ID", "sem-id")
                dur = msg.get("DURATION", None)
                # processa em thread separada para não bloquear o servidor
                t = threading.Thread(
                    target=self.processar_tarefa, args=(tid, dur), daemon=True
                )
                t.start()
                proto.enviar(conn, proto.montar_msg(
                    proto.TASK_RESPONSE, self.id,
                    TASK_ID=tid, STATUS="ACCEPTED"
                ))

            elif task == proto.COMMAND_REDIRECT:
                # master A nos enviou para trabalhar temporariamente no master B
                novo_host = msg.get("NEW_MASTER_HOST")
                nova_porta = msg.get("NEW_MASTER_PORT")
                self.log.info(f"redirecionado para {novo_host}:{nova_porta}")

                orig_host, orig_porta = self.master_original
                self.master_host = novo_host
                self.master_porta = nova_porta

                # registra-se no master B como temporário, informando quem é o dono original
                self.registrar_temporario(
                    novo_host, nova_porta,
                    orig_host, orig_porta
                )

            elif task == proto.COMMAND_RELEASE:
                # master B nos libera; voltamos para o master A original
                self.log.info("liberado, voltando ao master original")
                h, p = self.master_original
                self.master_host = h
                self.master_porta = p

                # registra de volta no master A como worker permanente
                self.registrar(h, p)

                # notifica o master A que voltamos
                try:
                    conn2 = socket.create_connection((h, p), timeout=5)
                    proto.enviar(conn2, proto.avisar_retorno(self.id, self.id))
                    conn2.close()
                except Exception as e:
                    self.log.error(f"erro ao notificar retorno: {e}")

            # ── Mensagens de eleição ──────────────────────────────────────────

            elif task == proto.ELECTION_START:
                # Um peer iniciou uma eleição. Participamos enviando nosso voto.
                self._registrar_peer_da_mensagem(msg)
                disco = self._disco_livre()
                proto.enviar(conn, proto.votar(
                    self.id, disco, self.meu_ip, self.minha_porta
                ))
                self.log.info(
                    f"recebi ELECTION_START de {addr} — votei "
                    f"(disco_livre={disco // (1024**3):.1f} GB)"
                )

            elif task == proto.ELECTION_RESULT:
                # O consenso foi alcançado; atualiza master atual.
                novo_id   = msg.get("NEW_MASTER_ID")
                novo_host = msg.get("NEW_MASTER_HOST")
                novo_port = int(msg.get("NEW_MASTER_PORT"))
                self.log.info(
                    f"ELEIÇÃO CONCLUÍDA — novo master: {novo_id} "
                    f"em {novo_host}:{novo_port}"
                )
                # Só atualiza se não sou o novo master (ele já gerencia a si mesmo)
                if novo_id != self.id:
                    self.master_host  = novo_host
                    self.master_porta = novo_port
                    self._falhas_heartbeat = 0
                    self._eleicao_em_andamento = False
                    # Re-registra no novo master
                    threading.Thread(
                        target=self.registrar,
                        args=(novo_host, novo_port),
                        daemon=True
                    ).start()

        except Exception as e:
            self.log.error(f"erro ao tratar mensagem de {addr}: {e}")
        finally:
            conn.close()

    # ── Loops principais ──────────────────────────────────────────────────────

    def loop_escuta(self):
        self.log.info(f"escutando na porta {self.minha_porta}")
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

    def loop_heartbeat(self):
        """
        Envia heartbeat ao master atual a cada `heartbeat_intervalo` segundos.

        Regra de negócio de eleição:
        - A cada falha consecutiva, incrementa `_falhas_heartbeat`.
        - Ao atingir `heartbeat_falhas_limite` (padrão = 4), considera o master
          como DOWN e dispara o processo de eleição.
        - O contador é resetado a cada heartbeat bem-sucedido.
        """
        while self.rodando:
            time.sleep(self.heartbeat_intervalo)
            try:
                conn = socket.create_connection(
                    (self.master_host, self.master_porta), timeout=3
                )
                proto.enviar(conn, proto.heartbeat(self.id))
                resp = proto.receber(conn)
                conn.close()

                if resp and resp.get("RESPONSE") == "ALIVE":
                    self.log.info("heartbeat: Status ALIVE")
                    self._falhas_heartbeat = 0   # reseta contador
                else:
                    self.log.warning("heartbeat: resposta inesperada")
                    self._registrar_falha_heartbeat()

            except Exception:
                self.log.warning(
                    f"heartbeat: Status OFFLINE "
                    f"(falha {self._falhas_heartbeat + 1}/{self.heartbeat_falhas_limite})"
                )
                self._registrar_falha_heartbeat()

    # ── Eleição ───────────────────────────────────────────────────────────────

    def _registrar_falha_heartbeat(self):
        """
        Incrementa o contador de falhas. Ao atingir o limite configurável,
        inicia eleição (apenas uma vez — protegido por lock).
        """
        self._falhas_heartbeat += 1
        if self._falhas_heartbeat >= self.heartbeat_falhas_limite:
            with self._eleicao_lock:
                if not self._eleicao_em_andamento:
                    self._eleicao_em_andamento = True
                    self.log.warning(
                        f"Master OFFLINE confirmado após "
                        f"{self._falhas_heartbeat} falhas consecutivas — "
                        "iniciando eleição!"
                    )
                    threading.Thread(
                        target=self._conduzir_eleicao, daemon=True
                    ).start()

    def _disco_livre(self):
        """Retorna bytes livres na partição onde o processo está rodando."""
        return shutil.disk_usage("/").free

    def _registrar_peer_da_mensagem(self, msg):
        """Aprende endereço de um peer a partir de mensagem de eleição."""
        host = msg.get("CANDIDATE_HOST")
        port = msg.get("CANDIDATE_PORT")
        if host and port:
            entry = (host, int(port))
            with self._peers_lock:
                if entry not in self.peers:
                    self.peers.append(entry)

    def _conduzir_eleicao(self):
        """
        Protocolo de eleição por consenso:

        1. Este worker envia ELECTION_START para todos os peers conhecidos,
           informando seu próprio espaço livre em disco.
        2. Coleta as respostas (ELECTION_VOTE) com o espaço livre de cada peer.
        3. Inclui a si mesmo na contagem.
        4. O candidato com MAIOR espaço livre em disco é eleito novo master.
        5. Envia ELECTION_RESULT para todos os peers (broadcast de consenso).
        6. Se eleito, promove-se a master instanciando um objeto Master local.
        """
        self.log.info("=== ELEIÇÃO INICIADA ===")
        meu_disco = self._disco_livre()

        # Candidatos: {worker_id -> {"disco": bytes, "host": str, "porta": int}}
        candidatos = {
            self.id: {
                "disco": meu_disco,
                "host":  self.meu_ip,
                "porta": self.minha_porta,
            }
        }

        with self._peers_lock:
            peers_snapshot = list(self.peers)

        # Coleta votos dos peers
        for (peer_host, peer_port) in peers_snapshot:
            try:
                conn = socket.create_connection((peer_host, peer_port), timeout=5)
                proto.enviar(conn, proto.iniciar_eleicao(
                    self.id, meu_disco, self.meu_ip, self.minha_porta
                ))
                resp = proto.receber(conn)
                conn.close()

                if resp and resp.get("TASK") == proto.ELECTION_VOTE:
                    pid   = resp.get("SERVER_UUID", f"{peer_host}:{peer_port}")
                    pdisk = resp.get("DISCO_LIVRE", 0)
                    phost = resp.get("CANDIDATE_HOST", peer_host)
                    pport = resp.get("CANDIDATE_PORT", peer_port)
                    candidatos[pid] = {
                        "disco": pdisk,
                        "host":  phost,
                        "porta": int(pport),
                    }
                    self.log.info(
                        f"voto recebido de {pid} — "
                        f"disco_livre={pdisk // (1024**3):.1f} GB"
                    )
            except Exception as e:
                self.log.warning(f"peer {peer_host}:{peer_port} não respondeu: {e}")

        # Determina vencedor: maior espaço livre em disco
        eleito_id = max(candidatos, key=lambda wid: candidatos[wid]["disco"])
        eleito    = candidatos[eleito_id]

        self.log.info(
            f"=== CONSENSO ALCANÇADO === "
            f"Novo master: {eleito_id} "
            f"(disco_livre={eleito['disco'] // (1024**3):.1f} GB) "
            f"em {eleito['host']}:{eleito['porta']}"
        )

        # Broadcast do resultado para todos os peers
        resultado = proto.resultado_eleicao(
            self.id, eleito_id, eleito["host"], eleito["porta"]
        )
        for (peer_host, peer_port) in peers_snapshot:
            try:
                conn = socket.create_connection((peer_host, peer_port), timeout=5)
                proto.enviar(conn, resultado)
                conn.close()
            except Exception as e:
                self.log.warning(f"falha ao notificar {peer_host}:{peer_port}: {e}")

        # Atualiza estado local
        if eleito_id == self.id:
            self._promover_a_master(eleito["host"], eleito["porta"])
        else:
            self.master_host  = eleito["host"]
            self.master_porta = int(eleito["porta"])
            self._falhas_heartbeat     = 0
            self._eleicao_em_andamento = False
            # Registra no novo master
            self.registrar(self.master_host, self.master_porta)

    def _promover_a_master(self, host, porta):
        """
        Promove este worker a master: instancia um objeto Master na mesma
        porta em que o worker está escutando (ou porta configurável), e
        inicia seus loops. Os demais workers irão se registrar após receberem
        ELECTION_RESULT.
        """
        # Importação local para evitar dependência circular em casos simples
        from master import Master

        self.log.info(
            f"!!! SOU O NOVO MASTER — subindo servidor master em {host}:{porta} !!!"
        )

        novo_master = Master(
            mid=self.id,
            host=host,
            porta=porta,
        )
        # Passa os peers conhecidos como vizinhos do novo master
        with self._peers_lock:
            for (ph, pp) in self.peers:
                novo_master.vizinhos.append((ph, pp))

        novo_master.iniciar(simular=False)
        self._eleicao_em_andamento = False

        self.log.info("Worker promovido a master — loops do master ativos")

    # ── Gerenciamento de peers ────────────────────────────────────────────────

    def adicionar_peer(self, host, porta):
        """
        Registra um peer (outro worker) como participante de eleições futuras.
        Deve ser chamado externamente antes de `iniciar()`, ou pode ser
        chamado a qualquer momento durante a execução.
        """
        entry = (host, int(porta))
        with self._peers_lock:
            if entry not in self.peers:
                self.peers.append(entry)
                self.log.info(f"peer adicionado: {host}:{porta}")

    # ── Inicialização ─────────────────────────────────────────────────────────

    def iniciar(self):
        self.registrar(self.master_host, self.master_porta)
        threading.Thread(target=self.loop_escuta, daemon=True).start()
        threading.Thread(target=self.loop_heartbeat, daemon=True).start()
        self.log.info(
            f"worker pronto "
            f"(heartbeat a cada {self.heartbeat_intervalo}s, "
            f"limite={self.heartbeat_falhas_limite} falhas)"
        )


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "uso: python worker.py <id> <master_host> <master_porta> "
            "[meu_ip] [minha_porta] [hb_intervalo] [hb_falhas_limite]"
        )
        sys.exit(1)

    wid          = sys.argv[1]
    mhost        = sys.argv[2]
    mport        = int(sys.argv[3])
    meu_ip       = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"
    minha_porta  = int(sys.argv[5]) if len(sys.argv) > 5 else 0
    hb_intervalo = int(sys.argv[6]) if len(sys.argv) > 6 else 30
    hb_limite    = int(sys.argv[7]) if len(sys.argv) > 7 else HEARTBEAT_FALHAS_LIMITE

    w = Worker(
        wid, mhost, mport,
        meu_ip=meu_ip,
        minha_porta=minha_porta,
        heartbeat_intervalo=hb_intervalo,
        heartbeat_falhas_limite=hb_limite,
    )
    w.iniciar()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        w.rodando = False
        print(f"\nworker {wid} encerrado")
