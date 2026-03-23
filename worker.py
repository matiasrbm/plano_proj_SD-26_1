import socket
import threading
import time
import random
import sys
import logging

import protocol as proto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker %(name)s] %(message)s",
    datefmt="%H:%M:%S"
)


class Worker:
    def __init__(self, wid, master_host, master_porta, meu_ip="0.0.0.0", minha_porta=0):
        self.id = wid
        self.master_host = master_host
        self.master_porta = master_porta
        # guarda endereço do master original para poder voltar após empréstimo
        self.master_original = (master_host, master_porta)
        self.meu_ip = meu_ip          # IP real reportado aos masters
        self.rodando = True
        self.log = logging.getLogger(wid)

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

    def loop_heartbeat(self, intervalo=30):
        """Envia heartbeat ao master atual a cada 30 segundos (conforme plano)."""
        while self.rodando:
            time.sleep(intervalo)
            try:
                conn = socket.create_connection(
                    (self.master_host, self.master_porta), timeout=3
                )
                proto.enviar(conn, proto.heartbeat(self.id))
                resp = proto.receber(conn)
                conn.close()
                if resp and resp.get("RESPONSE") == "ALIVE":
                    self.log.info("heartbeat: Status ALIVE")
                else:
                    self.log.warning("heartbeat: resposta inesperada")
            except Exception:
                self.log.warning("heartbeat: Status OFFLINE - Tentando Reconectar")

    def iniciar(self):
        self.registrar(self.master_host, self.master_porta)
        threading.Thread(target=self.loop_escuta, daemon=True).start()
        threading.Thread(target=self.loop_heartbeat, daemon=True).start()
        self.log.info("worker pronto")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("uso: python worker.py <id> <master_host> <master_porta> [meu_ip] [minha_porta]")
        sys.exit(1)

    wid         = sys.argv[1]
    mhost       = sys.argv[2]
    mport       = int(sys.argv[3])
    meu_ip      = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"
    minha_porta = int(sys.argv[5]) if len(sys.argv) > 5 else 0

    w = Worker(wid, mhost, mport, meu_ip=meu_ip, minha_porta=minha_porta)
    w.iniciar()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        w.rodando = False
        print(f"\nworker {wid} encerrado")
