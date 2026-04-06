"""
PC1 — Master A + Workers A1, A2, A3
IP local: 10.62.206.19
Vizinho:  10.62.206.211 (PC2 / Master B)

Fluxo esperado:
  1. Master A sobe na porta 9000 (threshold=3, baixo para saturar rápido)
  2. Workers A1-A3 se registram e conhecem seus peers entre si
  3. Aguarda Master B ficar no ar (PC2 deve ser iniciado em seguida)
  4. Inicia geração de carga a 2 req/s → satura → solicita ajuda ao Master B

Eleição:
  - Caso Master A fique OFFLINE, workers detectam após 4 falhas de heartbeat
    e iniciam eleição por consenso (critério: maior espaço livre em disco).
  - O limite de 4 falhas pode ser ajustado via HEARTBEAT_FALHAS_LIMITE abaixo.
"""

import threading
import time
from master import Master
from worker import Worker

MEU_IP    = "10.62.206.19"
IP_COLEGA = "10.62.206.211"

# ── Configuração de eleição ───────────────────────────────────────────────────
HEARTBEAT_INTERVALO   = 30   # segundos entre cada heartbeat
HEARTBEAT_FALHAS_LIMITE = 4  # falhas consecutivas antes de iniciar eleição


def main():
    print("=== PC1 — Master A + Workers A ===")

    master_a = Master(
        "A", MEU_IP, 9000,
        threshold=3,                    # threshold baixo para demonstrar saturação
        vizinhos=[(IP_COLEGA, 9100)]    # Master B do colega
    )
    master_a.iniciar(simular=False)
    time.sleep(1)

    # Portas fixas para que os workers se conheçam como peers de eleição
    portas_workers = {
        "A1": 9201,
        "A2": 9202,
        "A3": 9203,
    }

    print("Master A no ar. Registrando workers A1, A2, A3...")
    workers = []
    for wid, porta in portas_workers.items():
        w = Worker(
            wid, MEU_IP, 9000,
            meu_ip=MEU_IP,
            minha_porta=porta,
            heartbeat_intervalo=HEARTBEAT_INTERVALO,
            heartbeat_falhas_limite=HEARTBEAT_FALHAS_LIMITE,
        )
        workers.append(w)

    # Registra peers cruzados: cada worker conhece os demais
    for w in workers:
        for outro in workers:
            if outro.id != w.id:
                w.adicionar_peer(MEU_IP, portas_workers[outro.id])

    for w in workers:
        w.iniciar()
        time.sleep(0.2)

    time.sleep(1)
    print(f"Farm do Master A: {list(master_a.workers.keys())}")
    print("Aguardando Master B do colega ficar no ar (3 s)...")
    time.sleep(3)

    print("Iniciando carga a 2 req/s — observe os logs!")
    threading.Thread(target=master_a.simular_carga, args=(2.0,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for w in workers:
            w.rodando = False
        master_a.rodando = False
        print("\nPC1 encerrado")


if __name__ == "__main__":
    main()
