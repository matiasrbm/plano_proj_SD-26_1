"""
PC2 — Master B + Workers B1, B2, B3
IP local: 10.62.206.211
Vizinho:  10.62.206.19 (PC1 / Master A)

Fluxo esperado:
  1. Master B sobe na porta 9100 (threshold=9, alto para nunca saturar sozinho)
  2. Workers B1-B3 se registram e conhecem seus peers entre si
  3. Aguarda PC1 saturar e pedir ajuda
  4. Master B empresta até 2 workers → eles se registram temporariamente no Master A
  5. Quando a carga do Master A normalizar, os workers são devolvidos

Eleição:
  - Caso Master B fique OFFLINE, workers detectam após 4 falhas de heartbeat
    e iniciam eleição por consenso (critério: maior espaço livre em disco).
  - O limite de 4 falhas pode ser ajustado via HEARTBEAT_FALHAS_LIMITE abaixo.
"""

import time
from master import Master
from worker import Worker

MEU_IP    = "10.62.206.211"
IP_COLEGA = "10.62.206.19"

# ── Configuração de eleição ───────────────────────────────────────────────────
HEARTBEAT_INTERVALO     = 30   # segundos entre cada heartbeat
HEARTBEAT_FALHAS_LIMITE = 4    # falhas consecutivas antes de iniciar eleição


def main():
    print("=== PC2 — Master B + Workers B ===")

    master_b = Master(
        "B", MEU_IP, 9100,
        threshold=9,                    # threshold alto: Master B raramente satura
        vizinhos=[(IP_COLEGA, 9000)]    # Master A do colega
    )
    master_b.iniciar(simular=False)
    time.sleep(1)

    # Portas fixas para que os workers se conheçam como peers de eleição
    portas_workers = {
        "B1": 9301,
        "B2": 9302,
        "B3": 9303,
    }

    print("Master B no ar. Registrando workers B1, B2, B3...")
    workers = []
    for wid, porta in portas_workers.items():
        w = Worker(
            wid, MEU_IP, 9100,
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
    print(f"Farm do Master B: {list(master_b.workers.keys())}")
    print("Aguardando PC1 saturar e solicitar ajuda...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for w in workers:
            w.rodando = False
        master_b.rodando = False
        print("\nPC2 encerrado")


if __name__ == "__main__":
    main()
