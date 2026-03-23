"""
PC2 — Master B + Workers B1, B2, B3
IP local: 10.62.206.211
Vizinho:  10.62.206.19 (PC1 / Master A)

Fluxo esperado:
  1. Master B sobe na porta 9100 (threshold=9, alto para nunca saturar sozinho)
  2. Workers B1-B3 se registram
  3. Aguarda PC1 saturar e pedir ajuda
  4. Master B empresta até 2 workers → eles se registram temporariamente no Master A
  5. Quando a carga do Master A normalizar, os workers são devolvidos
"""

import time
from master import Master
from worker import Worker

MEU_IP    = "10.62.206.211"
IP_COLEGA = "10.62.206.19"


def main():
    print("=== PC2 — Master B + Workers B ===")

    master_b = Master(
        "B", MEU_IP, 9100,
        threshold=9,                    # threshold alto: Master B raramente satura
        vizinhos=[(IP_COLEGA, 9000)]    # Master A do colega
    )
    master_b.iniciar(simular=False)
    time.sleep(1)

    print("Master B no ar. Registrando workers B1, B2, B3...")
    workers = []
    for i in range(1, 4):
        w = Worker(f"B{i}", MEU_IP, 9100, meu_ip=MEU_IP, minha_porta=9300 + i)
        w.iniciar()
        workers.append(w)
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
