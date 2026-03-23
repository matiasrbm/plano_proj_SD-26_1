"""
PC1 — Master A + Workers A1, A2, A3
IP local: 10.62.206.19
Vizinho:  10.62.206.211 (PC2 / Master B)

Fluxo esperado:
  1. Master A sobe na porta 9000 (threshold=3, baixo para saturar rápido)
  2. Workers A1-A3 se registram
  3. Aguarda Master B ficar no ar (PC2 deve ser iniciado em seguida)
  4. Inicia geração de carga a 2 req/s → satura → solicita ajuda ao Master B
"""

import threading
import time
from master import Master
from worker import Worker

MEU_IP    = "10.62.206.19"
IP_COLEGA = "10.62.206.211"


def main():
    print("=== PC1 — Master A + Workers A ===")

    master_a = Master(
        "A", MEU_IP, 9000,
        threshold=3,                    # threshold baixo para demonstrar saturação
        vizinhos=[(IP_COLEGA, 9100)]    # Master B do colega
    )
    master_a.iniciar(simular=False)
    time.sleep(1)

    print("Master A no ar. Registrando workers A1, A2, A3...")
    workers = []
    for i in range(1, 4):
        w = Worker(f"A{i}", MEU_IP, 9000, meu_ip=MEU_IP, minha_porta=9200 + i)
        w.iniciar()
        workers.append(w)
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
