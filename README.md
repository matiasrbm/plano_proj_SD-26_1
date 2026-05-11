# p2p-michael

Projeto P2P em Python com Master/Worker, descoberta UDP e transicao para TCP via eleicao deterministica.

## Como executar

1. Inicie o Master:

```bash
python master.py
```

2. Inicie o Worker sem host/porta para usar a descoberta UDP:

```bash
python worker.py
```

3. Se quiser testar o caminho manual, inicie o Worker com host e porta:

```bash
python worker.py 127.0.0.1 5000
```

## Fluxo novo

- O Worker envia um probe `DISCOVERY` por UDP.
- O Master responde com `DISCOVERY_REPLY`, incluindo `MASTER_NAME`, `MASTER_IP` e `MASTER_PORT`.
- O Worker escolhe o menor `MASTER_NAME` em ordem lexicografica.
- O Worker conecta por TCP ao Master vencedor e envia `ELECTION_ACK`.
- Depois do `ACK`, o fluxo atual de apresentacao, heartbeat e tarefas continua como antes.

## Arquivos principais

- `config.py`: constantes compartilhadas de descoberta e eleicao.
- `master.py`: responde a descoberta UDP e confirma a eleicao via TCP.
- `worker.py`: faz descoberta, escolhe o vencedor e inicia o heartbeat.

## Observacao

Os logs foram deixados mais detalhados nas etapas novas para facilitar a conferência do fluxo e a validacao manual.