p2p-michael

Como Executar
1. Iniciar o Master
  -> python master.py

2. Iniciar o Worker utilizando descoberta UDP
   -> python worker.py

3. Iniciar o Worker manualmente com host e porta
   -> python worker.py 127.0.0.1 5000


- > Fluxo de Funcionamento
O Worker envia um probe DISCOVERY via UDP.
O Master responde com DISCOVERY_REPLY.
A resposta inclui:
MASTER_NAME
MASTER_IP
MASTER_PORT
O Worker seleciona o menor MASTER_NAME em ordem lexicográfica.
O Worker estabelece conexão TCP com o Master escolhido.
O Worker envia ELECTION_ACK.
Após o ACK, o sistema continua com:
apresentação do Worker
heartbeat
processamento de tarefas





Estrutura Principal
config.py

Contém constantes e parâmetros compartilhados utilizados na descoberta e na eleição.

master.py
Responsável por:
responder requisições UDP
confirmar a eleição
iniciar a comunicação TCP


worker.py
Responsável por:
realizar descoberta UDP
selecionar o Master vencedor
iniciar heartbeat e comunicação TCP


           Exemplo de Fluxo
Worker → DISCOVERY
Master → DISCOVERY_REPLY
Worker → Seleção do menor MASTER_NAME
Worker → Conexão TCP
Worker → ELECTION_ACK
Heartbeat → Execução contínua
