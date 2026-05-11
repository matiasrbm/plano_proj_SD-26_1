# p2p-michael

Projeto P2P em Python com Master/Worker, identificacao UDP e migracao para TCP via selecao deterministica.

Execute o Worker sem host/porta para utilizar a identificacao UDP:
python worker.py

Caso queira validar o modo manual, execute o Worker com host e porta:
python worker.py 127.0.0.1 5000

Novo funcionamento
O Worker envia um probe DISCOVERY utilizando UDP.
O Master retorna DISCOVERY_REPLY, contendo MASTER_NAME, MASTER_IP e MASTER_PORT.
O Worker define o menor MASTER_NAME em ordem lexicografica.
O Worker estabelece conexao TCP com o Master selecionado e transmite ELECTION_ACK.
Apos o ACK, o processo atual de identificacao, heartbeat e distribuicao de tarefas segue normalmente.


Principais arquivos
config.py: parametros compartilhados de identificacao e selecao.
master.py: responde a identificacao UDP e valida a selecao via TCP.
worker.py: executa a identificacao, determina o vencedor e inicializa o heartbeat.
