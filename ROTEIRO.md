# Roteiro da apresentação

Duração planejada de 6 minutos e 25 segundos, incluindo a demonstração.
Acrescente os nomes dos integrantes na abertura oral.

## Slide 1 — Objetivo — 40 segundos

O projeto simula um armazém no qual vários robôs recebem pedidos e
precisam buscar produtos ao mesmo tempo. O problema principal é
coordenar o compartilhamento dos corredores e do carregador.

A aplicação mostra os conceitos de programação concorrente e IPC em
C no Linux. As decisões de deslocamento e a bateria usam um modelo
discreto para que o comportamento possa ser acompanhado no terminal.

## Slide 2 — Processos e threads — 60 segundos

O supervisor cria um processo para cada robô usando fork. Cada filho
executa os pedidos e tem uma thread adicional de telemetria.

No supervisor, há threads para gerar pedidos, receber comandos por
FIFO e gravar os eventos. O fluxo principal desenha o painel e
acompanha os filhos.

Todos os forks ocorrem antes da criação das threads do supervisor.
Mostre no painel que os robôs têm PIDs diferentes.

## Slide 3 — IPC e sincronização — 70 segundos

Os pedidos são mensagens em uma fila POSIX limitada. A prioridade
define qual mensagem pendente será retirada primeiro.

O mapa, as posições e as métricas ficam em memória compartilhada.
Esses dados são protegidos por um mutex configurado para uso entre
processos.

O carregador é protegido por um semáforo de capacidade um.
As condições permitem esperar por rotas e por eventos sem ocupar
um núcleo em espera ativa.

## Slide 4 — Reserva e prevenção de deadlock — 65 segundos

Cada robô calcula uma rota usando BFS. Antes de se movimentar,
requisita todos os pontos da rota em uma operação protegida.

Se algum ponto já estiver reservado, o robô aguarda sem segurar
uma parte da rota. Isso elimina a situação em que dois robôs retêm
metade do caminho e ficam esperando um pelo outro.

Tickets organizam a admissão. Rotas independentes podem estar
reservadas ao mesmo tempo. Essa decisão prioriza correção e
facilidade de explicar o comportamento. Ela pode reduzir a
utilização quando trajetos se sobrepõem.

## Slide 5 — Demonstração — 110 segundos

Antes da apresentação, deixe dois terminais abertos na pasta do projeto.
No primeiro, execute make demo. Copie o caminho da FIFO exibido.

No segundo, envie estes comandos, substituindo ABC123 pelo nome real.

~~~bash
printf 'bateria 1 10\n' > execucoes/armazem-ABC123/comandos
printf 'pedido 4 12 1\npedido 4 8 1\npedido 4 4 1\npedido 3 12 9\n' \
  > execucoes/armazem-ABC123/comandos
~~~

Aponte os PIDs distintos, a aquisição do carregador, os estados
AGUARDA ROTA e a mudança de posição.

Para observar prioridade de forma controlada, use em um ensaio
separado apenas um robô.

~~~bash
./build/armazem --interativo --robos 1 --passo-ms 100
~~~

Envie um pedido longo e aguarde o início dele. Depois envie dois
pedidos normais e um urgente. O urgente será atendido depois do
pedido em execução e antes dos normais ainda na fila.

Ao terminar a demonstração principal, pressione Ctrl+C. Mostre
o resumo e a pasta com o CSV. Não conte pedidos interrompidos
como entregues.

## Slide 6 — Verificação e escolhas do projeto — 40 segundos

Mostre a saída de make test obtida no ensaio e os indicadores
da execução apresentada.

Explique que o projeto verifica entregas, duplicatas, posições,
bateria, prioridade, interrupção e instâncias simultâneas.
O caso de falha de filho deve terminar com erro e encerrar
a simulação.

As extensões possíveis incluem replanejamento de rotas e
recuperação de robôs. A versão apresentada usa reserva integral
e parada coordenada diante de falha.

## Perguntas prováveis

Por que processos e threads juntos?

Os robôs são processos independentes. As threads dividem atividades
dentro de cada processo, como executar um pedido e amostrar telemetria.

Por que usar mutex se já existe memória compartilhada?

O mapeamento permite acessar os mesmos dados. A exclusão mútua
coordena as operações que leem e alteram esses dados.

A prioridade do pedido altera a prioridade Linux do robô?

A prioridade é um atributo da mensagem e ordena a retirada dos
pedidos ainda na fila. Os processos usam o escalonamento normal
do Linux nesta implementação.

Por que os sinais são tratados no supervisor?

O supervisor controla o ciclo de vida dos filhos. Os sinais são
bloqueados antes do fork e tratados por sigtimedwait no supervisor,
que transforma a solicitação em uma flag compartilhada e acorda
as tarefas em espera.

Por que o robô não reserva apenas a próxima célula?

Uma reserva parcial exigiria um protocolo adicional para resolver
espera circular. A reserva integral permite demonstrar uma solução
com uma justificativa direta e observar o custo na concorrência.

Qual o custo da garantia?

Um robô pode reter pontos que usará apenas na volta. Tickets também
podem bloquear pedidos com caminhos disponíveis atrás de outro
que está esperando. Os indicadores ajudam a observar esse custo.
