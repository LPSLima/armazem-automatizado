# Armazém automatizado com robôs

Projeto da Avaliação Parcial 2 de Programação Concorrente e IPC,
Sistemas Eletrônicos de Tempo-Real, UEA.
Professor Fabio de Sousa Cardoso.

A aplicação simula até quatro robôs atendendo pedidos em um armazém.
Cada robô é um processo Linux. Cada processo tem uma thread de
telemetria além do fluxo principal que recebe pedidos e movimenta o robô.
O supervisor cria os processos antes de iniciar suas próprias threads.

Os pedidos contêm localização de coleta e prioridade. O robô recebe
um pedido, verifica a bateria, reserva o trajeto, coleta o produto e
retorna à doca. Um único carregador é compartilhado por todos.

## Ambiente

Ubuntu no WSL2 ou Linux com suporte a filas de mensagens POSIX.

Instale as ferramentas no terminal Ubuntu.

~~~bash
sudo apt update
sudo apt install -y build-essential make git python3
~~~

O código usa C11, pthreads e librt. Não precisa de placa, sensores,
bibliotecas gráficas ou privilégios de root durante a execução.

## Compilação e demonstração automática

~~~bash
make
./build/armazem
~~~

A execução padrão usa quatro robôs, 16 pedidos e movimentos de 100 ms.
Em um terminal, o painel é atualizado até que todos os pedidos aceitos
sejam entregues. Em redirecionamentos de saída, são emitidos os caminhos
da execução e um resumo final JSON.

~~~bash
./build/armazem --robos 4 --pedidos 48 --passo-ms 30
./build/armazem --robos 1 --pedidos 12 --sem-painel --passo-ms 5
~~~

As latências observadas incluem fila, espera por recarga, espera pela
reserva, deslocamento e coleta. O intervalo de movimento é um atraso
de simulação e depende do escalonamento do Linux.

## Interação em outro terminal

~~~bash
make demo
~~~

O programa mostra o caminho da FIFO, como
execucoes/armazem-ABC123/comandos. Em outro terminal Ubuntu, entre
na mesma pasta do projeto e substitua ABC123 pelo nome exibido.

~~~bash
printf 'pedido 2 8 1\n' > execucoes/armazem-ABC123/comandos
printf 'pedido 4 12 9\n' > execucoes/armazem-ABC123/comandos
printf 'bateria 1 10\n' > execucoes/armazem-ABC123/comandos
printf 'parar\n' > execucoes/armazem-ABC123/comandos
~~~

- pedido LINHA COLUNA PRIORIDADE aceita linhas 1 a 4, colunas 4, 8 ou 12
  e prioridades de 0 a 9.
- bateria ROBO VALOR aceita robôs 1 a 4, conforme a quantidade criada,
  e bateria entre 1 e 100. O ajuste só é aceito para um robô ocioso.
- parar e Ctrl+C solicitam o encerramento coordenado.

Pedidos de prioridade maior são retirados primeiro entre os que
continuam na fila. Em prioridades iguais, a fila POSIX preserva FIFO.
O pedido que já está sendo executado segue até a conclusão ou parada.

A fila comporta oito mensagens. Se estiver cheia, o gerador automático
aguarda e tenta novamente. Um pedido manual recusado gera o evento
COMANDO_FILA_CHEIA e não entra na contagem de pedidos aceitos.

## Mapa e reserva de trajeto

Há quatro docas, quatro corredores horizontais e dois corredores
verticais. A até L representam os pontos de coleta. Os dígitos 1 a 4
identificam os robôs. As cores indicam o proprietário de cada reserva.

O planejamento usa busca em largura, BFS, no mapa estático. O percurso
de volta é o inverso do percurso de ida. A doca é exclusiva do robô
e fica em uma extremidade sem passagem para outras rotas.

O monitor verifica o trajeto inteiro sob o mutex compartilhado.
Quando todos os pontos estão disponíveis, reserva o conjunto.
Caso contrário, o robô aguarda uma variável de condição sem manter
reservas parciais. Ao retornar à doca, libera o conjunto e acorda
os interessados.

Tickets sequenciais organizam a admissão das reservas. Um pedido de
reserva novo aguarda a vez dos anteriores. Depois de concedida uma
reserva, outra reserva independente pode ser concedida e os robôs
prosseguem concorrentemente.

A estratégia evita espera circular pelos pontos do trajeto porque
um robô aguardando uma reserva não retém parte dos pontos requisitados.
Em uma execução normal, cada rota reservada termina em tempo finito.
A reserva integral pode reduzir o aproveitamento dos corredores.
O robô espera a rota BFS original em vez de recalcular um desvio.

A prioridade de mensagens e a ordem de admissão de rotas são mecanismos
distintos. Prioridade estrita de mensagens pode adiar pedidos normais
se pedidos urgentes chegarem continuamente.

## Mecanismos utilizados

| Mecanismo | Aplicação concreta |
|---|---|
| fork e waitpid | Criação e coleta dos processos dos robôs |
| _exit | Encerramento dos filhos após juntar a thread |
| pthread_create e pthread_join | Telemetria em cada robô e serviços no supervisor |
| mq_open, mq_send, mq_timedreceive | Fila limitada de pedidos com prioridade |
| shm_open, ftruncate, mmap | Estado do mapa e dos robôs compartilhado entre processos |
| pthread_mutex com PTHREAD_PROCESS_SHARED | Proteção de posições, reservas, métricas e buffer |
| pthread_cond com PTHREAD_PROCESS_SHARED | Espera por rotas e produtor–consumidor de eventos |
| sem_init com pshared igual a 1 | Um carregador compartilhado |
| sem_timedwait e sem_post | Aquisição e liberação do carregador com verificação periódica de parada |
| mkfifo, poll e read | Comandos de um segundo terminal |
| pthread_sigmask e sigtimedwait | Tratamento coordenado de SIGINT e SIGTERM |
| Mutex robusto | Detecta morte do dono e inicia parada por falha |

A memória que contém mutexes, condições e semáforo é compartilhada.
Uma flag de processo compartilhado em um objeto localizado apenas na
pilha privada não atenderia a essa arquitetura.

A posição e a bateria são atualizadas pelo fluxo de execução do robô.
A thread de telemetria amostra periodicamente esses valores, publica
um sinal de bateria baixa e mantém a contagem de amostras.
A recarga é realizada na doca, com um conector compartilhado.

O supervisor possui um gerador de pedidos no modo automático,
uma thread leitora da FIFO e uma thread gravadora do CSV.
O fluxo principal recebe sinais, acompanha os filhos e desenha o painel.

## Logs e encerramento

Cada execução cria uma pasta exclusiva dentro de execucoes.
Ela contém eventos.csv e resumo.json. A FIFO comandos existe
somente enquanto a execução está ativa.

O CSV inclui sequência lógica, tempo monotônico relativo, PID, robô,
pedido, evento, posição e bateria. Ordene por sequencia para
reconstruir a ordem lógica. Se o buffer de eventos ficar cheio,
a espera dos produtores pode alterar a ordem de gravação física.
O arquivo é gravado por uma única thread.

A memória compartilhada tem seu nome removido logo após o mapeamento.
A fila perde o nome depois que todos os filhos a abriram. Os
mapeamentos e descritores continuam válidos durante a execução.
Essa estratégia permite instâncias simultâneas e reduz objetos
nomeados que poderiam ficar pendentes.

No encerramento, o supervisor informa a parada, acorda os monitores,
junta threads, recolhe os filhos e remove a FIFO. Os robôs verificam
a parada durante as esperas temporizadas. O logger drena os eventos
antes de encerrar. Pedidos que estavam pendentes permanecem
identificados no resumo, sem serem contados como entregues.

A morte inesperada de um filho encerra a simulação com código de erro.
A recuperação do mutex robusto sinaliza falha e interrompe o sistema.
Um processo morto não é substituído automaticamente.

## Verificações reproduzíveis

~~~bash
make test
~~~

O verificador usa processos Linux e IPC reais.

1. Um robô atende 12 pedidos.
2. Quatro robôs atendem 48 pedidos.
3. A FIFO recebe pedidos e uma mensagem urgente ultrapassa pedidos
   normais ainda pendentes.
4. SIGINT encerra a aplicação e remove a FIFO.
5. A morte forçada de um filho é detectada e encerra os demais.
6. Duas instâncias executam ao mesmo tempo sem compartilhar seus dados.

São verificados contagem de entregas, ausência de duplicatas, PIDs,
trajetória por células adjacentes, posições distintas, bateria e
registro completo dos eventos. Os testes têm timeout.

O número de rotas simultâneas observado depende das tarefas recebidas,
dos destinos e da ordem de execução dos processos. O mapa e o painel
permitem observar quais trajetos podem coexistir.

Se a criação da fila retornar erro, confira o suporte a POSIX mqueue
do seu Linux/WSL2 e os limites em /proc/sys/fs/mqueue.
Se o compilador falhar, copie a mensagem completa para diagnóstico.

## Estrutura

- src/armazem.c contém processos, threads, IPC, monitor e painel.
- src/rotas.c e src/rotas.h contêm o mapa e o planejador BFS.
- scripts/verificar.py executa os cenários de validação.
- apresentacao.html contém seis slides navegáveis.
- ROTEIRO.md contém a fala e a demonstração para 5 a 8 minutos.
- Makefile compila, executa e inicia as verificações.

## Apresentação

Abra apresentacao.html no navegador e use as setas para navegar.
F11 coloca o navegador em tela cheia. Ctrl+P imprime os seis slides.

No WSL, este comando abre a apresentação no Windows.

~~~bash
explorer.exe "$(wslpath -w "$PWD/apresentacao.html")"
~~~

Leia ROTEIRO.md e execute os cenários antes da apresentação.
Os indicadores mostrados durante a demonstração devem ser os medidos
na própria execução.

## Repositório Git e GitHub

Na pasta do projeto, execute os comandos abaixo.

~~~bash
git init
git add .
git commit -m "Implementa armazem concorrente com robos e IPC POSIX"
git branch -M main
~~~

Crie um repositório vazio chamado armazem-robos no GitHub.
Substitua SEU_USUARIO no comando seguinte.

~~~bash
git remote add origin https://github.com/SEU_USUARIO/armazem-robos.git
git push -u origin main
~~~

Inclua os nomes dos integrantes neste README antes de enviar o link
ao professor. O grupo pode ter até três alunos.

## Referências técnicas

- Linux man-pages sobre filas POSIX
  https://man7.org/linux/man-pages/man7/mq_overview.7.html
- Linux man-pages sobre sem_init
  https://man7.org/linux/man-pages/man3/sem_init.3.html
- POSIX sobre condições compartilhadas
  https://man7.org/linux/man-pages/man3/pthread_condattr_getpshared.3p.html
- Linux man-pages sobre fork
  https://man7.org/linux/man-pages/man2/fork.2.html
- Linux man-pages sobre mutex robusto
  https://man7.org/linux/man-pages/man3/pthread_mutexattr_setrobust.3.html

## Validação de referência

Em 23/11/2026, a compilação com avisos habilitados e os seis cenários
de make test passaram em um ambiente Linux. O cenário de quatro robôs
concluiu os 48 pedidos, com zero colisões, zero falhas e zero eventos
perdidos. A instalação no WSL deve ser conferida no computador de destino.
