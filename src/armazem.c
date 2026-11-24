#include "rotas.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mqueue.h>
#include <poll.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_ROBOS 4
#define FILA_PEDIDOS 8
#define FILA_EVENTOS 256

enum Estado {
    OCIOSO, ESPERANDO_ROTA, ANDANDO, COLETANDO,
    ESPERANDO_CARGA, CARREGANDO, ENCERRADO
};

typedef struct {
    unsigned id;
    int linha;
    int coluna;
    unsigned prioridade;
    double criado_ms;
} Pedido;

typedef struct {
    pid_t pid;
    Posicao posicao;
    int bateria;
    int bateria_baixa;
    enum Estado estado;
    unsigned pedido;
    unsigned entregas;
    unsigned amostras;
} Robo;

typedef struct {
    unsigned sequencia;
    double tempo_ms;
    pid_t pid;
    int robo;
    unsigned pedido;
    int x, y, bateria;
    char tipo[32];
} Evento;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t rota_mudou;
    pthread_cond_t evento_pronto;
    pthread_cond_t espaco_evento;
    sem_t carregador;

    bool parar;
    bool interrompido;
    bool gerador_concluido;
    bool logger_concluido;
    unsigned prontos;
    unsigned recebidos;
    unsigned concluidos;
    unsigned proximo_pedido;
    unsigned esperas_rota;
    unsigned fila_cheia;
    unsigned recargas;
    unsigned carregando;
    unsigned rotas_ativas;
    unsigned max_rotas;
    unsigned falhas;
    unsigned colisoes;
    unsigned eventos_perdidos;
    unsigned proximo_ticket;
    unsigned ticket_atual;
    unsigned proximo_evento;
    double inicio_ms;
    double latencia_total_ms;

    Robo robos[MAX_ROBOS];
    int reserva[ALTURA][LARGURA];
    Evento eventos[FILA_EVENTOS];
    unsigned inicio_evento;
    unsigned quantidade_eventos;
} Compartilhado;

typedef struct {
    Compartilhado *memoria;
    mqd_t fila;
    int comandos_fd;
    int quantidade_robos;
    int quantidade_pedidos;
    int passo_ms;
    bool interativo;
    bool painel;
    char nome_fila[80];
    char diretorio[80];
    char caminho_fifo[PATH_MAX];
    FILE *log;
} Aplicacao;

typedef struct {
    Aplicacao *app;
    int numero;
    atomic_bool terminou;
} ContextoRobo;

typedef struct {
    Robo robos[MAX_ROBOS];
    int reserva[ALTURA][LARGURA];
    unsigned recebidos, concluidos, esperas_rota, fila_cheia;
    unsigned recargas, rotas_ativas, max_rotas, falhas;
    unsigned colisoes, eventos_perdidos;
    bool interrompido;
    double latencia_media_ms;
} Retrato;

static int numero_valido(const char *texto, int minimo, int maximo);

static double agora_ms(void)
{
    struct timespec instante;
    clock_gettime(CLOCK_MONOTONIC, &instante);
    return instante.tv_sec * 1000.0 + instante.tv_nsec / 1000000.0;
}

static void dormir_ms(int milissegundos)
{
    struct timespec intervalo = {
        .tv_sec = milissegundos / 1000,
        .tv_nsec = (milissegundos % 1000) * 1000000L
    };
    while (nanosleep(&intervalo, &intervalo) == -1 && errno == EINTR) {}
}

static struct timespec prazo_curto(void)
{
    struct timespec prazo;
    clock_gettime(CLOCK_REALTIME, &prazo);
    prazo.tv_nsec += 200000000L;
    if (prazo.tv_nsec >= 1000000000L) {
        prazo.tv_sec++;
        prazo.tv_nsec -= 1000000000L;
    }
    return prazo;
}

/* Recuperacao de dono morto encerra a simulacao. Nao retoma uma
   operacao parcialmente alterada pelo processo que morreu. */
static void verificar_mutex(Compartilhado *s, int erro)
{
    if (erro == 0)
        return;
    if (erro == EOWNERDEAD) {
        if (pthread_mutex_consistent(&s->mutex) != 0)
            _exit(71);
        s->falhas++;
        s->parar = true;
        pthread_cond_broadcast(&s->rota_mudou);
        pthread_cond_broadcast(&s->evento_pronto);
        pthread_cond_broadcast(&s->espaco_evento);
        return;
    }
    fprintf(stderr, "Falha de sincronizacao: %s\n", strerror(erro));
    _exit(71);
}

static void travar(Compartilhado *s)
{
    verificar_mutex(s, pthread_mutex_lock(&s->mutex));
}

static void aguardar(Compartilhado *s, pthread_cond_t *condicao)
{
    verificar_mutex(s, pthread_cond_wait(condicao, &s->mutex));
}

/* Chamado com o mutex adquirido. O buffer e limitado e aplica
   contrapressao aos produtores enquanto o logger o esvazia. */
static void registrar(Compartilhado *s, int robo, unsigned pedido,
                      const char *tipo)
{
    /* Captura antes de uma eventual espera. A sequencia preserva a ordem
       logica dos eventos mesmo se produtores aguardarem espaco. */
    Evento evento = {
        .sequencia = ++s->proximo_evento,
        .tempo_ms = agora_ms() - s->inicio_ms,
        .pid = getpid(),
        .robo = robo, .pedido = pedido,
        .x = -1, .y = -1, .bateria = -1
    };
    snprintf(evento.tipo, sizeof evento.tipo, "%s", tipo);
    if (robo >= 0) {
        Robo *r = &s->robos[robo];
        evento.x = r->posicao.x;
        evento.y = r->posicao.y;
        evento.bateria = r->bateria;
    }
    while (s->quantidade_eventos == FILA_EVENTOS && !s->parar)
        aguardar(s, &s->espaco_evento);
    if (s->quantidade_eventos == FILA_EVENTOS) {
        s->eventos_perdidos++;
        return;
    }
    unsigned indice =
        (s->inicio_evento + s->quantidade_eventos) % FILA_EVENTOS;
    s->eventos[indice] = evento;
    s->quantidade_eventos++;
    pthread_cond_signal(&s->evento_pronto);
}

static void parar_travado(Compartilhado *s, bool interrompido)
{
    if (!s->parar) {
        s->parar = true;
        s->interrompido = interrompido;
        registrar(s, -1, 0, interrompido ? "PARADA_SOLICITADA" : "FIM_NORMAL");
    }
    pthread_cond_broadcast(&s->rota_mudou);
    pthread_cond_broadcast(&s->evento_pronto);
    pthread_cond_broadcast(&s->espaco_evento);
}

static bool encerrando(Compartilhado *s)
{
    travar(s);
    bool resultado = s->parar;
    pthread_mutex_unlock(&s->mutex);
    return resultado;
}

static void falha(Compartilhado *s, int robo, const char *tipo)
{
    travar(s);
    s->falhas++;
    registrar(s, robo, robo >= 0 ? s->robos[robo].pedido : 0, tipo);
    parar_travado(s, true);
    pthread_mutex_unlock(&s->mutex);
}

static Compartilhado *criar_memoria(int robos)
{
    char nome[80];
    snprintf(nome, sizeof nome, "/armazem_shm_%ld_%lld",
             (long)getpid(), (long long)agora_ms());
    int fd = shm_open(nome, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0)
        return NULL;
    if (ftruncate(fd, sizeof(Compartilhado)) < 0) {
        int erro = errno;
        shm_unlink(nome);
        close(fd);
        errno = erro;
        return NULL;
    }
    Compartilhado *s = mmap(NULL, sizeof *s, PROT_READ | PROT_WRITE,
                           MAP_SHARED, fd, 0);
    int erro = errno;
    /* O mapeamento continua valido depois de desvincular o nome. */
    shm_unlink(nome);
    close(fd);
    if (s == MAP_FAILED) {
        errno = erro;
        return NULL;
    }
    memset(s, 0, sizeof *s);

    pthread_mutexattr_t atributo_mutex;
    pthread_condattr_t atributo_condicao;
    bool mutex_attr = false, cond_attr = false, mutex_ok = false;
    bool rota_ok = false, evento_ok = false, espaco_ok = false;

    erro = pthread_mutexattr_init(&atributo_mutex);
    if (erro) goto cancelar;
    mutex_attr = true;
    erro = pthread_mutexattr_setpshared(&atributo_mutex, PTHREAD_PROCESS_SHARED);
    if (erro) goto cancelar;
    erro = pthread_mutexattr_setrobust(&atributo_mutex, PTHREAD_MUTEX_ROBUST);
    if (erro) goto cancelar;
    erro = pthread_mutex_init(&s->mutex, &atributo_mutex);
    if (erro) goto cancelar;
    mutex_ok = true;
    erro = pthread_condattr_init(&atributo_condicao);
    if (erro) goto cancelar;
    cond_attr = true;
    erro = pthread_condattr_setpshared(&atributo_condicao, PTHREAD_PROCESS_SHARED);
    if (erro) goto cancelar;
    erro = pthread_cond_init(&s->rota_mudou, &atributo_condicao);
    if (erro) goto cancelar;
    rota_ok = true;
    erro = pthread_cond_init(&s->evento_pronto, &atributo_condicao);
    if (erro) goto cancelar;
    evento_ok = true;
    erro = pthread_cond_init(&s->espaco_evento, &atributo_condicao);
    if (erro) goto cancelar;
    espaco_ok = true;
    if (sem_init(&s->carregador, 1, 1) < 0) {
        erro = errno;
        goto cancelar;
    }
    pthread_mutexattr_destroy(&atributo_mutex);
    pthread_condattr_destroy(&atributo_condicao);

    s->inicio_ms = agora_ms();
    s->proximo_pedido = 1;
    for (int i = 0; i < robos; i++) {
        s->robos[i].posicao = (Posicao){0, 1 + 2 * i};
        s->robos[i].bateria = 30 + 5 * i;
        s->robos[i].estado = OCIOSO;
    }
    return s;

cancelar:
    if (espaco_ok) pthread_cond_destroy(&s->espaco_evento);
    if (evento_ok) pthread_cond_destroy(&s->evento_pronto);
    if (rota_ok) pthread_cond_destroy(&s->rota_mudou);
    if (cond_attr) pthread_condattr_destroy(&atributo_condicao);
    if (mutex_ok) pthread_mutex_destroy(&s->mutex);
    if (mutex_attr) pthread_mutexattr_destroy(&atributo_mutex);
    munmap(s, sizeof *s);
    errno = erro;
    return NULL;
}

static void destruir_memoria(Compartilhado *s)
{
    sem_destroy(&s->carregador);
    pthread_cond_destroy(&s->espaco_evento);
    pthread_cond_destroy(&s->evento_pronto);
    pthread_cond_destroy(&s->rota_mudou);
    pthread_mutex_destroy(&s->mutex);
    munmap(s, sizeof *s);
}

/* Todo o conjunto e reservado sob um unico mutex. Um robo que aguarda
   nao segura uma parte do caminho. Tickets evitam ultrapassagens na
   admissao das reservas, ao custo de bloqueio na cabeca da fila. */
static bool reservar(Aplicacao *app, int numero, const Caminho *rota)
{
    Compartilhado *s = app->memoria;
    travar(s);
    unsigned ticket = s->proximo_ticket++;
    s->robos[numero].estado = ESPERANDO_ROTA;
    bool espera_registrada = false;

    for (;;) {
        bool disponivel = ticket == s->ticket_atual;
        for (int i = 1; i < rota->tamanho && disponivel; i++) {
            Posicao p = rota->pontos[i];
            disponivel = s->reserva[p.y][p.x] == 0;
        }
        if (s->parar) {
            pthread_mutex_unlock(&s->mutex);
            return false;
        }
        if (disponivel)
            break;
        if (!espera_registrada) {
            s->esperas_rota++;
            registrar(s, numero, s->robos[numero].pedido, "AGUARDA_ROTA");
            espera_registrada = true;
            /* registrar pode esperar por espaco no buffer de eventos. */
            continue;
        }
        aguardar(s, &s->rota_mudou);
    }
    for (int i = 1; i < rota->tamanho; i++) {
        Posicao p = rota->pontos[i];
        s->reserva[p.y][p.x] = numero + 1;
    }
    s->ticket_atual++;
    s->rotas_ativas++;
    if (s->rotas_ativas > s->max_rotas)
        s->max_rotas = s->rotas_ativas;
    registrar(s, numero, s->robos[numero].pedido, "RESERVA_CONCEDIDA");
    pthread_cond_broadcast(&s->rota_mudou);
    pthread_mutex_unlock(&s->mutex);
    return true;
}

static void liberar(Aplicacao *app, int numero, const Caminho *rota)
{
    Compartilhado *s = app->memoria;
    travar(s);
    for (int i = 1; i < rota->tamanho; i++) {
        Posicao p = rota->pontos[i];
        if (s->reserva[p.y][p.x] == numero + 1)
            s->reserva[p.y][p.x] = 0;
    }
    s->rotas_ativas--;
    registrar(s, numero, s->robos[numero].pedido, "RESERVA_LIBERADA");
    pthread_cond_broadcast(&s->rota_mudou);
    pthread_mutex_unlock(&s->mutex);
}

static bool movimentar(Aplicacao *app, int numero, Posicao proxima)
{
    Compartilhado *s = app->memoria;
    dormir_ms(app->passo_ms);
    travar(s);
    if (s->parar) {
        pthread_mutex_unlock(&s->mutex);
        return false;
    }
    Robo *r = &s->robos[numero];
    bool doca = proxima.x == 0 && proxima.y == 1 + 2 * numero;
    if (!doca && s->reserva[proxima.y][proxima.x] != numero + 1) {
        s->falhas++;
        registrar(s, numero, r->pedido, "ERRO_SEM_RESERVA");
        parar_travado(s, true);
        pthread_mutex_unlock(&s->mutex);
        return false;
    }
    for (int i = 0; i < app->quantidade_robos; i++) {
        if (i != numero && s->robos[i].posicao.x == proxima.x &&
                           s->robos[i].posicao.y == proxima.y) {
            s->colisoes++;
            s->falhas++;
            registrar(s, numero, r->pedido, "ERRO_COLISAO");
            parar_travado(s, true);
            pthread_mutex_unlock(&s->mutex);
            return false;
        }
    }
    if (r->bateria <= 0) {
        s->falhas++;
        registrar(s, numero, r->pedido, "ERRO_BATERIA");
        parar_travado(s, true);
        pthread_mutex_unlock(&s->mutex);
        return false;
    }
    r->posicao = proxima;
    r->bateria--;
    r->estado = ANDANDO;
    registrar(s, numero, r->pedido, "MOVIMENTO");
    pthread_mutex_unlock(&s->mutex);
    return true;
}

static bool recarregar(Aplicacao *app, int numero, int necessario)
{
    Compartilhado *s = app->memoria;
    travar(s);
    if (s->robos[numero].bateria >= necessario) {
        pthread_mutex_unlock(&s->mutex);
        return true;
    }
    s->robos[numero].estado = ESPERANDO_CARGA;
    registrar(s, numero, s->robos[numero].pedido, "AGUARDA_CARREGADOR");
    pthread_mutex_unlock(&s->mutex);

    while (!encerrando(s)) {
        struct timespec prazo = prazo_curto();
        if (sem_timedwait(&s->carregador, &prazo) == 0) {
            travar(s);
            if (s->parar) {
                pthread_mutex_unlock(&s->mutex);
                sem_post(&s->carregador);
                return false;
            }
            s->carregando++;
            if (s->carregando != 1) {
                s->falhas++;
                parar_travado(s, true);
            }
            s->robos[numero].estado = CARREGANDO;
            registrar(s, numero, s->robos[numero].pedido, "INICIO_RECARGA");
            pthread_mutex_unlock(&s->mutex);

            bool terminou = false;
            while (!encerrando(s) && !terminou) {
                dormir_ms(app->passo_ms);
                travar(s);
                Robo *r = &s->robos[numero];
                r->bateria += 10;
                if (r->bateria >= 100) {
                    r->bateria = 100;
                    terminou = true;
                }
                pthread_mutex_unlock(&s->mutex);
            }
            travar(s);
            s->carregando--;
            if (terminou) {
                s->recargas++;
                registrar(s, numero, s->robos[numero].pedido, "FIM_RECARGA");
            }
            pthread_mutex_unlock(&s->mutex);
            if (sem_post(&s->carregador) < 0)
                falha(s, numero, "ERRO_SEM_POST");
            return terminou && !encerrando(s);
        }
        if (errno != ETIMEDOUT && errno != EINTR) {
            falha(s, numero, "ERRO_SEM_WAIT");
            return false;
        }
    }
    return false;
}

static void *telemetria(void *argumento)
{
    ContextoRobo *contexto = argumento;
    Compartilhado *s = contexto->app->memoria;
    while (!atomic_load(&contexto->terminou) && !encerrando(s)) {
        travar(s);
        Robo *r = &s->robos[contexto->numero];
        r->amostras++;
        r->bateria_baixa = r->bateria < 40;
        pthread_mutex_unlock(&s->mutex);
        dormir_ms(200);
    }
    return NULL;
}

static int executar_robo(Aplicacao *app, int numero)
{
    Compartilhado *s = app->memoria;
    /* Nova abertura evita compartilhar O_NONBLOCK do descritor do pai. */
    mqd_t entrada = mq_open(app->nome_fila, O_RDONLY);
    mq_close(app->fila);
    close(app->comandos_fd);
    fclose(app->log);
    if (entrada == (mqd_t)-1) {
        falha(s, numero, "ERRO_ABRIR_FILA");
        return 1;
    }

    ContextoRobo contexto = {.app = app, .numero = numero};
    atomic_init(&contexto.terminou, false);
    pthread_t sensor;
    int erro = pthread_create(&sensor, NULL, telemetria, &contexto);
    if (erro) {
        mq_close(entrada);
        falha(s, numero, "ERRO_THREAD_SENSOR");
        return 1;
    }
    travar(s);
    s->robos[numero].pid = getpid();
    s->prontos++;
    registrar(s, numero, 0, "ROBO_PRONTO");
    pthread_mutex_unlock(&s->mutex);

    while (!encerrando(s)) {
        Pedido pedido;
        struct timespec prazo = prazo_curto();
        ssize_t tamanho = mq_timedreceive(entrada, (char *)&pedido,
                                         sizeof pedido, NULL, &prazo);
        if (tamanho < 0) {
            if (errno == ETIMEDOUT || errno == EINTR)
                continue;
            falha(s, numero, "ERRO_RECEBER");
            break;
        }
        if (tamanho != (ssize_t)sizeof pedido) {
            falha(s, numero, "ERRO_TAMANHO_PEDIDO");
            break;
        }
        travar(s);
        s->robos[numero].pedido = pedido.id;
        registrar(s, numero, pedido.id, "PEDIDO_INICIADO");
        pthread_mutex_unlock(&s->mutex);

        Posicao origem = {0, 1 + 2 * numero};
        Posicao destino = {pedido.coluna, 2 * (pedido.linha - 1) + 1};
        Caminho rota;
        if (calcular_caminho(origem, destino, &rota) < 0) {
            falha(s, numero, "ERRO_CAMINHO");
            break;
        }
        /* Consumo simulado: 1 ponto por movimento e 2 pela coleta.
           Ha margem suficiente para voltar a doca antes de recarregar. */
        int necessario = 2 * (rota.tamanho - 1) + 12;
        if (necessario < 45)
            necessario = 45;
        if (!recarregar(app, numero, necessario) ||
            !reservar(app, numero, &rota))
            break;

        bool completo = true;
        for (int i = 1; i < rota.tamanho && completo; i++)
            completo = movimentar(app, numero, rota.pontos[i]);
        if (completo) {
            travar(s);
            s->robos[numero].estado = COLETANDO;
            s->robos[numero].bateria -= 2;
            registrar(s, numero, pedido.id, "COLETA");
            pthread_mutex_unlock(&s->mutex);
            dormir_ms(2 * app->passo_ms);
        }
        for (int i = rota.tamanho - 2; i >= 0 && completo; i--)
            completo = movimentar(app, numero, rota.pontos[i]);

        travar(s);
        if (completo && !s->parar) {
            s->concluidos++;
            s->robos[numero].entregas++;
            s->latencia_total_ms +=
                agora_ms() - s->inicio_ms - pedido.criado_ms;
            registrar(s, numero, pedido.id, "PEDIDO_CONCLUIDO");
        }
        s->robos[numero].estado = OCIOSO;
        pthread_mutex_unlock(&s->mutex);
        liberar(app, numero, &rota);
        travar(s);
        s->robos[numero].pedido = 0;
        pthread_mutex_unlock(&s->mutex);
    }
    atomic_store(&contexto.terminou, true);
    pthread_join(sensor, NULL);
    mq_close(entrada);
    travar(s);
    s->robos[numero].estado = ENCERRADO;
    registrar(s, numero, s->robos[numero].pedido, "ROBO_ENCERRADO");
    pthread_mutex_unlock(&s->mutex);
    return 0;
}

/* Retorna 0 ao aceitar, 1 se cheia e -1 para parada ou erro.
   mq_send nao bloqueia sob o mutex. O contador so muda no sucesso. */
static int enviar_pedido(Aplicacao *app, int linha, int coluna,
                         unsigned prioridade)
{
    Compartilhado *s = app->memoria;
    travar(s);
    if (s->parar) {
        pthread_mutex_unlock(&s->mutex);
        return -1;
    }
    Pedido pedido = {
        .id = s->proximo_pedido,
        .linha = linha, .coluna = coluna, .prioridade = prioridade,
        .criado_ms = agora_ms() - s->inicio_ms
    };
    if (mq_send(app->fila, (const char *)&pedido, sizeof pedido,
                prioridade) < 0) {
        int erro = errno;
        if (erro == EAGAIN || erro == EINTR) {
            s->fila_cheia++;
            pthread_mutex_unlock(&s->mutex);
            return 1;
        }
        s->falhas++;
        registrar(s, -1, pedido.id, "ERRO_ENVIAR");
        parar_travado(s, true);
        pthread_mutex_unlock(&s->mutex);
        return -1;
    }
    s->proximo_pedido++;
    s->recebidos++;
    registrar(s, -1, pedido.id, prioridade > 1 ? "ENVIO_URGENTE" : "ENVIO_NORMAL");
    pthread_mutex_unlock(&s->mutex);
    return 0;
}

static void *gerar_pedidos(void *argumento)
{
    Aplicacao *app = argumento;
    for (int i = 0; i < app->quantidade_pedidos; i++) {
        int linha = i % 4 + 1;
        int coluna = 4 + 4 * ((i / 4) % 3);
        unsigned prioridade = i % 5 == 4 ? 9 : 1;
        int resultado;
        do {
            resultado = enviar_pedido(app, linha, coluna, prioridade);
            if (resultado == 1)
                dormir_ms(20);
        } while (resultado == 1 && !encerrando(app->memoria));
        if (resultado < 0 || encerrando(app->memoria))
            break;
    }
    travar(app->memoria);
    app->memoria->gerador_concluido = true;
    pthread_mutex_unlock(&app->memoria->mutex);
    return NULL;
}

static void processar_comando(Aplicacao *app, char *linha)
{
    char *restante = NULL;
    char *comando = strtok_r(linha, " \t", &restante);
    if (!comando)
        return;
    char *argumentos[4];
    unsigned quantidade = 0;
    char *token;
    while (quantidade < 4 &&
           (token = strtok_r(NULL, " \t", &restante)) != NULL)
        argumentos[quantidade++] = token;

    if (strcmp(comando, "parar") == 0 && quantidade == 0) {
        travar(app->memoria);
        parar_travado(app->memoria, true);
        pthread_mutex_unlock(&app->memoria->mutex);
        return;
    }
    if (strcmp(comando, "pedido") == 0 && quantidade == 3) {
        int linha_pedido = numero_valido(argumentos[0], 1, 4);
        int coluna = numero_valido(argumentos[1], 4, 12);
        int prioridade = numero_valido(argumentos[2], 0, 9);
        if (linha_pedido > 0 && (coluna == 4 || coluna == 8 || coluna == 12) &&
            prioridade >= 0) {
            if (enviar_pedido(app, linha_pedido, coluna,
                              (unsigned)prioridade) == 1) {
                travar(app->memoria);
                registrar(app->memoria, -1, 0, "COMANDO_FILA_CHEIA");
                pthread_mutex_unlock(&app->memoria->mutex);
            }
            return;
        }
    }
    if (strcmp(comando, "bateria") == 0 && quantidade == 2) {
        int numero = numero_valido(argumentos[0], 1, app->quantidade_robos);
        int bateria = numero_valido(argumentos[1], 1, 100);
        if (numero > 0 && bateria > 0) {
            travar(app->memoria);
            Robo *robo = &app->memoria->robos[numero - 1];
            if (robo->estado == OCIOSO && robo->pedido == 0) {
                robo->bateria = bateria;
                registrar(app->memoria, numero - 1, 0, "BATERIA_AJUSTADA");
            } else {
                registrar(app->memoria, numero - 1,
                           robo->pedido, "BATERIA_REJEITADA");
            }
            pthread_mutex_unlock(&app->memoria->mutex);
            return;
        }
    }
    travar(app->memoria);
    registrar(app->memoria, -1, 0, "COMANDO_INVALIDO");
    pthread_mutex_unlock(&app->memoria->mutex);
}

static void *receber_comandos(void *argumento)
{
    Aplicacao *app = argumento;
    char linha[160];
    size_t usados = 0;
    bool descartando = false;
    struct pollfd descritor = {.fd = app->comandos_fd, .events = POLLIN};

    while (!encerrando(app->memoria)) {
        int pronto = poll(&descritor, 1, 100);
        if (pronto < 0 && errno == EINTR)
            continue;
        if (pronto < 0 || (descritor.revents & (POLLERR | POLLNVAL))) {
            falha(app->memoria, -1, "ERRO_FIFO");
            break;
        }
        if (pronto == 0 || !(descritor.revents & POLLIN))
            continue;
        char dados[256];
        ssize_t tamanho = read(app->comandos_fd, dados, sizeof dados);
        if (tamanho < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        if (tamanho <= 0) {
            falha(app->memoria, -1, "ERRO_LER_FIFO");
            break;
        }
        for (ssize_t i = 0; i < tamanho; i++) {
            if (dados[i] == '\n') {
                if (!descartando && usados) {
                    linha[usados] = '\0';
                    processar_comando(app, linha);
                }
                usados = 0;
                descartando = false;
            } else if (dados[i] != '\r' && !descartando) {
                if (usados + 1 < sizeof linha)
                    linha[usados++] = dados[i];
                else
                    descartando = true;
            }
        }
    }
    return NULL;
}

static void *gravar_eventos(void *argumento)
{
    Aplicacao *app = argumento;
    Compartilhado *s = app->memoria;
    for (;;) {
        travar(s);
        while (s->quantidade_eventos == 0 && !s->logger_concluido)
            aguardar(s, &s->evento_pronto);
        if (s->quantidade_eventos == 0 && s->logger_concluido) {
            pthread_mutex_unlock(&s->mutex);
            break;
        }
        Evento evento = s->eventos[s->inicio_evento];
        s->inicio_evento = (s->inicio_evento + 1) % FILA_EVENTOS;
        s->quantidade_eventos--;
        pthread_cond_signal(&s->espaco_evento);
        pthread_mutex_unlock(&s->mutex);

        if (fprintf(app->log, "%u;%.3f;%ld;%d;%u;%s;%d;%d;%d\n",
                    evento.sequencia, evento.tempo_ms, (long)evento.pid,
                    evento.robo + 1,
                    evento.pedido, evento.tipo, evento.x, evento.y,
                    evento.bateria) < 0 || fflush(app->log) != 0) {
            travar(s);
            s->falhas++;
            s->logger_concluido = true;
            parar_travado(s, true);
            pthread_mutex_unlock(&s->mutex);
            break;
        }
    }
    return NULL;
}

static Retrato fotografar(Compartilhado *s)
{
    Retrato r;
    travar(s);
    memcpy(r.robos, s->robos, sizeof r.robos);
    memcpy(r.reserva, s->reserva, sizeof r.reserva);
    r.recebidos = s->recebidos;
    r.concluidos = s->concluidos;
    r.esperas_rota = s->esperas_rota;
    r.fila_cheia = s->fila_cheia;
    r.recargas = s->recargas;
    r.rotas_ativas = s->rotas_ativas;
    r.max_rotas = s->max_rotas;
    r.falhas = s->falhas;
    r.colisoes = s->colisoes;
    r.eventos_perdidos = s->eventos_perdidos;
    r.interrompido = s->interrompido;
    r.latencia_media_ms =
        s->concluidos ? s->latencia_total_ms / s->concluidos : 0;
    pthread_mutex_unlock(&s->mutex);
    return r;
}

static void desenhar(Aplicacao *app, const Retrato *r)
{
    const char *nomes[] = {
        "OCIOSO", "AGUARDA ROTA", "MOVENDO", "COLETANDO",
        "AGUARDA CARGA", "CARREGANDO", "ENCERRADO"
    };
    const int cores[] = {0, 36, 33, 35, 32};
    printf("\033[H\033[2J");
    printf("ARMAZEM AUTOMATIZADO | %d robos | PID supervisor %ld\n",
           app->quantidade_robos, (long)getpid());
    printf("Pedidos %u/%u | rotas %u | max. simultaneas %u | recargas %u\n",
           r->concluidos, r->recebidos, r->rotas_ativas,
           r->max_rotas, r->recargas);
    printf("Esperas de rota %u | tentativas fila cheia %u | falhas %u\n\n",
           r->esperas_rota, r->fila_cheia, r->falhas);
    for (int y = 0; y < ALTURA; y++) {
        for (int x = 0; x < LARGURA; x++) {
            char simbolo = piso_livre(x, y) ? '.' : '#';
            if (x == 0 && y % 2 == 1 && y > 0 && y < ALTURA - 1)
                simbolo = 'D';
            if (y % 2 == 1 && y > 0 && y < ALTURA - 1 &&
                (x == 4 || x == 8 || x == 12))
                simbolo = (char)('A' + (y / 2) * 3 + x / 4 - 1);
            for (int i = 0; i < app->quantidade_robos; i++)
                if (r->robos[i].posicao.x == x && r->robos[i].posicao.y == y)
                    simbolo = (char)('1' + i);
            int dono = r->reserva[y][x];
            printf("\033[%dm %c \033[0m", cores[dono], simbolo);
        }
        putchar('\n');
    }
    puts("\n1-4 robos | D docas | A-L coleta | cores identificam reservas");
    puts("Robo   PID       Pedido  Bateria   Entregas  Estado");
    for (int i = 0; i < app->quantidade_robos; i++) {
        const Robo *robo = &r->robos[i];
        printf("R%-5d %-9ld %-7u %3d%%%s    %-8u %s\n", i + 1,
               (long)robo->pid, robo->pedido, robo->bateria,
               robo->bateria_baixa ? "!" : " ",
               robo->entregas, nomes[robo->estado]);
    }
    printf("\nFIFO: %s\n", app->caminho_fifo);
    puts("Comandos: pedido LINHA COLUNA PRIORIDADE | bateria ROBO VALOR | parar");
    puts("Linhas 1-4 | colunas 4,8,12 | prioridade 0-9 | Ctrl+C encerra");
    fflush(stdout);
}

static void escrever_resultado(FILE *saida, const Retrato *r, int robos)
{
    fprintf(saida,
        "{\"recebidos\":%u,\"concluidos\":%u,\"pendentes\":%u,"
        "\"colisoes\":%u,\"falhas\":%u,\"eventos_perdidos\":%u,"
        "\"esperas_rota\":%u,\"fila_cheia\":%u,\"recargas\":%u,"
        "\"max_rotas\":%u,\"latencia_media_ms\":%.3f,"
        "\"interrompido\":%s,\"pids\":[",
        r->recebidos, r->concluidos, r->recebidos - r->concluidos,
        r->colisoes, r->falhas, r->eventos_perdidos,
        r->esperas_rota, r->fila_cheia, r->recargas,
        r->max_rotas, r->latencia_media_ms,
        r->interrompido ? "true" : "false");
    for (int i = 0; i < robos; i++)
        fprintf(saida, "%s%ld", i ? "," : "", (long)r->robos[i].pid);
    fputs("]}\n", saida);
}

static int numero_valido(const char *texto, int minimo, int maximo)
{
    char *fim;
    errno = 0;
    long valor = strtol(texto, &fim, 10);
    if (errno || *texto == '\0' || *fim || valor < minimo || valor > maximo)
        return -1;
    return (int)valor;
}

int main(int argc, char **argv)
{
    Aplicacao app = {
        .fila = (mqd_t)-1, .comandos_fd = -1,
        .quantidade_robos = 4, .quantidade_pedidos = 16,
        .passo_ms = 100, .painel = isatty(STDOUT_FILENO)
    };
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--interativo") == 0) {
            app.interativo = true;
        } else if (strcmp(argv[i], "--sem-painel") == 0) {
            app.painel = false;
        } else if (strcmp(argv[i], "--help") == 0) {
            puts("Uso: ./build/armazem [--robos 1..4] [--pedidos 1..1000]");
            puts("     [--passo-ms 1..2000] [--interativo] [--sem-painel]");
            return 0;
        } else if (i + 1 < argc) {
            const char *opcao = argv[i];
            const char *valor = argv[++i];
            int numero = -1;
            if (strcmp(opcao, "--robos") == 0) {
                numero = numero_valido(valor, 1, MAX_ROBOS);
                app.quantidade_robos = numero;
            } else if (strcmp(opcao, "--pedidos") == 0) {
                numero = numero_valido(valor, 1, 1000);
                app.quantidade_pedidos = numero;
            } else if (strcmp(opcao, "--passo-ms") == 0) {
                numero = numero_valido(valor, 1, 2000);
                app.passo_ms = numero;
            }
            if (numero < 0) {
                fprintf(stderr, "Opcao ou valor invalido: %s %s\n", opcao, valor);
                return 2;
            }
        } else {
            fprintf(stderr, "Opcao incompleta ou desconhecida: %s\n", argv[i]);
            return 2;
        }
    }

    sigset_t sinais;
    sigemptyset(&sinais);
    sigaddset(&sinais, SIGINT);
    sigaddset(&sinais, SIGTERM);
    int erro = pthread_sigmask(SIG_BLOCK, &sinais, NULL);
    if (erro) {
        fprintf(stderr, "Erro nos sinais: %s\n", strerror(erro));
        return 1;
    }

    app.memoria = criar_memoria(app.quantidade_robos);
    if (!app.memoria) {
        perror("Memoria compartilhada");
        return 1;
    }
    int resultado = 1;
    bool fila_criada = false, fifo_criada = false;
    bool logger_iniciado = false, comandos_iniciados = false;
    bool gerador_iniciado = false;
    pthread_t logger, comandos, gerador;
    pid_t filhos[MAX_ROBOS] = {0};
    bool vivo[MAX_ROBOS] = {false};
    unsigned criados = 0;
    char caminho_log[PATH_MAX];

    if (mkdir("execucoes", 0700) < 0 && errno != EEXIST) {
        perror("Diretorio execucoes");
        goto limpeza;
    }
    snprintf(app.diretorio, sizeof app.diretorio, "execucoes/armazem-XXXXXX");
    if (!mkdtemp(app.diretorio)) {
        perror("Diretorio da execucao");
        goto limpeza;
    }
    snprintf(app.caminho_fifo, sizeof app.caminho_fifo,
             "%s/comandos", app.diretorio);
    snprintf(caminho_log, sizeof caminho_log, "%s/eventos.csv", app.diretorio);
    app.log = fopen(caminho_log, "w");
    if (!app.log) {
        perror("Log CSV");
        goto limpeza;
    }
    if (fputs("sequencia;tempo_ms;pid;robo;pedido;evento;x;y;bateria\n",
              app.log) < 0 || fflush(app.log) != 0) {
        perror("Cabecalho CSV");
        goto limpeza;
    }
    if (mkfifo(app.caminho_fifo, 0600) < 0) {
        perror("FIFO");
        goto limpeza;
    }
    fifo_criada = true;
    app.comandos_fd = open(app.caminho_fifo, O_RDWR | O_NONBLOCK | O_CLOEXEC);
    if (app.comandos_fd < 0) {
        perror("Abrir FIFO");
        goto limpeza;
    }
    snprintf(app.nome_fila, sizeof app.nome_fila, "/armazem_mq_%ld_%lld",
             (long)getpid(), (long long)agora_ms());
    struct mq_attr atributos = {
        .mq_maxmsg = FILA_PEDIDOS, .mq_msgsize = sizeof(Pedido)
    };
    app.fila = mq_open(app.nome_fila,
                      O_CREAT | O_EXCL | O_RDWR | O_NONBLOCK,
                      0600, &atributos);
    if (app.fila == (mqd_t)-1) {
        perror("Fila POSIX");
        goto limpeza;
    }
    fila_criada = true;
    printf("EXECUCAO=%s\nFIFO=%s\n", app.diretorio, app.caminho_fifo);
    fflush(NULL);

    /* Todos os forks ocorrem antes de criar threads no supervisor. */
    for (int i = 0; i < app.quantidade_robos; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            goto encerramento;
        }
        if (pid == 0) {
            int codigo = executar_robo(&app, i);
            munmap(app.memoria, sizeof *app.memoria);
            _exit(codigo);
        }
        filhos[i] = pid;
        vivo[i] = true;
        criados++;
        travar(app.memoria);
        app.memoria->robos[i].pid = pid;
        pthread_mutex_unlock(&app.memoria->mutex);
    }
    erro = pthread_create(&logger, NULL, gravar_eventos, &app);
    if (erro) {
        fprintf(stderr, "Thread logger: %s\n", strerror(erro));
        goto encerramento;
    }
    logger_iniciado = true;
    erro = pthread_create(&comandos, NULL, receber_comandos, &app);
    if (erro) {
        fprintf(stderr, "Thread comandos: %s\n", strerror(erro));
        goto encerramento;
    }
    comandos_iniciados = true;
    if (!app.interativo) {
        erro = pthread_create(&gerador, NULL, gerar_pedidos, &app);
        if (erro) {
            fprintf(stderr, "Thread gerador: %s\n", strerror(erro));
            goto encerramento;
        }
        gerador_iniciado = true;
    }
    if (app.painel)
        fputs("\033[?25l", stdout);

    while (!encerrando(app.memoria)) {
        struct timespec espera = {.tv_nsec = 100000000L};
        int sinal = sigtimedwait(&sinais, NULL, &espera);
        if (sinal == SIGINT || sinal == SIGTERM) {
            travar(app.memoria);
            parar_travado(app.memoria, true);
            pthread_mutex_unlock(&app.memoria->mutex);
        }
        for (unsigned i = 0; i < criados; i++) {
            if (!vivo[i])
                continue;
            int status;
            pid_t retorno = waitpid(filhos[i], &status, WNOHANG);
            if (retorno == filhos[i]) {
                vivo[i] = false;
                if (!encerrando(app.memoria))
                    falha(app.memoria, (int)i, "PROCESSO_ENCERROU_CEDO");
            } else if (retorno < 0 && errno != EINTR) {
                falha(app.memoria, (int)i, "ERRO_WAITPID");
                vivo[i] = false;
            }
        }
        travar(app.memoria);
        bool fila_aberta = app.memoria->prontos == criados;
        bool terminou = !app.interativo && app.memoria->gerador_concluido &&
                        app.memoria->recebidos == app.memoria->concluidos;
        if (terminou)
            parar_travado(app.memoria, false);
        pthread_mutex_unlock(&app.memoria->mutex);
        /* Todos os filhos ja abriram a fila. O nome deixa de existir,
           mas seus descritores permanecem utilizaveis ate mq_close. */
        if (fila_criada && fila_aberta && mq_unlink(app.nome_fila) == 0)
            fila_criada = false;
        if (app.painel) {
            Retrato retrato = fotografar(app.memoria);
            desenhar(&app, &retrato);
        }
    }
    resultado = 0;

encerramento:
    travar(app.memoria);
    parar_travado(app.memoria, resultado != 0);
    pthread_mutex_unlock(&app.memoria->mutex);
    if (gerador_iniciado) pthread_join(gerador, NULL);
    if (comandos_iniciados) pthread_join(comandos, NULL);
    for (unsigned i = 0; i < criados; i++) {
        if (!vivo[i]) continue;
        int status;
        pid_t retorno;
        do {
            retorno = waitpid(filhos[i], &status, 0);
        } while (retorno < 0 && errno == EINTR);
        if (retorno < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
            falha(app.memoria, (int)i, "ERRO_ENCERRAR_PROCESSO");
    }
    travar(app.memoria);
    app.memoria->logger_concluido = true;
    pthread_cond_broadcast(&app.memoria->evento_pronto);
    pthread_mutex_unlock(&app.memoria->mutex);
    if (logger_iniciado) pthread_join(logger, NULL);

    {
        Retrato retrato = fotografar(app.memoria);
        if (retrato.falhas)
            resultado = 1;
        if (app.painel)
            fputs("\033[?25h\033[0m\n", stdout);
        fputs("RESULTADO_JSON ", stdout);
        escrever_resultado(stdout, &retrato, app.quantidade_robos);
        char resumo[PATH_MAX];
        snprintf(resumo, sizeof resumo, "%s/resumo.json", app.diretorio);
        FILE *arquivo = fopen(resumo, "w");
        if (arquivo) {
            escrever_resultado(arquivo, &retrato, app.quantidade_robos);
            if (fclose(arquivo) != 0) resultado = 1;
        } else {
            perror("Resumo JSON");
            resultado = 1;
        }
        printf("Log: %s\n", caminho_log);
    }

limpeza:
    if (app.fila != (mqd_t)-1) mq_close(app.fila);
    if (fila_criada) mq_unlink(app.nome_fila);
    if (app.comandos_fd >= 0) close(app.comandos_fd);
    if (fifo_criada) unlink(app.caminho_fifo);
    if (app.log && fclose(app.log) != 0) resultado = 1;
    destruir_memoria(app.memoria);
    return resultado;
}
