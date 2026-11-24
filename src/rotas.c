#include "rotas.h"

int piso_livre(int x, int y)
{
    if (x < 0 || x >= LARGURA || y < 1 || y >= ALTURA - 1)
        return 0;
    if (x == 0)
        return y % 2 == 1;
    return x <= 13 && (y % 2 == 1 || x == 1 || x == 13);
}

/* BFS encontra o menor caminho em numero de movimentos no mapa estatico. */
int calcular_caminho(Posicao origem, Posicao destino, Caminho *caminho)
{
    int anterior[MAX_CAMINHO];
    int fila[MAX_CAMINHO];
    int inicio = 0;
    int fim = 0;
    const int dx[] = {1, 0, -1, 0};
    const int dy[] = {0, 1, 0, -1};
    int fonte = origem.y * LARGURA + origem.x;
    int alvo = destino.y * LARGURA + destino.x;
    caminho->tamanho = 0;

    if (!piso_livre(origem.x, origem.y) ||
        !piso_livre(destino.x, destino.y))
        return -1;

    for (int i = 0; i < MAX_CAMINHO; i++)
        anterior[i] = -1;
    anterior[fonte] = fonte;
    fila[fim++] = fonte;

    while (inicio < fim && anterior[alvo] == -1) {
        int atual = fila[inicio++];
        int x = atual % LARGURA;
        int y = atual / LARGURA;
        for (int lado = 0; lado < 4; lado++) {
            int nx = x + dx[lado];
            int ny = y + dy[lado];
            if (!piso_livre(nx, ny))
                continue;
            int proximo = ny * LARGURA + nx;
            if (anterior[proximo] != -1)
                continue;
            anterior[proximo] = atual;
            fila[fim++] = proximo;
        }
    }
    if (anterior[alvo] == -1)
        return -1;

    Posicao inverso[MAX_CAMINHO];
    int quantidade = 0;
    for (int atual = alvo; ; atual = anterior[atual]) {
        inverso[quantidade++] =
            (Posicao){atual % LARGURA, atual / LARGURA};
        if (atual == fonte)
            break;
    }
    caminho->tamanho = quantidade;
    for (int i = 0; i < quantidade; i++)
        caminho->pontos[i] = inverso[quantidade - i - 1];
    return 0;
}
