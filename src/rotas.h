#ifndef ROTAS_H
#define ROTAS_H

#define LARGURA 15
#define ALTURA 9
#define MAX_CAMINHO (LARGURA * ALTURA)

typedef struct {
    int x;
    int y;
} Posicao;

typedef struct {
    Posicao pontos[MAX_CAMINHO];
    int tamanho;
} Caminho;

int piso_livre(int x, int y);
int calcular_caminho(Posicao origem, Posicao destino, Caminho *caminho);

#endif
