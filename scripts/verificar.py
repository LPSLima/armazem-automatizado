#!/usr/bin/env python3
"""Verifica IPC real, entregas, movimentos, prioridade e encerramento em Linux."""
import csv
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "build" / "armazem"


class Execucao:
    def __init__(self, *argumentos):
        self.temporario = tempfile.TemporaryDirectory(prefix="armazem-teste-")
        self.saida = Path(self.temporario.name) / "saida.txt"
        self.arquivo = self.saida.open("w")
        self.processo = subprocess.Popen(
            [str(BIN), "--sem-painel", *map(str, argumentos)],
            cwd=ROOT, stdout=self.arquivo, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def texto(self):
        return self.saida.read_text(errors="replace")

    def esperar(self, condicao, segundos=15):
        fim = time.monotonic() + segundos
        while time.monotonic() < fim:
            valor = condicao()
            if valor:
                return valor
            if self.processo.poll() is not None:
                raise AssertionError("Execucao encerrou antes do esperado\n" + self.texto())
            time.sleep(0.02)
        raise AssertionError("Tempo excedido\n" + self.texto())

    def diretorio(self):
        def localizar():
            for linha in self.texto().splitlines():
                if linha.startswith("EXECUCAO="):
                    return ROOT / linha.split("=", 1)[1]
            return None
        return self.esperar(localizar)

    def eventos(self):
        arquivo = self.diretorio() / "eventos.csv"
        with arquivo.open(newline="") as entrada:
            registros = list(csv.DictReader(entrada, delimiter=";"))
        # A ultima linha pode estar em gravacao durante a simulacao.
        registros = [r for r in registros if r.get("bateria") is not None]
        return sorted(registros, key=lambda r: int(r["sequencia"]))

    def comando(self, texto):
        with (self.diretorio() / "comandos").open("w") as fifo:
            fifo.write(texto.rstrip("\n") + "\n")

    def terminar(self, codigo=0, segundos=40):
        retorno = self.processo.wait(timeout=segundos)
        assert retorno == codigo, self.texto()
        linhas = [
            linha[len("RESULTADO_JSON "):]
            for linha in self.texto().splitlines()
            if linha.startswith("RESULTADO_JSON ")
        ]
        assert linhas, self.texto()
        return json.loads(linhas[-1])

    def fechar(self):
        if self.processo.poll() is None:
            os.killpg(self.processo.pid, signal.SIGKILL)
            self.processo.wait()
        self.arquivo.close()
        self.temporario.cleanup()


def validar_final(execucao, resultado, pedidos, robos):
    assert resultado["recebidos"] == pedidos, resultado
    assert resultado["concluidos"] == pedidos, resultado
    assert resultado["pendentes"] == 0, resultado
    assert resultado["falhas"] == 0, resultado
    assert resultado["colisoes"] == 0, resultado
    assert resultado["eventos_perdidos"] == 0, resultado
    assert len(set(resultado["pids"])) == robos, resultado
    assert all(pid > 0 for pid in resultado["pids"]), resultado
    eventos = execucao.eventos()
    sequencias = [int(e["sequencia"]) for e in eventos]
    assert sequencias == list(range(1, len(sequencias) + 1)), sequencias
    concluidos = [
        int(e["pedido"]) for e in eventos
        if e["evento"] == "PEDIDO_CONCLUIDO"
    ]
    assert sorted(concluidos) == list(range(1, pedidos + 1)), concluidos
    posicoes = {i + 1: (0, 1 + 2 * i) for i in range(robos)}
    for evento in eventos:
        if evento["evento"] != "MOVIMENTO":
            continue
        robo = int(evento["robo"])
        destino = (int(evento["x"]), int(evento["y"]))
        anterior = posicoes[robo]
        assert abs(destino[0] - anterior[0]) + abs(destino[1] - anterior[1]) == 1
        assert all(p != destino for r, p in posicoes.items() if r != robo)
        assert 0 <= int(evento["bateria"]) <= 100
        posicoes[robo] = destino
    assert not (execucao.diretorio() / "comandos").exists()


def teste_automatico(robos, pedidos):
    e = Execucao("--robos", robos, "--pedidos", pedidos, "--passo-ms", 1)
    try:
        resultado = e.terminar()
        validar_final(e, resultado, pedidos, robos)
        print("OK automatico", robos, "robos", pedidos, "pedidos", resultado)
    finally:
        e.fechar()


def teste_fifo_prioridade():
    e = Execucao("--interativo", "--robos", 1, "--passo-ms", 10)
    try:
        e.comando("pedido 4 12 1")
        e.esperar(lambda: any(
            r["evento"] == "PEDIDO_INICIADO" and r["pedido"] == "1"
            for r in e.eventos()
        ))
        e.comando("pedido 1 4 1\npedido 2 8 1\npedido 3 12 9")
        e.esperar(lambda: sum(
            r["evento"] == "PEDIDO_CONCLUIDO" for r in e.eventos()
        ) == 4)
        ordem = [
            int(r["pedido"]) for r in e.eventos()
            if r["evento"] == "PEDIDO_INICIADO"
        ]
        assert ordem == [1, 4, 2, 3], ordem
        e.comando("parar")
        resultado = e.terminar()
        validar_final(e, resultado, 4, 1)
        print("OK FIFO e prioridade de mensagens", ordem)
    finally:
        e.fechar()


def teste_interrupcao():
    e = Execucao("--interativo", "--passo-ms", 5)
    try:
        e.comando("pedido 4 12 1")
        e.processo.send_signal(signal.SIGINT)
        resultado = e.terminar()
        assert resultado["interrompido"]
        assert resultado["falhas"] == 0, resultado
        assert not (e.diretorio() / "comandos").exists()
        print("OK SIGINT e limpeza")
    finally:
        e.fechar()


def teste_falha_filho():
    e = Execucao("--interativo", "--passo-ms", 5)
    try:
        prontos = e.esperar(lambda: [
            r for r in e.eventos() if r["evento"] == "ROBO_PRONTO"
        ] if len([r for r in e.eventos() if r["evento"] == "ROBO_PRONTO"]) == 4 else None)
        assert len(prontos) == 4
        pid = int(prontos[0]["pid"])
        os.kill(pid, signal.SIGKILL)
        resultado = e.terminar(codigo=1)
        assert resultado["falhas"] >= 1, resultado
        print("OK falha de filho encerra a simulacao")
    finally:
        e.fechar()


def teste_instancias():
    execucoes = [Execucao("--pedidos", 12, "--passo-ms", 1) for _ in range(2)]
    try:
        for e in execucoes:
            validar_final(e, e.terminar(), 12, 4)
        assert execucoes[0].diretorio() != execucoes[1].diretorio()
        print("OK duas instancias independentes")
    finally:
        for e in execucoes:
            e.fechar()


def main():
    assert BIN.exists(), "Execute make antes dos testes"
    teste_automatico(1, 12)
    teste_automatico(4, 48)
    teste_fifo_prioridade()
    teste_interrupcao()
    teste_falha_filho()
    teste_instancias()
    print("TODAS AS VERIFICACOES PASSARAM")


if __name__ == "__main__":
    main()
