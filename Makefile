CC ?= gcc
CPPFLAGS += -D_GNU_SOURCE -Isrc
CFLAGS ?= -std=c11 -O2 -g -Wall -Wextra -Wpedantic
LDLIBS += -pthread -lrt
BIN := build/armazem
SOURCES := src/armazem.c src/rotas.c

.PHONY: all run demo test clean
all: $(BIN)

$(BIN): $(SOURCES) src/rotas.h
	mkdir -p build
	$(CC) $(CPPFLAGS) $(CFLAGS) $(SOURCES) -o $@ $(LDLIBS)

run: $(BIN)
	./$(BIN)

demo: $(BIN)
	./$(BIN) --interativo --passo-ms 120

test: $(BIN)
	python3 scripts/verificar.py

clean:
	rm -f $(BIN)
