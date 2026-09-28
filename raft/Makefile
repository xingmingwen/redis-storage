CC=gcc
CFLAGS=-O2 -Wall -Wextra -std=c99 -pthread

all: raft_server

raft_server: raft.o raft_server.o
	$(CC) $(CFLAGS) -o $@ $^

raft.o: raft.c raft.h
	$(CC) $(CFLAGS) -c raft.c

raft_server.o: raft_server.c raft.h
	$(CC) $(CFLAGS) -c raft_server.c

clean:
	rm -f raft_server *.o

.PHONY: all clean
