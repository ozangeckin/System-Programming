CFLAGS = -g -O2 -Wall
LIBS = -lpthread

all: client server

client: client.c
	$(CC) $(CFLAGS) client.c -o client $(LIBS)

server: server.c
	$(CC) $(CFLAGS) server.c -o server $(LIBS)

.PHONY: clean
clean:
	$(RM) server client
