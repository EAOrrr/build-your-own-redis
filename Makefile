all: server client

server:
	cd server && $(MAKE)

client:
	g++ -o redis-client ./client/client.cpp

clean:
	cd server && $(MAKE) clean
	rm -f redis-client

.PHONY: all server client clean
