all: server mylib.so

mylib.o: mylib.c
	gcc -Wall -fPIC -DPIC -c mylib.c

mylib.so: mylib.o
	ld -shared -o mylib.so mylib.o -ldl

LDFLAGS = -L../lib

server: server.c
	gcc $(LDFLAGS) -Wall -fPIC -DPIC -I../include -o server server.c -ldirtree

clean:
	rm -f *.o *.so $(PROGS)

