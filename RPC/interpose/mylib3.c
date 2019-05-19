#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <err.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include "../include/dirtree.h"
#define MAXMSGLEN 100000

int (*orig_open)(const char *pathname, int flags, ...);
int (*orig_close)(int fd);
ssize_t (*orig_read)(int fd, void *buf, size_t nbytes);
ssize_t (*orig_write)(int fd, const void *buf, size_t nbytes);
off_t (*orig_lseek)(int fd, off_t offset, int whence);
int (*orig_stat)(int ver, const char *path, struct stat *buf);
int (*orig_unlink)(const char *path);
ssize_t (*orig_getdirentries)(int fd, char *buf, size_t nbytes, off_t *basep);
struct dirtreenode *(*orig_getdirtree)(char *path);
void (*orig_freedirtree)(struct dirtreenode *root);

struct dirtreenode **dir_list;
int num_directories;

int open(const char *pathname, int flags, ...) {
    mode_t m=0;
    if (flags & O_CREAT) {
        va_list a;
        va_start(a, flags);
        m = va_arg(a, mode_t);
        va_end(a);
    }
	char *serverip;
	char *serverport;
    uint8_t buf[MAXMSGLEN];
	unsigned short port;
	int sockfd, rv;
	struct sockaddr_in srv;

	// Get environment variable indicating the ip address of the server
	serverip = getenv("server15440");
	if (!serverip) serverip = "127.0.0.1";

	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
	else {
		fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
		serverport = "15440";
	}
	port = (unsigned short)atoi(serverport);

	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error

	// setup address structure to point to server
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
	srv.sin_port = htons(port);			// server port

	// actually connect to the server
	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);

    // marshall params
    int plen = strlen(pathname);
    int msglen = 9 + plen; // 1: request num, 4: flags, 4: mode
    uint8_t msg[msglen];
    msg[0] = 0;
    memcpy(&msg[1], &flags, sizeof(int));
    memcpy(&msg[5], &m, sizeof(mode_t));
    memcpy(&msg[9], pathname, plen);
	send(sockfd, msg, msglen, 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);
    int fd;
    memcpy(&fd, buf, sizeof(int));
    if (fd == -1) errno = buf[4];

    // close socket
	orig_close(sockfd);

    return fd;
}

int close(int fd) {
	char *serverip;
	char *serverport;
    uint8_t buf[MAXMSGLEN];

	unsigned short port;
	int sockfd, rv;
	struct sockaddr_in srv;

	// Get environment variable indicating the ip address of the server
	serverip = getenv("server15440");
	if (!serverip) serverip = "127.0.0.1";

	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
	else {
		fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
		serverport = "15440";
	}
	port = (unsigned short)atoi(serverport);

	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error

	// setup address structure to point to server
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
	srv.sin_port = htons(port);			// server port

	// actually connect to the server
	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);

    // marshall params
    uint8_t msg[5]; // 1: request num, 4: fd
    msg[0] = 1;
    memcpy(&msg[1], &fd, sizeof(int));

	// send message to server
	send(sockfd, msg, 5, 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);

    int reply;
    memcpy(&reply, buf, sizeof(int));
    if (reply == -1) errno = buf[4];

    orig_close(sockfd);

    return reply;
}



ssize_t read(int fd, void *rbuf, size_t nbyte) {
    char *serverip;
    char *serverport;
    uint8_t buf[MAXMSGLEN+1];

    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;
    fprintf(stderr, "fd: %d\n", fd);
    fprintf(stderr, "nbyte: %zu\n", nbyte);
    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (!serverip) serverip = "127.0.0.1";

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
    else {
        fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
        serverport = "15440";
    }
    port = (unsigned short)atoi(serverport);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
    if (sockfd<0) err(1, 0);			// in case of error

    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));			// clear it first
    srv.sin_family = AF_INET;			// IP family
    srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
    srv.sin_port = htons(port);			// server port

    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv<0) err(1,0);
    // masrhalling
    uint8_t msg[13]; // 1: request num, 4: fd, 8: nbyte
    msg[0] = 3;
    memcpy(&msg[1], &fd, sizeof(int));
    memcpy(&msg[5], &nbyte, sizeof(size_t));

    // send message to server
    send(sockfd, msg, 13, 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);

    // unmarshalling
    ssize_t reply;
    memcpy(&reply, buf, sizeof(ssize_t));

    // if no error, copy what has been read into buf
    if (reply >= 0) memcpy(rbuf, &buf[9], reply);
    else buf[8] = errno;

    char haha[3];
    memcpy(haha, &buf[9], 2);
    haha[2] = '\0';
    fprintf(stderr, "read: %s\n", haha);
    fprintf(stderr, "reply: %zd\n", reply);
    // close socket
    orig_close(sockfd);

    return reply;
}


ssize_t write(int fd, const void *rbuf, size_t nbyte) {
    char *serverip;
    char *serverport;
    uint8_t buf[MAXMSGLEN+1];

    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;

    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (!serverip) serverip = "127.0.0.1";

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
    else {
        fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
        serverport = "15440";
    }
    port = (unsigned short)atoi(serverport);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
    if (sockfd<0) err(1, 0);			// in case of error

    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));			// clear it first
    srv.sin_family = AF_INET;			// IP family
    srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
    srv.sin_port = htons(port);			// server port

    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv<0) err(1,0);

    // marshall params
    uint8_t msg[(5 + nbyte)]; // 1: request num, 4: fd
    msg[0] = 2;
    memcpy(&msg[1], &fd, sizeof(int));
    memcpy(&msg[5], rbuf, nbyte);

    // send message to server
    send(sockfd, msg, (5 + nbyte), 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);

    ssize_t reply;
    memcpy(&reply, buf, sizeof(ssize_t));
    if (reply < 0) errno = buf[8];

    // close socket
    orig_close(sockfd);

    return reply;
}


off_t lseek(int fd, off_t offset, int whence) {
    char *serverip;
    char *serverport;
    uint8_t buf[MAXMSGLEN];

    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;

    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (!serverip) serverip = "127.0.0.1";

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
    else {
        fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
        serverport = "15440";
    }
    port = (unsigned short)atoi(serverport);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
    if (sockfd<0) err(1, 0);			// in case of error

    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));			// clear it first
    srv.sin_family = AF_INET;			// IP family
    srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
    srv.sin_port = htons(port);			// server port

    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv<0) err(1,0);

    // marshalling
    uint8_t msg[17]; // 1: request num, 4: fd, 8: offset, 4: whence
    msg[0] = 4;
    memcpy(&msg[1], &fd, sizeof(int));
    memcpy(&msg[5], &offset, sizeof(off_t));
    memcpy(&msg[13], &whence, sizeof(int));

    // send message to server
    send(sockfd, msg, 17, 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    off_t reply;
    memcpy(&reply, buf, sizeof(off_t));
    if (reply < 0) errno = buf[8];

    // close socket
    orig_close(sockfd);

    return reply;
}


int __xstat(int ver, const char *pathname, struct stat *sbuf) {
    char *serverip;
    char *serverport;
    char buf[MAXMSGLEN+1];

    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;

    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (serverip) serverip = "127.0.0.1";

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
    else {
        fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
        serverport = "15440";
    }
    port = (unsigned short)atoi(serverport);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
    if (sockfd<0) err(1, 0);			// in case of error

    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));			// clear it first
    srv.sin_family = AF_INET;			// IP family
    srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
    srv.sin_port = htons(port);			// server port

    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv<0) err(1,0);

    // marshalling
    int plen = strlen(pathname);
    int stat_size = sizeof(struct stat);
    uint8_t msg[(plen + stat_size + 5)]; // 1: request num, 4: ver
    msg[0] = 5;
    memcpy(&msg[1], &ver, sizeof(int));
    memcpy(&msg[5], sbuf, stat_size);
    memcpy(&msg[stat_size+5], pathname, plen);

    // send message to server
    send(sockfd, msg, (plen + stat_size + 5), 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);
    int reply;
    memcpy(&reply, buf, sizeof(int));
    if (reply < 0) errno = buf[4];

    // close socket
    orig_close(sockfd);

    return reply;
}


int unlink(const char *path) {
    char *serverip;
    char *serverport;
    uint8_t buf[MAXMSGLEN];

    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;

    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (serverip) serverip = "127.0.0.1";

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
    else {
        fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
        serverport = "15440";
    }
    port = (unsigned short)atoi(serverport);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
    if (sockfd<0) err(1, 0);			// in case of error

    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));			// clear it first
    srv.sin_family = AF_INET;			// IP family
    srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
    srv.sin_port = htons(port);			// server port

    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv<0) err(1,0);

    //marshalling
    int plen = strlen(path);
    uint8_t msg[1+plen]; // 1: request num
    msg[0] = 6;
    memcpy(&msg[1], path, plen);
    // send message to server
    send(sockfd, msg, (plen + 1), 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);
    int reply;
    memcpy(&reply, buf, sizeof(int));
    if (reply < 0) errno = buf[4];

    // close socket
    orig_close(sockfd);

    return reply;
}


ssize_t getdirentries(int fd, char *cbuf, size_t nbytes, off_t *basep) {
    char *serverip;
    char *serverport;
    uint8_t buf[MAXMSGLEN];

    unsigned short port;
    int sockfd, rv;
    struct sockaddr_in srv;

    // Get environment variable indicating the ip address of the server
    serverip = getenv("server15440");
    if (serverip) serverip = "127.0.0.1";

    // Get environment variable indicating the port of the server
    serverport = getenv("serverport15440");
    if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
    else {
        fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
        serverport = "15440";
    }
    port = (unsigned short)atoi(serverport);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
    if (sockfd<0) err(1, 0);			// in case of error

    // setup address structure to point to server
    memset(&srv, 0, sizeof(srv));			// clear it first
    srv.sin_family = AF_INET;			// IP family
    srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
    srv.sin_port = htons(port);			// server port

    // actually connect to the server
    rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
    if (rv<0) err(1,0);

    // marshalling
    size_t offt_size = sizeof(off_t);
    uint8_t msg[offt_size+13]; //1:request num  4: fd, 8: nbytes
    msg[0] = 7;
    memcpy(&msg[1], &fd, sizeof(int));
    memcpy(&msg[5], &nbytes, sizeof(size_t));
    memcpy(&msg[13], basep, offt_size);

    // send message to server
    send(sockfd, msg, offt_size+13, 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);
    ssize_t reply;
    memcpy(&reply, buf, sizeof(ssize_t));
    memcpy(basep, &buf[9], offt_size);
    memcpy(cbuf, &buf[9+offt_size], (rv - (offt_size + 9)));
    if (reply < 0) errno = buf[8];

    // close socket
    orig_close(sockfd);

    return reply;
}


struct dirtreenode* getdirtree(const char *path) {
	char *serverip;
	char *serverport;
    uint8_t buf[MAXMSGLEN];

	unsigned short port;
	int sockfd, rv;
	struct sockaddr_in srv;

	// Get environment variable indicating the ip address of the server
	serverip = getenv("server15440");
	if (!serverip) serverip = "127.0.0.1";

	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
	else {
		fprintf(stderr, "Environment variable serverport15440 not found.  Using 15440\n");
		serverport = "15440";
	}
	port = (unsigned short)atoi(serverport);

	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error

	// setup address structure to point to server
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
	srv.sin_port = htons(port);			// server port

	// actually connect to the server
	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);

    // marshalling
    int plen = strlen(path);
    uint8_t msg[plen+1];
    msg[0] = 8;
    memcpy(&msg[1], path, plen);

	// send message to server
	send(sockfd, msg, plen+1, 0);	// send message; should check return value

    rv = recv(sockfd, buf, MAXMSGLEN, 0);
    if (rv<0) err(1,0);
    size_t dir_count;
    memcpy(&dir_count, buf, sizeof(size_t));
    if (dir_count == 0) {
        orig_close(sockfd);
        errno = buf[8];
        return NULL;
    }
    char dirs_array[dir_count][30];
    uint8_t dependencies[dir_count][dir_count];
    memcpy(dirs_array, &buf[9], sizeof(dirs_array));
    memcpy(dependencies, &buf[9+sizeof(dirs_array)], sizeof(dependencies));
    int i,j,c;
    struct dirtreenode **dirlist = malloc(sizeof(struct dirtreenode *) * dir_count);
    struct dirtreenode **subdirs;
    char tmp[30];
    for (i = 0; i < dir_count; i++) {
        dirlist[i] = (struct dirtreenode *)malloc(sizeof(struct dirtreenode));
        dirlist[i]->name = malloc(sizeof(30));
        memcpy(tmp, dirs_array[i], 30);
        memcpy(dirlist[i]->name, tmp, 30);
    }
    int num_child;
    for (i = 0; i < dir_count; i++) {
        num_child = 0;
        c = 0;
        for (j = i; j < dir_count; j++) {
            if (dependencies[i][j] == 1) num_child++;
        }
        dirlist[i]->num_subdirs = num_child;
        if (num_child > 0) {
            subdirs = malloc(num_child * sizeof(struct dirtreenode *));
            for (j = 0; j < dir_count; j++) {
                if (dependencies[i][dir_count-j-1] == 1) {
                    subdirs[c] = dirlist[dir_count-j-1];
                    c++;
                }
            }
            dirlist[i]->subdirs = subdirs;
        }
    }
    struct dirtreenode *root = dirlist[0];
    free(dirlist);
    orig_close(sockfd);
    return root;
}

void free_all(struct dirtreenode *root) {
    if (root == NULL) return;
    int i;
    //fprintf(stderr, "aaa\n");
    if (root->num_subdirs > 0) {
        for (i = 0; i < (root->num_subdirs); i++) {
            free_all((root->subdirs)[i]);
        }
    }
    //fprintf(stderr, "3333\n");
    //fprintf(stderr, "name: %s\n", root->name);
    //free((root->name));
    //fprintf(stderr, "4444\n");
    free(root);
    //fprintf(stderr, "555\n");
    return;
}
void freedirtree(struct dirtreenode *root) {
    free_all(root);
}


void _init(void) {
    orig_open = dlsym(RTLD_NEXT, "open");
    orig_close = dlsym(RTLD_NEXT, "close");
    orig_read = dlsym(RTLD_NEXT, "read");
    orig_write = dlsym(RTLD_NEXT, "write");
    orig_lseek = dlsym(RTLD_NEXT, "lseek");
    orig_stat = dlsym(RTLD_NEXT, "__xstat");
    orig_unlink = dlsym(RTLD_NEXT, "unlink");
    orig_getdirentries = dlsym(RTLD_NEXT, "getdirentries");
    orig_getdirtree = dlsym(RTLD_NEXT, "getdirtree");
    orig_freedirtree = dlsym(RTLD_NEXT, "freedirtree");
    fprintf(stderr, "Init mylib \n");
}
