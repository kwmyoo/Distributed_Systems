#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
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
#include "../include/dirtree.h"
#define MAXMSGLEN 100000
struct queue_node {
    int num;
    struct dirtreenode *dir;
    struct queue_node *next;
    struct queue_node *prev;
};
struct queue {
    struct queue_node *head;
    struct queue_node *tail;
};

struct queue *initialize_queue() {
    struct queue *Q = malloc(sizeof(struct queue));
    Q->head = NULL;
    Q->tail = NULL;
    return Q;
}

void enqueue(struct queue *Q, struct dirtreenode *target, int cnt) {
    struct queue_node *new = malloc(sizeof(struct queue_node));
    new->next = NULL;
    new->dir = target;
    new->prev = Q->tail;
    new->num = cnt;
    if (Q->head == NULL) Q->head = new;
    else (Q->tail)->next = new;
    Q->tail = new;
}
struct queue_node *dequeue(struct queue *Q) {
    struct queue_node *temp;
    if (Q->head == NULL) return NULL;
    if (Q->head == Q->tail) {
        temp = Q->head;
        Q->head = NULL;
        Q->tail = NULL;
        return temp;
    }
    struct queue_node *last = Q->tail;
    (last->prev)->next = NULL;
    Q->tail = last->prev;
    return last;
}


int count_all_dirs(struct dirtreenode *head) {
    if (head == NULL) return 0;
    int current = head->num_subdirs;
    int total_count = 1;
    int i, temp;
    for (i = 0; i < current; i++) {
        struct dirtreenode *child = (head->subdirs)[i];
        temp = count_all_dirs(child);
        total_count += temp;
    }
    return total_count;
}

void free_all(struct dirtreenode *root) {
    if (root == NULL) return;
    int i;
    if (root->num_subdirs > 0) {
        for (i = 0; i < (root->num_subdirs); i++) {
            free_all((root->subdirs)[i]);
        }
        free((root->subdirs));
    }
    free((root->name));
    free(root);
}

int main(int argc, char**argv) {
	uint8_t buf[MAXMSGLEN];
	char *serverport;

	unsigned short port;
	int sockfd, sessfd, rv;
	struct sockaddr_in srv, cli;
	socklen_t sa_size;

	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) port = (unsigned short)atoi(serverport);
	else port=15440;

	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error

	// setup address structure to indicate server port
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = htonl(INADDR_ANY);	// don't care IP address
	srv.sin_port = htons(port);			// server port

	// bind to our port
	rv = bind(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);

	// start listening for connections
	rv = listen(sockfd, 5);
	if (rv<0) err(1,0);

	// main server loop, handle clients one at a time, quit after 10 clients
	while( 1 ) {
		// wait for next client, get session socket
		sa_size = sizeof(struct sockaddr_in);
		sessfd = accept(sockfd, (struct sockaddr *)&cli, &sa_size);
		if (sessfd<0) err(1,0);
        rv = fork();
        if (rv == 0) { // child process
            //close(sockfd);
    		// get messages and send replies to this client, until it goes away
	    	while ( (rv=recv(sessfd, buf, MAXMSGLEN, 0)) > 0) {
		    	if (buf[0] == 0) { // open
                    // unmarshall
                    int flags;
                    mode_t m;
                    memcpy(&flags, &buf[1], sizeof(int));
                    memcpy(&m, &buf[5], sizeof(mode_t));
                    ssize_t plen = rv - 9;
                    char pathname[plen+1];
                    memcpy(pathname, &buf[9], plen);
                    pathname[plen] = '\0';

                    // marshall
                    uint8_t msg[5]; // 4: return value (int), 1: errno
                    int result = open(pathname, flags, m);
                    memcpy(msg, &result, sizeof(int));
                    msg[4] = errno;
                    send(sessfd, msg, 5, 0);
                } else if (buf[0] == 1) { // close
                    //unmarshall
                    int fd;
                    memcpy(&fd, &buf[1], sizeof(int));

                    // marshall
                    uint8_t msg[5]; // 4: return value, 1: errno
                    int result = close(fd);
                    memcpy(msg, &result, sizeof(int));
                    msg[4] = errno;
                    send(sessfd, msg, 5, 0);
                } else if (buf[0] == 2) { // write
                    // unmarshall
                    int fd;
                    size_t nbyte;
                    memcpy(&fd, &buf[1], sizeof(int));
                    memcpy(&nbyte, &buf[5], sizeof(size_t));

                    uint8_t writebuf[nbyte];
                    if (nbyte > (rv-13)) { // if the input is big
                        memcpy(writebuf, &buf[13], (rv - 13));
                        size_t counter = rv - 13;
                        while ((counter < nbyte) &&
                               ((rv = recv(sessfd, buf, MAXMSGLEN, 0)) > 0)) {
                            memcpy(&writebuf[counter], buf, rv);
                            counter += (size_t)rv;
                        }
                    } else memcpy(writebuf, &buf[13],  nbyte);

                    // marshall
                    uint8_t msg[9]; // 8: retrurn value, 1: errno
                    ssize_t result = write(fd, writebuf, nbyte);
                    memcpy(msg, &result, sizeof(ssize_t));
                    msg[8] = errno;

                    send(sessfd, msg, 9, 0);
                } else if (buf[0] == 3) { // read
                    // unmarshall
                    int fd;
                    size_t nbyte;
                    memcpy(&fd, &buf[1], sizeof(int));
                    memcpy(&nbyte, &buf[5], sizeof(size_t));

                    // marshall
                    uint8_t msg[nbyte+9]; // 8: return val, 1: errno
                    ssize_t result = read(fd, &msg[9], nbyte);
                    memcpy(msg, &result, sizeof(ssize_t));
                    msg[8] = errno;
                    size_t newlen;
                    if (result < 0) newlen = 0;
                    else newlen = (size_t)result;
                    send(sessfd, msg, newlen + 9, 0);
                } else if (buf[0] == 4) { // lseek
                    int fd, whence;
                    off_t offset;
                    memcpy(&fd, &buf[1], sizeof(int));
                    memcpy(&offset, &buf[5], sizeof(off_t));
                    memcpy(&whence, &buf[13], sizeof(int));

                    uint8_t msg[9]; // 8: return val, 1: errno;
                    off_t result = lseek(fd, offset, whence);
                    memcpy(msg, &result, sizeof(off_t));
                    msg[8] = errno;

                    send(sessfd, msg, 9, 0);
                } else if (buf[0] == 5) { // xstat
                    int stat_size = sizeof(struct stat);
                    int plen = rv - (5 + stat_size);

                    int ver;
                    struct stat *sbuf = malloc(sizeof(struct stat));
                    char pathname[plen+1];
                    memcpy(&ver, &buf[1], sizeof(int));
                    memcpy(sbuf, &buf[5], stat_size);
                    memcpy(pathname, &buf[stat_size+5], plen);
                    pathname[plen] = '\0';

                    uint8_t msg[5]; // 4: return val, 1: errno
                    int result = __xstat(ver, pathname, sbuf);
                    memcpy(msg, &result, sizeof(int));
                    msg[4] = errno;
                    free(sbuf);

                    send(sessfd, msg, 5, 0);
                } else if (buf[0] == 6) { // unlink
                    char pathname[rv];
                    memcpy(pathname, &buf[1], (rv-1));
                    pathname[rv-1] = '\0';

                    uint8_t msg[5];
                    int result = unlink(pathname);
                    memcpy(msg, &result, sizeof(int));
                    msg[4] = errno;

                    send(sessfd, msg, 5, 0);
                } else if (buf[0] == 7) { // getdirentries
                    int fd;
                    size_t offt_size = sizeof(off_t);
                    size_t nbytes;
                    off_t *basep = malloc(sizeof(off_t));
                    memcpy(&fd, &buf[1], sizeof(int));
                    memcpy(&nbytes, &buf[5], sizeof(nbytes));
                    memcpy(basep, &buf[13], offt_size);

                    uint8_t msg[nbytes+offt_size+9]; // 8: return val, 1: errno
                    ssize_t result = getdirentries(fd, &msg[9u+offt_size], nbytes, basep);
                    memcpy(msg, &result, sizeof(ssize_t));
                    msg[8] = errno;
                    size_t newlen;
                    if (result < 0) newlen = 0;
                    else newlen = (size_t)result;
                    memcpy(&msg[9], basep, offt_size);
                    free(basep);

                    send(sessfd, msg, (newlen+offt_size+9), 0);
                } else if (buf[0] == 8) { // getdirtree
                    char path[rv];
                    memcpy(path, &buf[1], (rv-1));
                    path[rv-1] = '\0';

                    struct dirtreenode *root = getdirtree(path);
                    size_t dir_count = count_all_dirs(root);
                    char dirs_array[dir_count][30]; // array of names
                    uint8_t dependencies[dir_count][dir_count]; // dependency matrix
                    memset(dependencies, 0, dir_count * dir_count * sizeof(int8_t));

                    int counter = 0;
                    int num_child, i, num;
                    struct queue *Q = initialize_queue();
                    enqueue(Q, root, counter);
                    struct queue_node *target;
                    struct dirtreenode *temp;

                    while(1) {
                        target = dequeue(Q);
                        if (target == NULL) break;
                        temp = target->dir;
                        memcpy(dirs_array[target->num], temp->name, 30);
                        num_child = temp->num_subdirs;
                        for (i = 0; i < num_child; i++) {
                            counter++;
                            enqueue(Q, (temp->subdirs)[i], counter);
                            dependencies[target->num][counter] = 1;
                        }
                        free(target);
                    }
                    uint8_t msg[9 + sizeof(dirs_array) + sizeof(dependencies)];
                    memcpy(msg, &dir_count, sizeof(size_t));
                    msg[8] = errno;
                    memcpy(&msg[9], dirs_array, sizeof(dirs_array));
                    memcpy(&msg[9+sizeof(dirs_array)], dependencies, sizeof(dependencies));
                    send(sessfd, msg, (9 + sizeof(dirs_array) + sizeof(dependencies)), 0);
                    free_all(root);
                }
		    }
        }
        // parent process
        close(sessfd);
	}

	// close socket
	close(sockfd);

	return 0;
}

