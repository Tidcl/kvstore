#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <memory.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/sendfile.h>

#define BUFFER_LENGTH 1024  //避免出现临时数字
#define EVENT_LENGTH 1024
#define LINE_BUFFER_LENGTH 1024

#define K_MAX_LENGTH 1024
#define V_MAX_LENGTH 1024
#define MAX_KEY_COUNT 128

typedef int (*callback)(int fd, int events, void* argv);

typedef struct connect_s {
    int fd; 
    callback cb;
    char rbuffer[BUFFER_LENGTH]; 
    int rc;
    int count;  //决定下一次读多长
    char wbuffer[BUFFER_LENGTH];
    int wc;  

    char resource[BUFFER_LENGTH];
    int enable_sendfile;

    struct kvstore_s* kvheader;
}connect_t;

//动态数组
typedef struct connblock_s{
    connect_t* block;
    struct connblock_s* next;
}connblock_t;

typedef struct reactor_s{
    connblock_t* blockheader;
    int blkcnt;
    int epfd;
    struct epoll_event events[EVENT_LENGTH];
}reactor_t;


typedef struct kvpair_s{
    char key[K_MAX_LENGTH]; //避免频繁malloc，出现内存碎片。宁可提前分配好
    char value[V_MAX_LENGTH];
}kvpair_t;

//每一个connect就有一个kvstore
typedef struct kvstore_s{

    struct kvpair_s* table;
    int maxpairs;   //最多存储多少key
    int numpairs;
}kvstore_t;

int init_kvpair(kvstore_t* kvstore){
    if(kvstore == NULL) return -1;
    kvstore->table = malloc(MAX_KEY_COUNT * sizeof(kvpair_t));
    kvstore->maxpairs = MAX_KEY_COUNT;
    kvstore->numpairs = 0;  
}
int dest_kvpair(kvstore_t* kvstore){
    if(kvstore == NULL) return -1;
    free(kvstore->table);
}
int put_kvpair(kvstore_t* kvstore, const char* key, const char* value){
    if(!kvstore || !kvstore->table || !key || !value){
        return -1;
    }

    int idx = kvstore->numpairs++;  //解决多线程竞态锁住idx就行

    strncpy(kvstore->table[idx].key, key, K_MAX_LENGTH);
    strncpy(kvstore->table[idx].value, value, K_MAX_LENGTH);
}
char* get_kvpair(kvstore_t* store, const char* key){
    
    for (size_t i = 0; i < store->numpairs; i++)
    {
       if(strcmp(store->table[i].key, key) == 0){
        return store->table[i].value;
       }
    }
    return NULL;
}


//reactor初始化
int init_reactor(reactor_t* reactor){
    if(reactor == NULL) return -1;

#if 0
    reactor->blockheader = malloc(sizeof(connblock_t));
    if(reactor->blockheader == NULL) return -1;

    reactor->blockheader->block = malloc(1024 * sizeof(connect_t));
    if(reactor->blockheader->block == NULL) return -1;
#else
    reactor->blockheader = (connblock_t*)malloc(BUFFER_LENGTH * sizeof(connect_t) + sizeof(connblock_t));
    if(reactor->blockheader == NULL) return -1;
    reactor->blockheader->block = (connect_t*)(reactor->blockheader + 1);
    reactor->blkcnt = 1;
    reactor->blockheader->next = NULL;
    reactor->epfd = epoll_create(1);
#endif
return 0;
}

int connect_block(reactor_t* reactor){
    connblock_t* blk = reactor->blockheader;
    while(blk->next != NULL){
        blk = blk->next;
    }

    connblock_t* newblk = (connblock_t*)malloc(BUFFER_LENGTH * sizeof(connect_t) + sizeof(connblock_t));
    newblk->block = (connect_t*)(newblk + 1);
    newblk->next = NULL;
    blk->next = newblk;

    reactor->blkcnt++;
    return 0;
}

connect_t* connect_index(reactor_t* reactor, int fd){
    if(reactor == NULL) return NULL;

    int blkidx = fd / EVENT_LENGTH; //第几块
    while(blkidx >= reactor->blkcnt){
        connect_block(reactor);
    }

    int i = 0;
    connblock_t* blk = reactor->blockheader;
    while (i++ < blkidx)
    {
        blk = blk->next;
    }
    return &blk->block[fd % EVENT_LENGTH];
}

void dest_reactor(reactor_t* reactor){
    printf("destory reactor.\n");
    if(reactor == NULL) return;

    if(reactor->blockheader){
        free(reactor->blockheader);
    }

    close(reactor->epfd);
}


int readline(char* allbuf, int idx, char* linebuf){
    
    memset(linebuf, 0, LINE_BUFFER_LENGTH);
    int len = strlen(allbuf);
    // printf(allbuf[idx]);
    for (; idx < len; idx++)
    {
        if(allbuf[idx] == '\r' && allbuf[idx+1] == '\n'){
            return idx + 2;
        }else{
            *(linebuf++) = allbuf[idx];
        }
    }
    return idx;
}

#define HTTP_WEB_ROOT "."
// GET / HTTP/1.1
// Host: 192.168.31.222:9997
// Connection: keep-alive
// Cache-Control: max-age=0
// Upgrade-Insecure-Requests: 1
// User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0
// Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
// Accept-Encoding: gzip, deflate
// Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6
int http_request(connect_t* conn){
    printf("http --> request:\n%s\n", conn->rbuffer);

    char linebuffer[1024] = {0};
    int idx = readline(conn->rbuffer, 0, linebuffer);
#if 1
    if(strstr(linebuffer, "GET")){
        int i = 0;
        while (linebuffer[sizeof("GET ") + i] != ' '){
            i++;
        }
        linebuffer[sizeof("GET ") + i] = '\0';
        memset(conn->resource, 0, BUFFER_LENGTH);
        sprintf(conn->resource, "%s%s", HTTP_WEB_ROOT, linebuffer + 4);
    }else{
        idx = readline(conn->rbuffer, idx, linebuffer);
        int i = 0;
        char* key = linebuffer;
        while(linebuffer[i++] != ':');
        linebuffer[i] = '\0';

        char* value = linebuffer + i + 1;

        put_kvpair(conn->kvheader, key, value);
    }
    // idx = readline(conn->rbuffer, idx, linebuffer);
#endif

    return 0;
}

int http_response(connect_t* conn){

#if 1 //sendfile
    int filefd = open(conn->resource, O_RDONLY);
    if(filefd == -1){
        printf("open failed\n");
        return -1;
    }
    
    struct stat stat_buf;
    fstat(filefd, &stat_buf);
    close(filefd);

    int len = sprintf(conn->wbuffer, 
    "HTTP/1.1 200 OK\r\n"
    "Accept-Ranges: bytes\r\n"
    "Content-Length: %ld\r\n"
    "Content-Type: text/html\r\n"
    "Date: Sat Nov 30 03:04:18 PM UTC 2024\r\n"
    "\r\n", stat_buf.st_size);

    conn->wc = len;
    conn->enable_sendfile = 1;

    
#elif 0 //读取文件
    int filefd = open(conn->resource, O_RDONLY);
    if(filefd == -1){
        printf("open failed\n");
        return -1;
    }
    struct stat stat_buf;
    fstat(filefd, &stat_buf);


    int len = sprintf(conn->wbuffer, 
    "HTTP/1.1 200 OK\r\n"
    "Accept-Ranges: bytes\r\n"
    "Content-Length: %ld\r\n"
    "Content-Type: text/html\r\n"
    "Date: Sat Nov 30 03:04:18 PM UTC 2024\r\n"
    "\r\n", stat_buf.st_size);

    len += read(filefd, conn->wbuffer + len, BUFFER_LENGTH - len);
    conn->wc = len;
    close(filefd);
#endif
    return 0;
}

int init_server(int port){
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(struct sockaddr_in));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    //绑定
    if( -1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))){
        printf("bind failed: %s", strerror(errno)); 
        return -1;  
    }
    printf("listen port: %d\n", port);
    //监听
    listen(sockfd, 10); //backlog 队列的最大长度为10
    return sockfd;
}

void set_event(int epfd, int iofd, int op, int event){
    struct epoll_event ev;
    ev.events = event;
    ev.data.fd = iofd;
    epoll_ctl(epfd, op, iofd, &ev);
}


int set_listener(reactor_t* reactor, int fd, callback cb){
    if(reactor == NULL || reactor->blockheader == NULL) return -1;
    reactor->blockheader->block[fd].fd = fd;
    reactor->blockheader->block[fd].cb = cb;

    set_event(reactor->epfd, fd, EPOLL_CTL_ADD, EPOLLIN);
    return 0;
}

extern int recv_cb(int fd, int events, void* argv);

int send_cb(int fd, int events, void* argv){
    reactor_t* reactor = (reactor_t*)argv;
    connect_t* conn = connect_index(reactor, fd);
    int ret = send(fd, conn->wbuffer, conn->wc, 0);
    conn->wc -= ret;

#if 0   //使用sendfile 0拷贝发送
    if(conn->enable_sendfile){
        int filefd = open(conn->resource, O_RDONLY);
        if(filefd == -1){
            printf("open failed\n");
            return -1;
        }
        struct stat stat_buf;
        fstat(filefd, &stat_buf);
        int ret = sendfile(fd, filefd, NULL, stat_buf.st_size);
        if(ret == -1){
            printf("sendfile failed errno: %d\n", ret);
        }
        close(filefd);
    }
#endif

    conn->cb = recv_cb;
    set_event(reactor->epfd, fd, EPOLL_CTL_MOD, EPOLLIN);
    return 0;
}

int recv_cb(int fd, int events, void* argv){
    reactor_t* reactor = (reactor_t*)argv;
    connect_t* conn = connect_index(reactor, fd);

    // short length = 0;    //模拟读取协议长度，假设协议前两个字节表示数据长度
    // recv(fd, &length, 2, 0);
    // length = ntohs(length); //当协议修改了，就不好变通

    int ret = recv(fd, conn->rbuffer + conn->rc, conn->count, 0);
    if(ret < 0){
        printf("recv error %s \n", strerror(errno));
        conn->fd = -1;
        conn->rc = 0;
        conn->wc = 0;

        set_event(reactor->epfd, fd, EPOLL_CTL_DEL, 0);
        close(fd);
        dest_kvpair(conn->kvheader);
        return -1;
    }else if(ret == 0){
        conn->fd = -1;
        conn->rc = 0;
        conn->wc = 0;

        set_event(reactor->epfd, fd, EPOLL_CTL_DEL, 0);
        close(fd);
        dest_kvpair(conn->kvheader);
        return -1;
    }
    printf("recv: %s\n", conn->rbuffer);
    conn->rc += ret;
#if 0
    http_request(conn);
    conn->cb = send_cb;
    set_event(reactor->epfd, fd, EPOLL_CTL_MOD, EPOLLOUT);
#elif 1 //echo
    conn->wc = conn->rc;
    memcpy(conn->wbuffer, conn->rbuffer, conn->rc);
    conn->rc -= conn->rc;
    memset(conn->rbuffer, 0, BUFFER_LENGTH);

    conn->cb = send_cb;
    set_event(reactor->epfd, fd, EPOLL_CTL_MOD, EPOLLOUT);
#elif 0
    conn->count = 20;
    // reactor->blockheader->block[fd].cb = send_cv;
    set_event(reactor->epfd, fd, EPOLL_CTL_ADD, EPOLLIN);
#endif
    return 0;
}

int accept_cb(int fd, int events, void* argv){
    reactor_t* reactor = (reactor_t*)argv;
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    connect_t* conn = connect_index(reactor, clientfd);
    conn->fd = clientfd;
    conn->cb = recv_cb;
    conn->count = BUFFER_LENGTH;

    conn->kvheader = malloc(sizeof(kvstore_t));
    init_kvpair(conn->kvheader);

    set_event(reactor->epfd, clientfd, EPOLL_CTL_ADD, EPOLLIN);
    return 0;
}

int main(int argc, char* argv[]){
    #if 1
        if(argc < 2) return -1;

        reactor_t reactor;
        if(-1 == init_reactor(&reactor)){
            return -1;
        }

        int port = atoi(argv[1]);
        for (size_t i = 0; i < 1; i++)    //2024年11月30日 11点23分 代码改为监听100个端口
        {
            int sockfd = init_server(port+i);
            if(sockfd == -1){
                return -1;
            }
             set_listener(&reactor, sockfd, accept_cb);   
        }

        
        struct epoll_event events[EVENT_LENGTH] = {0};
        while(1){
            int nready = epoll_wait(reactor.epfd, events, EVENT_LENGTH, -1);
            if(nready < 0) continue;
            for (size_t i = 0; i < nready; i++)
            {   
                int iofd = events[i].data.fd;
                connect_t* conn = connect_index(&reactor, iofd);
                if (events[i].events & EPOLLIN)
                {
                    conn->cb(iofd, events[i].events, &reactor);
                }

                if (events[i].events & EPOLLOUT)
                {
                    conn->cb(iofd, events[i].events, &reactor);
                }   
            }
        }
    #else
    

    // 设置fd为非阻塞
    // int flags = fcntl(sockfd, F_GETFL, 0);
    // flags |= O_NONBLOCK;
    // fcntl(sockfd, F_SETFL, flags);

    //accept 从监听处创建一个fd，创建一个客户端的专属套接字
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    int epfd = epoll_create(1); //该参数表示epoll能容纳多少fd，但是因为linux3.0之后更新了内部实现，只需要该值大于1即可

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = sockfd;    //绑定一个数据，当事件触发时，可以取出该数据进行逻辑处理
    epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev);

    struct epoll_event events[1024] = {0};   //每次取出多少个事件
    while(1){
        int nready = epoll_wait(epfd, events, 1024, -1);
        if(nready < 0) continue;
        for (size_t i = 0; i < nready; i++)
        {
            int connfd = events[i].data.fd;
            if(connfd == sockfd)
            {
                int clientfd = accept(sockfd, (struct sockaddr*)&clientaddr, &len);
                struct epoll_event cev;
                cev.events = EPOLLIN | EPOLLET;   
                cev.data.fd = clientfd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, clientfd, &cev);
            }else if(events[i].events & EPOLLIN ) {
                // char buffer[BUFFER_LENGTH] = {0};

                int ret = recv(connfd, buffer, BUFFER_LENGTH, 0);
                if(ret == 0) {
                    printf("close %d\n", connfd);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, connfd, NULL);
                    close(connfd);
                }else if(ret > 0){
                    printf("ret: %d, buffer: %s\n", ret, buffer);
                    send(connfd, buffer, ret, 0);
                }
            }
        }
    }
#endif
    dest_reactor(&reactor);
    return 0;
}