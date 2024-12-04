#include <stdio.h>
#include <assert.h>
#include <sys/socket.h>
#include <string.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/time.h>

int print;

int conn_kvstore(const char* ip, int port){
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    printf("connfd=%d %s %d\n", connfd, ip, port);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(struct sockaddr_in));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    int ret = connect(connfd, (struct sockaddr*)&addr, sizeof(struct sockaddr_in));
    printf("connect ret = %d\n", ret);
    if(ret){
        return -1;
    }
    return connfd;
}

int testcase(int connfd, const char* cmd, const char* result, char* casename){
    send(connfd, cmd, strlen(cmd), 0);
    char buffer[1024] = {0};
    int ret = recv(connfd, buffer, 1024, 0);
    if(ret <= 0){
        perror(errno);
    }else{
        if(print){
            printf("[%s]%s", cmd, buffer);
            if(strcmp(buffer, result) == 0){
                printf(" Yes\n");
                return 0;
            }else{
                printf(" No\n");
                return -1;
            }
        }
    }
}

void array_test(int connfd){
    testcase(connfd, "SET 1 1", "success", "array");
    testcase(connfd, "COUNT", "1", "array");
    testcase(connfd, "SET 2 2", "success", "array");
    testcase(connfd, "COUNT", "2", "array");
    testcase(connfd, "SET 3 3", "success", "array");
    testcase(connfd, "COUNT", "3", "array");
    testcase(connfd, "SET 4 4", "success", "array");
    testcase(connfd, "COUNT", "4", "array");
    printf("\n");
    testcase(connfd, "EXIST 1", "success", "array");
    testcase(connfd, "GET 1", "1", "array");
    testcase(connfd, "DELETE 1", "success", "array");
    testcase(connfd, "COUNT", "2", "array");
    printf("\n");
    testcase(connfd, "GET 3", "3", "array");
    testcase(connfd, "GET 3", "3", "array");
    testcase(connfd, "GET 4", "failed", "array");
    printf("\n");
    testcase(connfd, "GET nano", "nano", "array");
    testcase(connfd, "COUNT", "1", "array");
    testcase(connfd, "DELETE nano", "success", "array");
    testcase(connfd, "EXIST nano", "success", "array");
    printf("\n");
}

void rbtree_test(int connfd){
    
    testcase(connfd, "RSET 1 1", "success", "rbtree");
    testcase(connfd, "RCOUNT", "1", "rbtree");
    testcase(connfd, "RSET 2 2", "success", "rbtree");
    testcase(connfd, "RCOUNT", "2", "rbtree");
    testcase(connfd, "RSET 3 3", "success", "rbtree");
    testcase(connfd, "RCOUNT", "3", "rbtree");
    testcase(connfd, "RSET 4 4", "success", "rbtree");
    testcase(connfd, "RCOUNT", "4", "rbtree");
    testcase(connfd, "REXIST 1", "success", "rbtree");
    testcase(connfd, "RGET 1", "1", "rbtree");
    testcase(connfd, "RDELETE 1", "success", "rbtree");
    testcase(connfd, "RCOUNT", "3", "rbtree");
    testcase(connfd, "RGET 3", "3", "rbtree");
    testcase(connfd, "RGET 3", "3", "rbtree");
    testcase(connfd, "RGET 4", "failed", "rbtree");
    testcase(connfd, "RGET nano", "nano", "rbtree");
    testcase(connfd, "RCOUNT", "1", "rbtree");
    testcase(connfd, "RDELETE nano", "success", "rbtree");
    testcase(connfd, "REXIST nano", "success", "rbtree");

    testcase(connfd, "RSET nano nano", "success", "rbtree");
    testcase(connfd, "RGET nano", "nano", "rbtree");
    testcase(connfd, "RCOUNT", "1", "rbtree");
    testcase(connfd, "RDELETE nano", "success", "rbtree");
    testcase(connfd, "REXIST nano", "success", "rbtree");
}


void hash_test(int connfd){
    
    testcase(connfd, "HSET 1 1", "success", "hash");
    testcase(connfd, "HCOUNT", "1", "hash");
    testcase(connfd, "HSET 2 2", "success", "hash");
    testcase(connfd, "HCOUNT", "2", "hash");
    testcase(connfd, "HSET 3 3", "success", "hash");
    testcase(connfd, "HCOUNT", "3", "hash");
    testcase(connfd, "HSET 4 4", "success", "hash");
    testcase(connfd, "HCOUNT", "4", "hash");
    testcase(connfd, "HEXIST 1", "success", "hash");
    testcase(connfd, "HGET 1", "1", "hash");
    testcase(connfd, "HDELETE 1", "success", "hash");
    testcase(connfd, "HCOUNT", "3", "hash");
    testcase(connfd, "HGET nano", "nano", "hash");

    testcase(connfd, "HGET 3", "3", "hash");
    testcase(connfd, "HGET 3", "3", "hash");
    testcase(connfd, "HGET 4", "failed", "hash");
    
    testcase(connfd, "HCOUNT", "1", "hash");
    testcase(connfd, "HDELETE 1", "success", "hash");
    testcase(connfd, "HEXIST nano", "success", "hash");

    testcase(connfd, "HSET nano nano", "success", "hash");
    testcase(connfd, "HGET nano", "nano", "hash");
    testcase(connfd, "HCOUNT", "1", "hash");
    testcase(connfd, "HDELETE nano", "success", "hash");
    testcase(connfd, "HEXIST nano", "success", "hash");
}

void rbtree_test_10w(int connfd){
    for (size_t i = 0; i < 1e5; i++)
    {
        char msg[512] = {0};
        snprintf(msg, 512, "RSET rkey%d rvalue%d", i, i);

        char casename[64] = {0};
        snprintf(casename, 64, "case%d", i);
        testcase(connfd, msg, "success", casename);
    }
}

void array_test_10w(int connfd){
    for (size_t i = 0; i < 1e5; i++)
    {
        char msg[512] = {0};
        snprintf(msg, 512, "SET key%d value%d", i, i);

        char casename[64] = {0};
        snprintf(casename, 64, "case%d", i);
        testcase(connfd, msg, "success", casename);
    }
}

void hash_test_10w(int connfd){
    for (size_t i = 0; i < 1e5; i++)
    {
        char msg[512] = {0};
        snprintf(msg, 512, "HSET key%d value%d", i, i);

        char casename[64] = {0};
        snprintf(casename, 64, "case%d", i);
        testcase(connfd, msg, "success", casename);
    }
}

void skiplist_test(int connfd){
    testcase(connfd, "ZSET 1 1", "success", "skiplist");
    testcase(connfd, "ZCOUNT", "1", "skiplist");
    testcase(connfd, "ZSET 2 2", "success", "skiplist");
    testcase(connfd, "ZCOUNT", "2", "skiplist");
    testcase(connfd, "ZSET 3 3", "success", "skiplist");
    testcase(connfd, "ZCOUNT", "3", "skiplist");
    testcase(connfd, "ZSET 4 4", "success", "skiplist");
    testcase(connfd, "ZCOUNT", "4", "skiplist");
    testcase(connfd, "ZEXIST 1", "success", "skiplist");
    testcase(connfd, "ZGET 1", "1", "skiplist");
    testcase(connfd, "ZDELETE 1", "success", "skiplist");
    testcase(connfd, "ZCOUNT", "3", "skiplist");
    testcase(connfd, "ZGET nano", "nano", "skiplist");

    testcase(connfd, "ZGET 3", "3", "skiplist");
    testcase(connfd, "ZGET 3", "3", "skiplist");
    testcase(connfd, "ZGET 4", "failed", "skiplist");
    
    testcase(connfd, "ZCOUNT", "1", "skiplist");
    testcase(connfd, "ZDELETE 1", "success", "skiplist");
    testcase(connfd, "ZEXIST nano", "success", "skiplist");

    testcase(connfd, "ZSET nano nano", "success", "skiplist");
    testcase(connfd, "ZGET nano", "nano", "skiplist");
    testcase(connfd, "ZCOUNT", "1", "skiplist");
    testcase(connfd, "ZDELETE nano", "success", "skiplist");
    testcase(connfd, "ZEXIST nano", "success", "skiplist");
}

void skiplist_test_10w(int connfd){
for (size_t i = 0; i < 1e5; i++)
    {
        char msg[512] = {0};
        snprintf(msg, 512, "ZSET key%d value%d", i, i);

        char casename[64] = {0};
        snprintf(casename, 64, "case%d", i);
        testcase(connfd, msg, "success", casename);
    }
}

int main(int argc, char* argv[]){
    print = 0;
    assert(argc == 3);
    
    if(argc != 3){
        printf("argc != 3\n");
        return -1;
    }

    const char* ip = argv[1];
    int port = atoi(argv[2]);

    int connfd = conn_kvstore(ip, port);
    if(connfd == -1){
        printf("connect failed\n");
        return -1;
    }

{
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    array_test_10w(connfd);
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    int ms = (tv_end.tv_sec - tv_begin.tv_sec)*1000 + (tv_end.tv_usec - tv_begin.tv_usec)/1000;
    printf("array_test_10w time used: %dms\n", ms);
}

{
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    rbtree_test_10w(connfd);
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    int ms = (tv_end.tv_sec - tv_begin.tv_sec)*1000 + (tv_end.tv_usec - tv_begin.tv_usec)/1000;
    printf("rbtree_test_10w time used: %dms\n", ms);
}

{
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    hash_test_10w(connfd);
    struct timeval tv_end;   
    gettimeofday(&tv_end, NULL);
    int ms = (tv_end.tv_sec - tv_begin.tv_sec)*1000 + (tv_end.tv_usec - tv_begin.tv_usec)/1000;
    printf("hash_test_10w time used: %dms\n", ms);
}

    
{
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
    skiplist_test_10w(connfd);
    struct timeval tv_end;   
    gettimeofday(&tv_end, NULL);
    int ms = (tv_end.tv_sec - tv_begin.tv_sec)*1000 + (tv_end.tv_usec - tv_begin.tv_usec)/1000;
    printf("skiplist_test_10w time used: %dms\n", ms);
}

    print = 1;
    testcase(connfd, "COUNT", "100000", "array");
    testcase(connfd, "RCOUNT", "100000", "rbtree");
    testcase(connfd, "HCOUNT", "100000", "hash");
    testcase(connfd, "ZCOUNT", "100000", "skiplist");
    close(connfd);

    return 0;        
}