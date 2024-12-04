
//		gcc -g -o kvstore ./kvstore.c -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl

#include "nty_coroutine.h"

#include <arpa/inet.h>
#include "engine.c"

#define MAX_CLIENT_NUM			1000000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define MAX_TOKEN 32
#define CLIENT_MSG_LENGTH 1024

const char* result[] = {
	"success",
	"failed"
};

typedef enum cmd_result_s{
	RESULT_SUCCESS = 0,
	RESULT_FAILED
} cmd_result_t;

// 数组：
// SET key value
// GET key
// COUNT
// DELETE key
// EXIST key

// 红黑树命令：
// RSET key value
// RGET key
// RCOUNT
// RDELETE key
// REXIST key

// b树：
// BSET key value
// BGET key
// BCOUNT
// BDELETE key
// BEXIST key

// hash：
// HSET key value
// HGET key
// HCOUNT
// HDELETE key
// HEXIST key

// 跳表：
// ZSET key value
// ZGET key
// ZCOUNT
// ZDELETE key
// ZEXIST key

//redis协议，比如设置字符串
//3\r\n 表示3token SET name nn
//3\r\n	表示下面的字符长度
//SET\r\n
//4\r\n
//name\r\n
//2\r\n
//nn\r\n

// CMD_ERROR 	格式错误
// CMD_QUIT		
typedef enum cmd_s{
	SET,
	GET,
	COUNT,
	DELETE,
	EXIST,

	RSET,
	RGET,
	RCOUNT,
	RDELETE,
	REXIST,

	BSET,
	BGET,
	BCOUNT,
	BDELETE,
	BEXIST,

	HSET,
	HGET,
	HCOUNT,
	HDELETE,
	HEXIST,

	ZSET,
	ZGET,
	ZCOUNT,
	ZDELETE,
	ZEXIST,

	ERROR,
	QUIT,
}cmd_t;


#if 0
struct cmd_arg {
	const char* cmd;
	int args;
};

struct cmd_arg* commands[] = {
	{"SET", 2}, {"GET", 1}, {"COUNT", 1}, {"DELETE", 1}, {"EXIST", 1}
}
#endif

const char* commands[] = {
	"SET", "GET", "COUNT", "DELETE", "EXIST",
	"RSET", "RGET", "RCOUNT", "RDELETE", "REXIST",
	"BSET", "BGET", "BCOUNT", "BDELETE", "BEXIST",
	"HSET", "HGET", "HCOUNT", "HDELETE", "HEXIST",
	"ZSET", "ZGET", "ZCOUNT", "ZDELETE", "ZEXIST",
};


int split_tokens(char** tokens, char* msg){
	int count = 0;
	char* token = strtok(msg, " ");
	while(token != NULL){
		tokens[count++] = token;
		token = strtok(NULL, " ");
	}

	return count;
}



int parse_protocal(char* msg, char** tokens, int count){
	
	if(tokens == NULL || tokens[0] == NULL || count == 0) 
		return -1;



	int cmd = SET;
	for (cmd; cmd <= ZEXIST; cmd++)
	{
		if(strcmp(tokens[0], commands[cmd]) == 0){
			break;
		}
	}


	// for (size_t i = 0; i < count; i++)
	// {
	// 	printf("%s ", tokens[i]);
	// }
	// printf("\nparse_protocal cmd = %d\n", cmd);

	int ret = 0;
	//使用过滤器设计模式改造，再添加handler做独立处理
	switch (cmd)
	{	
	/**
	 * tokens[0] cmd
	 * tokens[1] key
	 * tokens[2] value
	 */
	//array
		case SET:
			assert(count == 3);
			ret = array_set(tokens[1], tokens[2]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0 ? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case GET:
			char* value = array_get(tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			if(value == NULL){
				snprintf(msg, CLIENT_MSG_LENGTH, result[RESULT_FAILED]);
			}else{
				snprintf(msg, CLIENT_MSG_LENGTH, value);
			}
		break;
		case COUNT:
			ret = array_count();
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, "%d", ret);
		break;
		case DELETE:
			ret = array_delete(tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0 ? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case EXIST:
			ret = array_exist(tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 1 ? RESULT_SUCCESS : RESULT_FAILED]);
		break;


		//rbtree
		case RSET:
			ret = rbtree_set(tokens[1], tokens[2]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case RGET:
			rbtree_node* node = rbtree_get(tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			if(node == NULL){
				snprintf(msg, CLIENT_MSG_LENGTH, result[RESULT_FAILED]);
			}else{
				snprintf(msg, CLIENT_MSG_LENGTH, node->value);
			}
		break;
		case RCOUNT:	
			ret = rbtree_count();
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, "%d", ret);
		break;
		case RDELETE:
			ret = rbtree_del(tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0  ? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case REXIST:
			ret = rbtree_exist(tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 1  ? RESULT_SUCCESS : RESULT_FAILED]);
		break;

		//btree
		case BSET:break;
		case BGET:break;
		case BCOUNT:break;
		case BDELETE:break;
		case BEXIST:break;

		//hash
		case HSET:
			ret = put_kv_hashtable(&hash, tokens[1], tokens[2]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case HGET:
			char* buf = get_kv_hashtable(&hash, tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			if(buf == NULL){
				snprintf(msg, CLIENT_MSG_LENGTH, result[RESULT_FAILED]);
			}else{
				snprintf(msg, CLIENT_MSG_LENGTH, buf);
			}
		break;
		case HCOUNT:
			ret = count_kv_hashtable(&hash);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, "%d", ret);
		break;
		case HDELETE:
			ret = delete_kv_hashtable(&hash, tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0  ? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case HEXIST:
			ret = exist_kv_hashtable(&hash, tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 1  ? RESULT_SUCCESS : RESULT_FAILED]);
		break;

		//跳表
		case ZSET:
			ret = put_kv_skiptable(&skiplist, tokens[1], tokens[2]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case ZGET:
			buf = get_kv_skiptable(&skiplist, tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			if(buf == NULL){
				snprintf(msg, CLIENT_MSG_LENGTH, result[RESULT_FAILED]);
			}else{
				snprintf(msg, CLIENT_MSG_LENGTH, buf);
			}
		break;
		case ZCOUNT:
			ret = count_kv_skiptable(&skiplist);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, "%d", ret);
		break;
		case ZDELETE:
			ret = delete_kv_skiptable(&skiplist, tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 0  ? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		case ZEXIST:
			ret = exist_kv_skiptable(&skiplist, tokens[1]);
			memset(msg, 0, CLIENT_MSG_LENGTH);
			snprintf(msg, CLIENT_MSG_LENGTH, result[ret == 1  ? RESULT_SUCCESS : RESULT_FAILED]);
		break;
		default:
			break;
	}
	
	return 0;
}

int protocol(char* msg, int length){
	char* tokens[MAX_TOKEN] = {0};
	int count = split_tokens(tokens, msg);
	
	return parse_protocal(msg, tokens, count);
}


void server_reader(void *arg) {
	int fd = *(int *)arg;
	int ret = 0;

 
	struct pollfd fds;
	fds.fd = fd;
	fds.events = POLLIN;

	while (1) {
		
		char buf[CLIENT_MSG_LENGTH] = {0};
		ret = recv(fd, buf, CLIENT_MSG_LENGTH, 0);
		if (ret > 0) {
			if(fd > MAX_CLIENT_NUM) {
				printf("read from server: %.*s\n", ret, buf);
			}
				
			protocol(buf, ret);
			ret = send(fd, buf, strlen(buf), 0);
			if (ret == -1) {
				close(fd);
				break;
			}
		} else if (ret == 0) {	
			close(fd);
			break;
		}

	}
}

void persistenceCoThreadFun(void* arg){
	while(1){
		sleep(5);
		engine_persistence();
	}
}

void server(void *arg) {

	unsigned short port = *(unsigned short *)arg;
	free(arg);

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return ;

	struct sockaddr_in local, remote;
	local.sin_family = AF_INET;
	local.sin_port = htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

	listen(fd, 20);
	printf("listen port : %d\n", port);

	
	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
		if (cli_fd % 1000 == 999) {

			struct timeval tv_cur;
			memcpy(&tv_cur, &tv_begin, sizeof(struct timeval));
			
			gettimeofday(&tv_begin, NULL);
			int time_used = TIME_SUB_MS(tv_begin, tv_cur);
			
			printf("client fd : %d, time_used: %d\n", cli_fd, time_used);
		}
		printf("new client comming\n");

		nty_coroutine *read_co;
		nty_coroutine_create(&read_co, server_reader, &cli_fd);
	}
}



int main(int argc, char *argv[]) {
	init_kvengine();

	nty_coroutine *co = NULL;

	int i = 0;
	unsigned short base_port = 9000;
	for (i = 0;i < 1;i ++) {
		unsigned short *port = calloc(1, sizeof(unsigned short));
		*port = base_port + i;
		nty_coroutine_create(&co, server, port); ////////no run
	}

	pthread_t persisThread;
	pthread_create(&persisThread, NULL, persistenceCoThreadFun, NULL);

	nty_schedule_run(); //run
	pthread_join(persisThread, NULL);

	dest_kvengine();
	return 0;
}



