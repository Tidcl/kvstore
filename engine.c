#include "array.c"
#include "rbtree.c"
#include "hash.c"
#include "dhash.c"
#include "skiptable.c"


#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1
#define ENABLE_HASH 1
#define ENABLE_SKIPLIST 1

void init_kvengine(void) {
    init_array();
#if ENABLE_RBTREE
	init_rbtree(&tree);
#endif
#if ENABLE_HASH
	init_hashtable(&hash);
	hashtable = create_hash_table(INITIAL_SIZE);
#endif
#if ENABLE_SKIPLIST
	init_skiptable(&skiplist);
#endif

    engine_unpersistence();
}


void dest_kvengine(void) {
    engine_persistence();

    dest_array();
#if ENABLE_RBTREE
	dest_rbtree(&tree);
#endif
#if ENABLE_HASH
	dest_hashtable(&hash);
	destroy_hash_table(hashtable);
#endif
#if ENABLE_SKIPLIST
	dest_skiptable(&skiplist);
#endif
}


void engine_persistence(){
    __pid_t pid = fork();
    if(pid == 0){
        printf("in sub proceesor persistence\n");
        array_persistence(&mem1);
        rbtree_persistence(&tree);
        exit(0);
    }
}

void engine_unpersistence(){
    array_unpersistence(&mem1);
    rbtree_unpersistence(&tree);
}