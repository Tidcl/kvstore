#include "array.c"

#define ENABLE_PERSISTENCE 0

mempool_t mem;
int main(){
#if ENABLE_PERSISTENCE
    memp_init(&mem, sizeof(array_item_t));
    array_item_t* t1 = memp_alloc(&mem);
    t1->key = "uaisfuiawda";
    t1->value = "uaiadasdassfuiawda";
    array_item_t* t2 = memp_alloc(&mem);
    t2->key = "asafs2";
    t2->value = "2adwasdadaw";
    array_item_t* t3 = memp_alloc(&mem);
    t3->key = "wae3";
    t3->value = "awewwe3";

    array_persistence(&mem);
#else
    memp_init(&mem, sizeof(array_item_t));
    array_unpersistence(&mem);

    array_item_t* item = (array_item_t*)mem.mem;
    while(item->key != NULL){
        printf("item k=%s\t\t\t\tv=%s\n", item->key, item->value);
        item = item->next;
    }
#endif
    return 0;
}