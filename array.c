#pragma once
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

// struct ArrayFilter{};

// struct GETFilter{};

// struct COUNTFilter{};

// struct DELETEFilter{};

// struct EXISTFilter{};

// #include "mem_pool/mempool.c"

#define ENABLE_ARRAY 1
#define ENABLE_MEM_POLL 1

#define ARRAY_ITEM_SIZE 100000+1

typedef struct array_item_s{
#if ENABLE_MEM_POLL
    void* next;
#endif
	char* key;
	char* value;
}array_item_t;

typedef struct mempool_s {
	int block_size; // 
	int free_count;

	char *free_ptr;
	char *mem;
    char* startAddr;
} mempool_t;

#if ENABLE_MEM_POLL
mempool_t mem1;
#else
array_item_t array_items[ARRAY_ITEM_SIZE] = {0};
#endif

void* mpmalloc(size_t size){
    return malloc(size);
}

void mpfree(void* ptr){
    free(ptr);
}

#if ENABLE_MEM_POLL
#define MEM_ITEM_SIZE	100001

int memp_init(mempool_t *m, int block_size) {

	if (!m) return -2;

    int mem_page_size = MEM_ITEM_SIZE * block_size;

    // m->item_count = mem_page_size;
	m->block_size = block_size;
	m->free_count = mem_page_size / block_size;

    // printf("malloc mem %d\n", mem_page_size);
	m->free_ptr = (char*)malloc(mem_page_size);
    m->startAddr = m->free_ptr;
    memset(m->free_ptr, 0, mem_page_size);
	if (!m->free_ptr) return -1;
	m->mem = m->free_ptr;

	int i = 0;
	char *ptr = m->free_ptr;
	for (i = 0;i < m->free_count;i ++) {

		*(char **)ptr = ptr + block_size;
        //king老师你觉得这样行不行？
        // long long nextPtrStartAddr = ptr + block_size;
		// memcpy(ptr, &nextPtrStartAddr, sizeof(void*));
        ptr += block_size;

	}
	*(char **)ptr = NULL;

	return 0;
}


void* memp_alloc(mempool_t *m) {

	if (!m || m->free_count == 0) return NULL;

	void *ptr = m->free_ptr;

	m->free_ptr = *(char **)ptr;
	m->free_count --;

	return ptr;
}

void *memp_free(mempool_t *m, void *ptr) {

	*(char**)ptr = m->free_ptr;
	m->free_ptr = (char *)ptr;
	m->free_count ++;
}

void memp_destory(mempool_t *m){
    if(m){
        free(m->startAddr);
    }
}

void init_array(){
    memp_init(&mem1, sizeof(array_item_t));
}

void dest_array(){
    memp_destory(&mem1);
}
#endif



#if ENABLE_ARRAY
int _array_count = 0;
pthread_mutex_t array_lock;

array_item_t* search_item(const char* key){
    if(!key) return NULL;

#if ENABLE_MEM_POLL
    array_item_t* itemPtr = (array_item_t*)mem1.mem;
    while(itemPtr){
        if(itemPtr->next != NULL  && itemPtr->key != NULL && strcmp(itemPtr->key, key) == 0){
            return itemPtr;
        }

        if(itemPtr->next != NULL){
            itemPtr = itemPtr->next;
        }else{
            break;
        }
    }
#else
    int i = 0;
    for (i = 0; i < _array_count; i++)
    {
        if(array_items[i].key && 0 == strcmp(array_items[i].key, key)){
            return array_items + i;
        }
    }
#endif
    return NULL;
}

extern int array_exist(const char* key);

//SET
int array_set(const char* key, const char* value){
    if(key == NULL || value == NULL || _array_count == ARRAY_ITEM_SIZE - 1)
        return -1;
    if(array_exist(key)){
        return -1;
    }

    char* copy = (char*)mpmalloc(strlen(key)+1);  //可以改为内存池
    if(copy == NULL){
        return -1;
    }
    strncpy(copy, key, strlen(key)+1);

    char* vcopy = (char*)mpmalloc(strlen(value)+1);  //可以改为内存池
    if(vcopy == NULL){
        mpfree(vcopy);
        return -1;
    }
    strncpy(vcopy, value, strlen(value)+1);

    // size_t i = 0;
    // for (i; i <= _array_count; i++)
    // {
    //     if(array_items[i].key == NULL && array_items[i].value == NULL){
    //         break;
    //     }
    // }

    
    // printf("_array_count = %d,  i = %d\n", _array_count, i);
#if ENABLE_MEM_POLL
    array_item_t* item = memp_alloc(&mem1);
    item->key = copy;
    item->value = vcopy;
    item->next = mem1.free_ptr;
#else
    array_items[i].key = copy;
    array_items[i].value = vcopy;
#endif

    pthread_mutex_lock(&array_lock);
    _array_count++;
    pthread_mutex_unlock(&array_lock);
    return 0;
}

//GET
char* array_get(const char* key){
    array_item_t* t = search_item(key);
    if(t){
        return t->value;
    }
    return NULL;
}

//COUNT
int array_count(){
    return _array_count;
}

//DELETE
int array_delete(const char* key){
    if(key == NULL){
        return -1;
    }
    
    array_item_t* item = search_item(key);
    if(item == NULL){
        return -1;
    }

    if(item->key){
        mpfree(item->key);
        item->key = NULL;
    }
    
    if(item->value){
        mpfree(item->value);
        item->value = NULL;
    }
    pthread_mutex_lock(&array_lock);
    _array_count--;
    pthread_mutex_unlock(&array_lock);
#if ENABLE_MEM_POLL
    memp_free(&mem1, item);
#endif
    return 0;
}

//EXIST
int array_exist(const char* key){

    #if ENABLE_MEM_POLL
        array_item_t* item = (array_item_t*)mem1.mem;
        while(item->key != NULL){
            if(strcmp(item->key, key) == 0){
                return 1;
            }

            item = item->next;
        }

        return 0;
    #else
        return search_item(key) == NULL ? 0 : 1;
    #endif
    
}

#define ARRAY_PERSISTENCE_FILE "./PersistenceArrayData"

int array_persistence(mempool_t* mem){
    //持久化到文件
    FILE* file = fopen(ARRAY_PERSISTENCE_FILE, "wb");
    if(file == NULL){
        return -1;
    }

    //向文件写入字节
    array_item_t* item = (array_item_t*)mem->mem;
    while(item->key != NULL){
        //存入item中key和value长度和数据
        size_t len = strlen(item->key);
        fwrite(&len, sizeof(size_t), 1, file);
        fwrite(item->key, sizeof(char), len, file);
        len = strlen(item->value);
        fwrite(&len, sizeof(size_t), 1, file);
        fwrite(item->value, sizeof(char), len, file);

        item = (array_item_t*)item->next;
    }
    fflush(file);
    fclose(file);

    return 0;
}

void array_unpersistence(mempool_t* mem){
    //持久化文件构造
    FILE* file = fopen(ARRAY_PERSISTENCE_FILE, "rb");
    if(file == NULL){
        return -1;
    }

    int ret = 0;
    pthread_mutex_lock(&array_lock);
    while(mem->free_count > 0){
        if(feof(file) != 0){
            close(file);
            break;
        }

        //读取key
        size_t len = 0;
        fread(&len, sizeof(size_t), 1, file);
        char* keybuffer = malloc(sizeof(char) * 128);
        memset(keybuffer, 0, 128);
        fread(keybuffer, sizeof(char), len, file);
        //读取value
        len = 0;
        fread(&len, sizeof(size_t), 1, file);
        char* valbuffer = malloc(sizeof(char) * 128);
        memset(valbuffer, 0, 128);
        fread(valbuffer, sizeof(char), len, file);

        if(keybuffer[0] != '\0'){
            array_item_t* memitem = memp_alloc(mem);
            memitem->key = keybuffer;
            memitem->value = valbuffer;
        }
    }
    pthread_mutex_unlock(&array_lock);
}
#endif