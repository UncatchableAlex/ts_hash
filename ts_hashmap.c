#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap. 
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {
  ts_hashmap_t *map = (ts_hashmap_t*) malloc(sizeof(ts_hashmap_t));
  ts_entry_t **table = (ts_entry_t**) calloc(capacity, sizeof(ts_entry_t*));
  map->table = table;
  map->capacity = capacity;
  map->size = 0;
  map->numOps = 0;
  map->numThreads = 0;
  map->bucketLocks = malloc(sizeof(pthread_mutex_t*)*capacity);
  map->sizeLock = malloc(sizeof(pthread_mutex_t));
  map->globalLock = malloc(sizeof(pthread_mutex_t));
  map->numThreadsLock = malloc(sizeof(pthread_mutex_t));
  // Initialize each mutex lock
  for (int i = 0; i < capacity; i++) {
      map->bucketLocks[i] = malloc(sizeof(pthread_mutex_t));
      pthread_mutex_init(map->bucketLocks[i], NULL);
  }
  pthread_mutex_init(map->sizeLock, NULL);
  pthread_mutex_init(map->globalLock, NULL);
  pthread_mutex_init(map->numThreadsLock, NULL);
  return map;
}


void acquireBucketAccess(ts_hashmap_t *map, int bucketIdx) {
  // acquire and release the global lock. If a rehash is taking place, the thread will stall on this step and NOT enter the critical section.
  // FURTHERMORE!!!! this lock MUST BE ACQUIRED FIRST. In the event of a rehash, the bucketlocks will be REPLACED. Acquiring a bucketLock 
  // before a rehash is USELESS
  pthread_mutex_lock(map->globalLock);
  pthread_mutex_t *bucketLock = (map->bucketLocks)[bucketIdx];
  // acquire the bucketLock first
  pthread_mutex_lock(bucketLock);
  (map->numOps)++;
  // increment the number of threads in critical sections:
  pthread_mutex_lock(map->numThreadsLock);
  (map->numThreads)++;
  pthread_mutex_unlock(map->numThreadsLock);
  // FINALLY, we can release the global lock
  pthread_mutex_unlock(map->globalLock);
}

void releaseBucketAccess(ts_hashmap_t *map, int bucketIdx) {
  pthread_mutex_t *bucketLock = (map->bucketLocks)[bucketIdx];
  // release the bucket lock and decrement the number of threads in critical sections
  pthread_mutex_unlock(bucketLock);
  pthread_mutex_lock(map->numThreadsLock);
  (map->numThreads)--;
  pthread_mutex_unlock(map->numThreadsLock);
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  int bucketIdx = ((unsigned int) key) % (map->capacity);
  acquireBucketAccess(map, bucketIdx);
  // get the head of the bucket that we think the entry is in:
  ts_entry_t *currEntry = (map->table)[bucketIdx];
  // iterate through the bucket until we find an entry with a matching key, or reach the end of the bucket
  while (currEntry != NULL) {
    // return the corresponding value if we find it
    if (currEntry->key == key) {
      int temp = currEntry->value;
      releaseBucketAccess(map, bucketIdx);
      return temp;
    }
    // get the next entry in the bucket
    currEntry = currEntry->next;
  }
  // we couldn't find any entries with a matching key. return INT_MAX
  releaseBucketAccess(map, bucketIdx);
  return INT_MAX;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  int bucketIdx = ((unsigned int) key) % (map->capacity);
  acquireBucketAccess(map, bucketIdx);
  // get the head of the bucket that we think the entry is in:
  ts_entry_t *currEntry = (map->table)[bucketIdx];
  // iterate through the bucket until we find an entry with a matching key, or reach the end of the bucket
  while (currEntry != NULL) {
    // return the corresponding value if we find it
    if (currEntry->key == key) {
      int temp = currEntry->value;
      currEntry->value = value;
      releaseBucketAccess(map, bucketIdx);
      return temp;
    }
    // get the next entry in the bucket
    currEntry = currEntry->next;
  }
  // The key wasn't in the bucket, so we will make a new entry:
  ts_entry_t *old_bucket_head = (map->table)[bucketIdx];
  // make a new entry for the new head of this bucket:
  ts_entry_t *new_bucket_head = malloc(sizeof(ts_entry_t));
  // fill the entry
  new_bucket_head->key = key;
  new_bucket_head->value = value;
  // set the next value as the old head:
  new_bucket_head->next = old_bucket_head;
  // make the table point to this entry as the head:
  (map->table)[bucketIdx] = new_bucket_head;
  // unlock the bucket and increment the size and num ops
  releaseBucketAccess(map, bucketIdx);
  atomic_mutate_size(map, 1);
  // check to see if we have to rehash
  pthread_mutex_lock(map->globalLock);
  double loadFactor = ((double)map->size)/((double)map->capacity);
  if (loadFactor >= MAX_LOAD_FACTOR) {
   // rehash(map);
  } 
  pthread_mutex_unlock(map->globalLock);
  return INT_MAX;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  int bucketIdx = ((unsigned int) key) % (map->capacity);
  acquireBucketAccess(map, bucketIdx);
  // get the head of the bucket that we think the entry is in:
  ts_entry_t *currEntry = (map->table)[((unsigned int) key) % (map->capacity)];
  // If the bucket is empty, we don't have to do anything. Just return inf
  if (currEntry == NULL) {
    releaseBucketAccess(map, bucketIdx);
    return INT_MAX;
  }
  // if the first entry is the one we want to delete, just delete it and we're done
  if (currEntry->key == key) {
    // set the new head:
    (map->table)[((unsigned int)key) % (map->capacity)] = currEntry->next;
    int temp = currEntry->value;
    free(currEntry);
    currEntry = NULL;
    releaseBucketAccess(map, bucketIdx);
    atomic_mutate_size(map, 0);
    return temp;
  }
  // we now know that the first entry isn't the one that we want to delete, so we will iterate through the rest:
  ts_entry_t *prevEntry = currEntry;
  currEntry = currEntry->next;
  // iterate through the bucket until we find an entry with a matching key, or reach the end of the bucket
  while (currEntry != NULL) {
    // return the corresponding value if we find it and delete its entry:
    if (currEntry->key == key) {
      int temp = currEntry->value;
      // cut currEntry entry out of the bucket
      prevEntry->next = currEntry->next;
      free(currEntry);
      currEntry = NULL;
      releaseBucketAccess(map, bucketIdx);
      atomic_mutate_size(map, 0);
      return temp;
    }
    // get the next entry in the bucket
    prevEntry = currEntry;
    currEntry = currEntry->next;
  }
  // if we couldn't find any entries with the target key, then return inf:
  releaseBucketAccess(map, bucketIdx);
  return INT_MAX;
}


/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
  // iterate through each list, free up all nodes
  for (int i = 0; i < map->capacity; i++) {
    ts_entry_t *currEntry = (map->table)[i];
    // free all the nodes in the bucket
    while (currEntry != NULL) {
      ts_entry_t *nextEntry = currEntry->next;
      free(currEntry);
      currEntry = nextEntry;
    }
    pthread_mutex_destroy(map->bucketLocks[i]);
    free(map->bucketLocks[i]);
  }
  pthread_mutex_destroy(map->globalLock);
  pthread_mutex_destroy(map->sizeLock);
  pthread_mutex_destroy(map->numThreadsLock);
  free(map->table);
  free(map->bucketLocks);
  free(map->sizeLock);
  free(map->globalLock);
  free(map->numThreadsLock);
  // free the map itself:
  free(map);
}

/**
 * Increment the size field in the given hashmap atomically (either increment or decrement)
 * @param map a pointer to the map
*/
void atomic_mutate_size(ts_hashmap_t *map, int isInc) {
    //increment the number of operations performed:
    pthread_mutex_lock(map->sizeLock);
    (map->size) = isInc ? (map->size) + 1 : (map->size) - 1;
    pthread_mutex_unlock(map->sizeLock);
}

/**
 * Performs a rehash of the contents of the map.
 * @param map the map to rehash
*/
void rehash(ts_hashmap_t *map) {
  // spin-wait until all the exiting threads have left critical sections:
  printf("trying to rehash. critical threads %d\n", map->numThreads);
  while (map->numThreads);
  printf("attempting rehash. critical threads %d\n", map->numThreads);
  int newCapacity = map->capacity * 2;
  ts_entry_t **newTable = (ts_entry_t**) calloc(newCapacity, sizeof(ts_entry_t*));
  pthread_mutex_t **newBucketLocks = malloc(sizeof(pthread_mutex_t*)*newCapacity);
  // Initialize each new bucketLock
  for (int i = 0; i < newCapacity; i++) {
      newBucketLocks[i] = malloc(sizeof(pthread_mutex_t));
      pthread_mutex_init(newBucketLocks[i], NULL);
  }  
  // iterate through each bucket
  for (int i = 0; i < map->capacity; i++) {
    ts_entry_t *curr = (map->table)[i];
    while (curr) {
      ts_entry_t *nextCurr = curr->next;
      // get the new bucket index:
      int newBucketIdx = ((unsigned int) curr->key) % newCapacity;
      // set the next value as the old head of the new bucket:
      curr->next = newTable[newBucketIdx];
      // make the table point to this entry as the new head:
      newTable[newBucketIdx] = curr;
      // move on the the next entry in this bucket
      curr = nextCurr;
    }
  }
  // iterate through each bucket, free up all nodes
  for (int i = 0; i < map->capacity; i++) {
    ts_entry_t *currEntry = (map->table)[i];
    free(currEntry);
    pthread_mutex_destroy(map->bucketLocks[i]);
    free(map->bucketLocks[i]);
  }
  free(map->bucketLocks);
  free(map->table);
  map->capacity = newCapacity;
  map->bucketLocks = newBucketLocks;
  map->table = newTable;
}