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
  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  // increment the number of operations performed:
  (map->numOps)++;
  // get the head of the bucket that we think the entry is in:
  ts_entry_t *currEntry = (map->table)[key % (map->capacity)];
  // iterate through the bucket until we find an entry with a matching key, or reach the end of the bucket
  while (currEntry != NULL) {
    // return the corresponding value if we find it
    if (currEntry->key == key) {
      return currEntry->value;
    }
    // get the next entry in the bucket
    currEntry = currEntry->next;
  }
  // we couldn't find any entries with a matching key. return INT_MAX
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
  // increment the number of operations performed:
  (map->numOps)++;  
  // get the head of the bucket that we think the entry is in:
  ts_entry_t *currEntry = (map->table)[key % (map->capacity)];
  // iterate through the bucket until we find an entry with a matching key, or reach the end of the bucket
  while (currEntry != NULL) {
    // return the corresponding value if we find it
    if (currEntry->key == key) {
      int temp = currEntry->value;
      currEntry->value = value;
      return temp;
    }
    // get the next entry in the bucket
    currEntry = currEntry->next;
  }
  ts_entry_t *old_bucket_head = (map->table)[key % (map->capacity)];
  // make a new entry for the new head of this bucket:
  ts_entry_t *new_bucket_head = malloc(sizeof(ts_entry_t));
  // fill the entry
  new_bucket_head->key = key;
  new_bucket_head->value = value;
  // set the next value as the old head:
  new_bucket_head->next = old_bucket_head;
  // make the table point to this entry as the head:
  (map->table)[key % (map->capacity)] = new_bucket_head;
  return INT_MAX;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  // increment the number of operations performed:
  (map->numOps)++;
  // get the head of the bucket that we think the entry is in:
  ts_entry_t *currEntry = (map->table)[key % (map->capacity)];
  // If the bucket is empty, we don't have to do anything. Just return inf
  if (currEntry == NULL) {
    return INT_MAX;
  }
  // if there is only one entry in the bucket and it's the one that we want to delete, just delete it and we're done
  if (currEntry->next == NULL && currEntry->key == key) {
    int temp = currEntry->value;
    free(currEntry);
    currEntry = NULL;
    (map->table)[key % (map->capacity)] = NULL;
    return temp;
  }
  // if there is only one entry in the bucket and it's not the one we want, just return inf
  if (currEntry->next == NULL && currEntry->key != key) {
    return INT_MAX;
  }
  // so, now we know that there are at least two entries in our bucket and the first one isn't the one that we are trying to delete:
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
      return temp;
    }
    // get the next entry in the bucket
    prevEntry = currEntry;
    currEntry = currEntry->next;
  }
  // if we couldn't find any entries with the target key, then return inf:
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
  }
  // TODO: free the hash table
  free(map->table);
  // TODO: destroy locks

  // free the map itself:
  free(map);
}