/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_list.h is for what ...
 *
 * Version: ***: ob_sql_list.h  Sat Nov 24 19:06:29 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_LIST_H_
#define OB_SQL_LIST_H_

/**
 * 列表，参考kernel上的list.h
 */
#include "ob_sql_define.h"

OB_SQL_CPP_START

// from kernel list
typedef struct ob_sql_list_t ob_sql_list_t;

struct ob_sql_list_t {
    ob_sql_list_t *next, *prev;
};

#define OB_SQL_LIST_HEAD_INIT(name) {&(name), &(name)}
#define ob_sql_list_init(ptr) do {                \
        (ptr)->next = (ptr);                    \
        (ptr)->prev = (ptr);                    \
    } while (0)

static inline void __ob_sql_list_add(ob_sql_list_t *list,
                                   ob_sql_list_t *prev, ob_sql_list_t *next)
{
    next->prev = list;
    list->next = next;
    list->prev = prev;
    prev->next = list;
}
// list head to add it after
static inline void ob_sql_list_add_head(ob_sql_list_t *list, ob_sql_list_t *head)
{
    __ob_sql_list_add(list, head, head->next);
}
// list head to add it before
static inline void ob_sql_list_add_tail(ob_sql_list_t *list, ob_sql_list_t *head)
{
    __ob_sql_list_add(list, head->prev, head);
}
static inline void __ob_sql_list_del(ob_sql_list_t *prev, ob_sql_list_t *next)
{
    next->prev = prev;
    prev->next = next;
}
// deletes entry from list
static inline void ob_sql_list_del(ob_sql_list_t *entry)
{
    __ob_sql_list_del(entry->prev, entry->next);
    ob_sql_list_init(entry);
}
// tests whether a list is empty
static inline int ob_sql_list_empty(const ob_sql_list_t *head)
{
    return (head->next == head);
}
// move list to new_list
static inline void ob_sql_list_movelist(ob_sql_list_t *list, ob_sql_list_t *new_list)
{
    if (!ob_sql_list_empty(list)) {
        new_list->prev = list->prev;
        new_list->next = list->next;
        new_list->prev->next = new_list;
        new_list->next->prev = new_list;
        ob_sql_list_init(list);
    } else {
        ob_sql_list_init(new_list);
    }
}
// join list to head
static inline void ob_sql_list_join(ob_sql_list_t *list, ob_sql_list_t *head)
{
    if (!ob_sql_list_empty(list)) {
        ob_sql_list_t *first = list->next;
        ob_sql_list_t *last = list->prev;
        ob_sql_list_t *at = head->prev;

        first->prev = at;
        at->next = first;
        last->next = head;
        head->prev = last;
    }
}

// get last
#define ob_sql_list_get_last(list, type, member)                              \
    ob_sql_list_empty(list) ? NULL : ob_sql_list_entry((list)->prev, type, member)

// get first
#define ob_sql_list_get_first(list, type, member)                             \
    ob_sql_list_empty(list) ? NULL : ob_sql_list_entry((list)->next, type, member)

// caller should make sure list has more than n elements
#define ob_sql_list_get(pos, head, member, n)                      \
    for (pos = ob_sql_list_entry((head)->next, typeof(*pos), member);         \
            &pos->member != (head) && n-- > 0;                                         \
            pos = ob_sql_list_entry(pos->member.next, typeof(*pos), member))


#define ob_sql_list_entry(ptr, type, member) ({                               \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);                \
        (type *)( (char *)__mptr - offsetof(type,member) );})

#define ob_sql_list_for_each_entry(pos, head, member)                         \
    for (pos = ob_sql_list_entry((head)->next, typeof(*pos), member);         \
         &pos->member != (head);                                        \
         pos = ob_sql_list_entry(pos->member.next, typeof(*pos), member))

#define ob_sql_list_for_each_entry_safe(pos, n, head, member)                 \
    for (pos = ob_sql_list_entry((head)->next, typeof(*pos), member),         \
            n = ob_sql_list_entry(pos->member.next, typeof(*pos), member);    \
            &pos->member != (head);                                         \
            pos = n, n = ob_sql_list_entry(n->member.next, typeof(*n), member))

OB_SQL_CPP_END

#endif
