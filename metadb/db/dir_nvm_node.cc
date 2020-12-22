/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-21 20:18:14
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#include "dir_nvm_node.h"
#include "nvm_node_allocator.h"

namespace metadb {

LinkNode *AllocLinkNode(){
    LinkNode *node = static_cast<LinkNode *>(node_allocator->AllocateAndInit(DIR_LINK_NODE_SIZE, 0));
    node->SetTypeNodrain(DirNodeType::LINKNODE_TYPE);
    return node;
}

struct LinkNodeSearchResult {
    bool key_find;
    uint32_t key_index;
    uint32_t key_offset;
    uint32_t key_num;
    uint32_t key_len;
    bool value_is_bptree;  //value 在bptree中;
    bool fname_find;
    uint32_t fname_index;
    uint32_t fname_offset;
    uint32_t value_len;

    LinkNodeSearchResult() : key_find(false), key_index(0), key_offset(0), key_num(0), key_len(0),
        value_is_bptree(false), fname_find(false), fname_index(0), fname_offset(0), value_len(0) {}
    ~LinkNodeSearchResult() {}
}



int LinkNodeSearch(LinkNodeSearchResult &res, LinkNode *cur, const inode_id_t key, const Slice &fname){
    if(compare_inode_id(key, cur->min_key) < 0 || compare_inode_id(key, cur->max_key) > 0) return 1;
    inode_id_t temp_key;
    uint32_t key_num, key_len;
    uint32_t offset = 0;
    uint32_t i = 0;
    for(; i < cur->num; i++){
        cur->DecodeBufGetKeyNumLen(offset, temp_key, key_num, key_len);
        if(compare_inode_id(key, temp_key) == 0) {  //找到key
            res.key_find = true;
            res.key_index = i;
            res.key_offset = offset;
            res.key_num = key_num;
            res.key_len = key_len;
            if(key_num == 0){ //value 是bptree
                res.value_is_bptree = true;
            } else {
                uint64_t hash_fname = MurmurHash64(fname.data(), fname.size());
                uint64_t get_hash_fname;
                uint32_t value_len;
                uint32_t value_offset = offset + sizeof(inode_id_t) + 4 + 4;
                uint32_t j = 0;
                for(; j < key_num; j++) {
                    cur->DecodeBufGetHashfnameAndLen(value_offset, get_hash_fname, value_len);
                    if(get_hash_fname == hash_fname){  //找到fname
                        res.fname_find = true;
                        res.fname_index = j;
                        res.fname_offset = value_offset;
                        res.value_len = value_len;

                        return 0;
                    }
                    if(get_hash_fname > hash_fname) {
                        res.fname_index = j;
                        res.fname_offset = value_offset;
                        res.value_len = value_len;
                        return 0;
                    }
                    value_offset += (8 + 4 + value_len);
                }
                res.fname_index = j;
                res.fname_offset = value_offset;
                
            }

            return 0;
        }

        if(compare_inode_id(key, temp_key) < 0) {  //未找到
            res.key_index = i;
            res.key_offset = offset;
            return 1；
        }

        offset += (sizeof(inode_id_t) + 4 + 4 + key_len);
    }
    res.key_index = i;
    res.key_offset = offset;
    return 1;
}

int LinkNodeInsert(LinkListOp &op, LinkNode *cur, const inode_id_t key, const Slice &fname, const inode_id_t value){
    LinkNodeSearchResult res;
    LinkNodeSearch(res, cur, key, fname);
    if(res.key_find){
        if(res.value_is_bptree) { //bptree 插入
            pointer_t bptree = cur->DecodeBufGetBptree(res.key_offset + 8 + 4);
            //bptree 插入
            return 0;
        }
        if(res.fname_find) { //node直接修改
            assert(fname.size() == (res.value_len - sizeof(inode_id_t)));  //以防hash值不一样
            uint32_t offset = res.fname_offset + 8 + 4 + (res.value_len - sizeof(inode_id_t));
            cur->SetBufPersist(offset, &value, sizeof(inode_id_t));
            return 0;
        }
        //
        uint32_t add_len = 8 + 4 + fname.size() + sizeof(inode_id_t);
        if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
            LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
            node_allocator->nvm_memmove_nodrain(new_node, cur, DIR_LINK_NODE_SIZE);
            uint32_t insert_offset = res.fname_offset;
            uint32_t need_move = cur->len - insert_offset;
            if(need_move > 0) {
                new_node->SetBufNodrain(insert_offset + add_len, cur->buf + insert_offset, need_move);
            } 
            uint64_t hash_fname = MurmurHash64(fname.data(), fname.size());
            uint32_t value_len = fname.size() + sizeof(inode_id_t);
            new_node->SetBufNodrain(insert_offset, &hash_fname, 8);
            insert_offset += 8;
            new_node->SetBufNodrain(insert_offset, &value_len, 4);
            insert_offset += 4;
            new_node->SetBufNodrain(insert_offset, fname.data(), fname.size());
            insert_offset += fname.size();
            new_node->SetBufNodrain(insert_offset, &value, sizeof(inode_id_t));
            uint32_t new_key_num = res.key_num + 1;
            uint32_t new_key_len = res.key_len + add_len;
            insert_offset = res.key_offset + 8;
            new_node->SetBufNodrain(insert_offset, &new_key_num, 4);
            insert_offset += 4;
            new_node->SetBufNodrain(insert_offset, &new_key_len, 4);
            new_node->Flush();

            pointer_t next = new_node->next;
            if(!IS_INVALID_POINTER(next)){
                LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(next));
                next_node->SetPrevPersist(NODE_GET_OFFSET(new_node));
            }
            pointer_t prev = new_node->prev;
            if(!IS_INVALID_POINTER(prev)){
                LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(prev));
                prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
            }
            op.free_list.push_back(NODE_GET_OFFSET(cur));
            op.delete_node_num++;
            op.add_node_num++;
            return 0;
        } else {
            
        }
    }
}

int LinkListInsert(LinkListOp &op, const inode_id_t key, const Slice &fname, const inode_id_t value){
    LinkNode *head = static_cast<LinkNode *>(NODE_GET_POINTER(op.root));
    pointer_t cur = op.root;
    pointer_t next = cur->next;
    pointer_t prev = INVALID_POINTER;

    LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->max_key) > 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    if (IS_INVALID_POINTER(cur)) {  //key最大

    }
}



} // namespace name