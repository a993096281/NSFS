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
    if(compare_inode_id(key, cur->min_key) < 0) {
        res.key_index = 0;
        res.key_offset = 0;
        return 1;
    }
    if(compare_inode_id(key, cur->max_key) > 0) {
        res.key_index = cur->num;
        res.key_offset = cur->len;
        return 1;
    }

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

void MemoryEncodeKey(char *buf, inode_id_t key){
    memcpy(buf, &key, sizeof(inode_id_t));
}

void MemoryEncodeKVnumKVlen(char *buf, uint32_t key_num, uint32_t key_len){
    uint32_t insert_offset = 0;
    memcpy(buf + insert_offset, &key_num, 4);
    insert_offset += 4;
    memcpy(buf + insert_offset, &key_len, 4);
}

void MemoryEncodeFnameKey(char *buf, const Slice &fname, const inode_id_t value){
    uint64_t hash_fname = MurmurHash64(fname.data(), fname.size());
    uint32_t value_len = fname.size() + sizeof(inode_id_t);
    uint32_t insert_offset = 0;
    memcpy(buf + insert_offset, &hash_fname, 8);
    insert_offset += 8;
    memcpy(buf + insert_offset, &value_len, 4);
    insert_offset += 4;
    memcpy(buf + insert_offset, fname.data(), fname.size());
    insert_offset += fname.size();
    memcpy(buf + insert_offset, &value, sizeof(inode_id_t));   
}

void MemoryDecodeGetKeyNumLen(const char *buf, inode_id_t &key, uint32_t &key_num, uint32_t &key_len){
    key = *static_cast<inode_id_t *>(buf);
    key_num = *static_cast<uint32_t *>(buf + sizeof(inode_id_t));
    key_len = *static_cast<uint32_t *>(buf + sizeof(inode_id_t) + 4);
}

int LinkNodeSplit(const char *buf, uint32_t len, vector<pointer_t> &res){   //有可能分裂为三个节点
    uint32_t offset = 0;
    uint32_t last_node_offset = 0;
    uint32_t new_node_len = 0;
    uint16_t new_node_key_num = 0;
    inode_id_t key;
    uint32_t key_num, key_len;
    inode_id_t min_key, prev_key;
    pointer_t prev = INVALID_POINTER;
    uint32_t split_size = len / 2; //分为两半的size；
    while(offset < len) {
        MemoryDecodeGetKeyNumLen(buf + offset, key, key_num, key_len);
        if(new_node_len == 0) min_key = key;

        if((new_node_len + sizeof(inode_id_t) + 8 + key_len)  > LINK_NODE_CAPACITY){  //该节点不可再增加内容，前面的内容生成一个节点
            LinkNode *new_node = AllocLinkNode();
            new_node->SetNumAndLenNodrain(new_node_key_num, new_node_len);
            new_node->SetMinkeyNodrain(min_key);
            new_node->SetMaxkeyNodrain(prev_key);
            new_node->SetPrevNodrain(prev);
            new_node->SetBufNodrain(0, buf + last_node_offset, new_node_len);
            last_node_offset = offset;
            split_size = (len - last_node_offset) > LINK_NODE_CAPACITY ? (len - last_node_offset) / 2 : len - last_node_offset;  //剩下的长度大于一个节点，再次分割，否则直接剩下的内容组成一个节点
            new_node_key_num = 0;
            new_node_len = 0;
            min_key = key;
            prev = NODE_GET_OFFSET(new_node);
            res.push_back(prev);
            
        }
        offset += (sizeof(inode_id_t) + 8 + key_len);
        prev_key = key;
        new_node_len += (sizeof(inode_id_t) + 8 + key_len);
        new_node_key_num++;
        if(new_node_len >= split_size || offset == len) { //长度超过剩余内容一半，或者到达末尾，可生成一个节点
            LinkNode *new_node = AllocLinkNode();
            new_node->SetNumAndLenNodrain(new_node_key_num, new_node_len);
            new_node->SetMinkeyNodrain(min_key);
            new_node->SetMaxkeyNodrain(key);
            new_node->SetPrevNodrain(prev);
            new_node->SetBufNodrain(0, buf + last_node_offset, new_node_len);
            last_node_offset = offset;
            split_size = (len - last_node_offset) > LINK_NODE_CAPACITY ? (len - last_node_offset) / 2 : len - last_node_offset;  //剩下的长度大于一个节点，再次分割，否则直接剩下的内容组成一个节点
            new_node_key_num = 0;
            new_node_len = 0;
            prev = NODE_GET_OFFSET(new_node);
            res.push_back(prev);
        }
    }
    assert(offset == len);

    for(uint32_t i = 0; i < res.size() - 1; i++){  //设置next指针
        LinkNode *cur = static_cast<LinkNode *>(NODE_GET_POINTER(res[i]));
        cur->SetNextNodrain(res[i+1]);
    }
    return 0;

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
            op.res = NODE_GET_OFFSET(cur);
            return 0;
        }
        //
        uint32_t add_len = 8 + 4 + fname.size() + sizeof(inode_id_t);
        if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
            LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
            new_node->CopyBy(cur);
            uint32_t insert_offset = res.fname_offset;
            uint32_t need_move = cur->len - insert_offset;
            if(need_move > 0) {
                new_node->SetBufNodrain(insert_offset + add_len, cur->buf + insert_offset, need_move);
            }
            char *buf = new char[add_len];
            MemoryEncodeFnameKey(buf, fname, value);
            new_node->SetBufNodrain(insert_offset, buf, add_len); 
            
            insert_offset = res.key_offset + 8;
            MemoryEncodeKVnumKVlen(buf, res.key_num + 1, res.key_len + add_len);
            new_node->SetBufNodrain(insert_offset, buf, 8);
            
            new_node->SetNumAndLenNodrain(cur->num, cur->len + add_len);
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

            op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
            op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
            op.res = NODE_GET_OFFSET(new_node);

            delete[] buf;
            return 0;
        } else {
            if((8 + 4 + 4 + res.key_len + add_len) > LINK_NODE_CAPACITY){  //同pinode_id聚齐的kv大于一个LinkNode Size，转成B+tree;

                return 0;
            }

            //普通分裂,再插入
            uint32_t new_beyond_buf_size = cur->len + add_len;
            char *buf = new char[new_beyond_buf_size];
            memcpy(buf, cur->buf, cur->len);
            uint32_t insert_offset = res.fname_offset;
            uint32_t need_move = cur->len - insert_offset;
            if(need_move > 0) {
                memmove(buf + add_len, buf + insert_offset, need_move);
            } 
            MemoryEncodeFnameKey(buf + insert_offset, fname, value);
            insert_offset = res.key_offset + 8;
            MemoryEncodeKVnumKVlen(buf + insert_offset, res.key_num + 1, res.key_len + add_len);

            vector<pointer_t> res;
            res.reserve(3);
            LinkNodeSplit(buf, new_beyond_buf_size, res);
            uint32_t res_size = res.size();

            if(!IS_INVALID_POINTER(cur->next)){
                LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[res_size - 1]));
                new_node->SetNextNodrain(cur->next);
            }
            if(!IS_INVALID_POINTER(cur->prev)){
                LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[0]));
                new_node->SetPrevNodrain(cur->prev);
            }

            for(auto it : res){
                op.add_linknode_list.push_back(it);
                LinkNode *temp = static_cast<LinkNode *>(NODE_GET_POINTER(it));
                temp->Flush();
            }

            if(!IS_INVALID_POINTER(cur->next)){
                LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
                next_node->SetPrevPersist(NODE_GET_OFFSET(res[res_size - 1]));
            }
            if(!IS_INVALID_POINTER(cur->prev)){
                LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
                prev_node->SetNextPersist(NODE_GET_OFFSET(res[0]));
            }
            
            op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
            op.res = NODE_GET_OFFSET(res[0]);

            delete[] buf;
            return 0;
        }
    }

    uint32_t add_len = 8 + 4 + 4 + 8 + 4 + fname.size() + sizeof(inode_id_t);
    if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
        LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
        new_node->CopyBy(cur);
        uint32_t insert_offset = res.key_offset;
        uint32_t need_move = cur->len - insert_offset;
        if(need_move > 0) {
            new_node->SetBufNodrain(insert_offset + add_len, cur->buf + insert_offset, need_move);
        }
        char *buf = new char[add_len];
        MemoryEncodeKey(buf + insert_offset, key);
        insert_offset += sizeof(inode_id_t);
        MemoryEncodeKVnumKVlen(buf + insert_offset, 1, 8 + 4 + fname.size() + sizeof(inode_id_t));
        insert_offset += 8;
        MemoryEncodeFnameKey(buf + insert_offset, fname, value);
        new_node->SetBufNodrain(res.key_offset, buf, add_len); 

        if(res.key_index == 0){ //插入的是最小值
            new_node->SetMinkeyNodrain(key);
        }
        if(res.key_index == cur->num){ //插入的是最大值
            new_node->SetMaxkeyNodrain(key);
        }
        
        new_node->SetNumAndLenNodrain(cur->num + 1, cur->len + add_len);
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

        op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
        op.res = NODE_GET_OFFSET(new_node);

        delete[] buf;
        return 0;
    } else {
        if((add_len) > LINK_NODE_CAPACITY){  //单独一个KV大于，LinkNode Size，转成B+tree;
            assert(0); //文件名太长，超过MAX_FNAME_LEN
            return 0;
        }

        //普通分裂,再插入
        uint32_t new_beyond_buf_size = cur->len + add_len;
        char *buf = new char[new_beyond_buf_size];
        memcpy(buf, cur->buf, cur->len);
        uint32_t insert_offset = res.key_offset;
        uint32_t need_move = cur->len - insert_offset;
        if(need_move > 0) {
            memmove(buf + add_len, buf + insert_offset, need_move);
        } 
        MemoryEncodeKey(buf + insert_offset, key);
        insert_offset += sizeof(inode_id_t);
        MemoryEncodeKVnumKVlen(buf + insert_offset, 1, 8 + 4 + fname.size() + sizeof(inode_id_t));
        insert_offset += 8;
        MemoryEncodeFnameKey(buf + insert_offset, fname, value);

        vector<pointer_t> res;
        res.reserve(3);
        LinkNodeSplit(buf, new_beyond_buf_size, res);
        uint32_t res_size = res.size();

        if(!IS_INVALID_POINTER(cur->next)){
            LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[res_size - 1]));
            new_node->SetNextNodrain(cur->next);
        }
        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[0]));
            new_node->SetPrevNodrain(cur->prev);
        }

        for(auto it : res){
            op.add_linknode_list.push_back(it);
            LinkNode *temp = static_cast<LinkNode *>(NODE_GET_POINTER(it));
            temp->Flush();
        }

        if(!IS_INVALID_POINTER(cur->next)){
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            next_node->SetPrevPersist(NODE_GET_OFFSET(res[res_size - 1]));
        }
        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            prev_node->SetNextPersist(NODE_GET_OFFSET(res[0]));
        }
        
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
        op.res = NODE_GET_OFFSET(res[0]);

        delete[] buf;
        return 0;
    }

    

}

int LinkListInsert(LinkListOp &op, const inode_id_t key, const Slice &fname, const inode_id_t value){
    pointer_t head = op.root;
    pointer_t cur = op.root;
    pointer_t prev = op.root;

    LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t insert = prev;  //在该节点插入kv；
    LinkNode *insert_node = static_cast<LinkNode *>(NODE_GET_POINTER(insert));
    LinkNodeInsert(op, insert_node, key, fname, value);
    if(insert != head){  //插入的不是头节点，返回的op.res需要为头节点
        op.res = head;
    }

    //插入的是头节点，返回的op.res就是头结点；
    return 0;
}


int LinkListGet(LinkNode *root, const inode_id_t key, const Slice &fname, inode_id_t &value){
    pointer_t cur = NODE_GET_OFFSET(root);
    pointer_t prev = cur;

    LinkNode *cur_node = root;
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t search = prev;  //在该节点查找kv；
    LinkNode *search_node = static_cast<LinkNode *>(NODE_GET_POINTER(search));

    LinkNodeSearchResult res;
    LinkNodeSearch(res, search_node, key, fname);
    if(res.key_find){
        if(res.value_is_bptree){  //bptree中查找

        } else if (res.fname_find) {
            uint32_t get_offset = res.fname_offset + 8 + 4 + fname.size();
            value = *static_cast<inode_id_t *>(search_node->buf + get_offset);
            return 0;
        } else {
            return 2;
        }

    }
    return 2;
}

inode_id_t LinkNodeFindMaxKey(LinkNode *cur){
    inode_id_t temp_key = INVALID_INODE_ID_KEY;
    uint32_t key_num, key_len;
    uint32_t offset = 0;
    uint32_t i = 0;
    for(; i < cur->num; i++){
        cur->DecodeBufGetKeyNumLen(offset, temp_key, key_num, key_len);
        offset += (sizeof(inode_id_t) + 4 + 4 + key_len);
    }
    return temp_key;
}

int LinkNodeDelete(LinkListOp &op, LinkNode *cur, const inode_id_t key, const Slice &fname){
    LinkNodeSearchResult res;
    LinkNodeSearch(res, cur, key, fname);
    if(!res.key_find) return 1;
    if(res.value_is_bptree){  //bptree中删除
        return 0;
    }
    if(!res.fname_find) return 1;
    uint32_t del_len = (res.key_num == 1) ? (sizeof(inode_id_t) + 8 + 8 + 4 + res.key_len) : (8 + 4 + res.key_len);
    uint32_t remain_len = cur->len - del_len;
    if(remain_len != 0 && remain_len < LINK_NODE_TRIG_MERGE_SIZE ){ //可能需要合并
        if(!IS_INVALID_POINTER(cur->next)){  
            LinkNode *next_node = static_cast<LinkNode *>(cur->next);
            if(next_node->GetFreeSpace() >= remain_len){ //和后节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                uint32_t del_offset = (res.key_num == 1) ? res.key_offset : res.fname_offset;
                uint32_t need_move = cur->len - (del_offset + del_len);
                if(need_move > 0) {
                    new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
                }
                if(res.key_num > 1){  //修改key_num,key_len,
                    char buf[8];
                    MemoryEncodeKVnumKVlen(buf, res.key_num - 1, res.key_len - del_len);
                    new_node->SetBufNodrain(res.key_offset + 8, buf, 8);
                }
                uint16_t num = (res.key_num == 1) ? (cur->num - 1) : cur->num;
                new_node->SetNumAndLenNodrain(num, cur->len - del_len);
                if(res.key_num == 1 && res.key_index == 0){
                     //删除的是最小值
                    inode_id_t min_key;
                    new_node->DecodeBufGetKey(0, min_key);
                    new_node->SetMinkeyNodrain(min_key);
                    
                }
                new_node->SetBufNodrain(new_node->len, next_node->buf, next_node->len);
                new_node->SetNumAndLenNodrain(new_node->num + next_node->num, new_node->len + next_node->len);
                new_node->SetMaxkeyNodrain(next_node->max_key);
                new_node->SetNextNodrain(next_node->next);
                new_node->Flush();

                if(!IS_INVALID_POINTER(new_node->next)){
                    LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(new_node->next));
                    next_node->SetPrevPersist(NODE_GET_OFFSET(new_node));
                }
                    
                if(!IS_INVALID_POINTER(new_node->prev)){
                    LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(new_node->prev));
                    prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
                } else {
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(next_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
                
            }
        }

        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(cur->prev);
            if(prev_node->GetFreeSpace() >= remain_len){ //和前节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                uint32_t del_offset = (res.key_num == 1) ? res.key_offset : res.fname_offset;
                uint32_t need_move = cur->len - (del_offset + del_len);
                if(need_move > 0) {
                    new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
                }
                if(res.key_num > 1){  //修改key_num,key_len,
                    char buf[8];
                    MemoryEncodeKVnumKVlen(buf, res.key_num - 1, res.key_len - del_len);
                    new_node->SetBufNodrain(res.key_offset + 8, buf, 8);
                }
                uint16_t num = (res.key_num == 1) ? (cur->num - 1) : cur->num;
                new_node->SetNumAndLenNodrain(num, cur->len - del_len);
                if(res.key_num == 1 && res.key_index == (cur->num - 1)){  //删除的是最大值
                    inode_id_t max_key = LinkNodeFindMaxKey(new_node);
                    new_node->SetMaxkeyNodrain(max_key);
                }

                new_node->SetBufNodrain(prev_node->len, new_node->buf, new_node->len);
                new_node->SetBufNodrain(0, prev_node->buf, prev_node->len);
                new_node->SetNumAndLenNodrain(new_node->num + prev_node->num, new_node->len + prev_node->len);
                new_node->SetMinkeyNodrain(prev_node->min_key);
                new_node->SetPrevNodrain(prev_node->prev);
                new_node->Flush();

                if(!IS_INVALID_POINTER(new_node->next)){
                    LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(new_node->next));
                    next_node->SetPrevPersist(NODE_GET_OFFSET(new_node));
                }
                    
                if(!IS_INVALID_POINTER(new_node->prev)){
                    LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(new_node->prev));
                    prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
                } else {
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(prev_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
            }
        }

        //前后节点都不适合合并，保持单个节点；
    }

    //剩下内容组成单个节点
    if(remain_len == 0){  //直接删除节点；
        if(!IS_INVALID_POINTER(cur->prev)){  //前结点存在
            LinkNode *prev_node = static_cast<LinkNode *>(cur->prev);
            prev_node->SetNextPersist(cur->next);
        } else {
            op.res = cur->next;  //头结点替换
        }

        if(!IS_INVALID_POINTER(cur->next)){  //后节点存在
            LinkNode *next_node = static_cast<LinkNode *>(cur->next);
            next_node->SetPrevPersist(cur->prev);
        }
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
        return 0;
    }

    LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
    new_node->CopyBy(cur);
    uint32_t del_offset = (res.key_num == 1) ? res.key_offset : res.fname_offset;
    uint32_t need_move = cur->len - (del_offset + del_len);
    if(need_move > 0) {
        new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
    }
    if(res.key_num > 1){  //修改key_num,key_len,
        char buf[8];
        MemoryEncodeKVnumKVlen(buf, res.key_num - 1, res.key_len - del_len);
        new_node->SetBufNodrain(res.key_offset + 8, buf, 8);
    }
    uint16_t num = (res.key_num == 1) ? (cur->num - 1) : cur->num;
    new_node->SetNumAndLenNodrain(num, cur->len - del_len);
    if(res.key_num == 1){
        if(res.key_index == 0){ //删除的是最小值
            inode_id_t min_key;
            new_node->DecodeBufGetKey(0, min_key);
            new_node->SetMinkeyNodrain(min_key);
        }
        if(res.key_index == (cur->num - 1)){ //删除的是最大值
            inode_id_t max_key = LinkNodeFindMaxKey(new_node);
            new_node->SetMaxkeyNodrain(max_key);
        }
    }
    new_node->Flush();
    if(!IS_INVALID_POINTER(cur->next)){
        LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
        next_node->SetPrevPersist(NODE_GET_OFFSET(new_node));
    }
        
    if(!IS_INVALID_POINTER(cur->prev)){
        LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
        prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
    } else {
        op.res = NODE_GET_OFFSET(new_node);  //头结点替换
    }

    op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
    op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
    return 0;

}

int LinkListDelete(LinkListOp &op, const inode_id_t key, const Slice &fname){
    pointer_t head = op.root;
    pointer_t cur = op.root;
    pointer_t prev = op.root;

    LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t del = prev;  //在该节点插入kv；
    LinkNode *del_node = static_cast<LinkNode *>(NODE_GET_POINTER(del));
    return LinkNodeDelete(op, del_node, key, fname);
}

////// bptree
BptreeIndexNode *AllocBptreeIndexNode(){
    BptreeIndexNode *node = static_cast<BptreeIndexNode *>(node_allocator->AllocateAndInit(DIR_BPTREE_INDEX_NODE_SIZE, 0));
    node->SetTypeNodrain(DirNodeType::BPTREEINDEXNODE_TYPE);
    return node;
}

BptreeLeafNode *AllocBptreeLeafNode(){
    BptreeLeafNode *node = static_cast<BptreeLeafNode *>(node_allocator->AllocateAndInit(DIR_BPTREE_LEAF_NODE_SIZE, 0));
    node->SetTypeNodrain(DirNodeType::BPTREELEAFNODE_TYPE);
    return node;
}

struct BptreeIndexNodeSearch {  //中间节点查找的路径
    pointer_t node;
    uint32_t index;   //中间节点为index， 

    BptreeIndexNodeSearch() : node(INVALID_POINTER), index(0) {}
    ~BptreeIndexNodeSearch() {}
}

struct BptreeSearchResult {
    bool key_find;
    uint32_t index_level; //Bptree index搜索层数,0代表没有中间节点
    BptreeIndexNodeSearch path[MAX_DIR_BPTREE_LEVEL];  //中间层搜索路径
    pointer_t leaf_node;       //叶子节点查找结果
    uint32_t leaf_key_offset;  //叶子节点查找结果
    uint32_t leaf_value_len;   //叶子节点查找结果
    

    BptreeSearchResult() : key_find(false), index_level(0), leaf_node(INVALID_POINTER), leaf_key_offset(0), leaf_value_len(0) {}
    ~BptreeSearchResult() {}
}

bool IsIndexNode(pointer_t ptr){
    uint8_t type = *static_cast<uint8_t *>(NODE_GET_POINTER(ptr));
    return type == DirNodeType::BPTREEINDEXNODE_TYPE;
}


int BptreeSearch(BptreeSearchResult &res, pointer_t root, const uint64_t hash_key) {
    pointer_t cur = root;
    while(!IS_INVALID_POINTER(cur)){
        if(IsIndexNode(cur)){  //中间节点查找
            
        }
        else{    //叶子节点查找

        }
    }
}

int BptreeInsert(BptreeOp &op, const uint64_t hash_key, const Slice &fname, const inode_id_t value){
    BptreeSearchResult res;

}

int BptreeGet(pointer_t root, const uint64_t hash_key, const Slice &fname, inode_id_t &value);
int BptreeDelete(BptreeOp &op, const uint64_t hash_key, const Slice &fname);


//////

} // namespace name