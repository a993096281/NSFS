/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-17 16:18:09
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_NVM_NODE_H_
#define _METADB_NVM_NODE_H_

#include <vector>
#include "format.h"
#include "metadb/slice.h"
#include "metadb/inode.h"

using namespace std;

namespace metadb {

static const uint32_t LINK_NODE_CAPACITY = DIR_LINK_NODE_SIZE - 40;
static const uint32_t INDEX_NODE_CAPACITY = (DIR_BPTREE_INDEX_NODE_SIZE - 32) / sizeof(struct IndexNodeEntry);
static const uint32_t LEAF_NODE_CAPACITY = DIR_BPTREE_LEAF_NODE_SIZE - 24;

enum class DirNodeType : uint8_t {
    UNKNOWN_TYPE = 0,
    LINKNODE_TYPE = 1,
    BPTREEINDEXNODE_TYPE,
    BPTREELEAFNODE_TYPE
};

struct LinkNode {
    uint8_t type;
    uint8_t padding;
    uint16_t num;
    uint32_t len; 
    inode_id_t min_key;
    inode_id_t max_key;
    pointer_t prev;    //头结点，prev为空，
    pointer_t next;    //尾结点，next为空
    char buf[LINK_NODE_CAPACITY];       //  key:fname1,fname2...key2:fname1,fname2: |----key(8B)---|---num(4B)---|----len(4B)----|---hash(fname1)(8B)---|--value_len(4B)--|--value()--|
                                        //                        |---hash(fname1)(8B)---|--value_len(4B)--|--value---|...
                                        //  key:Bptree:    |----key(8B)---|---num(4B) = 0 ---|---b+tree_pointer(8B)---|
                                        // num == 0 不可能的情况，所以代表指向一棵Bptree
    LinkNode() {}
    ~LinkNode() {}

    uint32_t GetFreeSpace() {
        return LINK_NODE_CAPACITY - len;
    }

    void DecodeBufGetKeyNumLen(uint32_t offset, inode_id_t &key, uint32_t &key_num, uint32_t &key_len){
        key = *static_cast<inode_id_t *>(buf + offset);
        key_num = *static_cast<uint32_t *>(buf + offset + sizeof(inode_id_t));
        key_len = *static_cast<uint32_t *>(buf + offset + sizeof(inode_id_t) + 4);
    }

    void DecodeBufGetHashfnameAndLen(uint32_t offset, uint64_t &hash_fname, uint32_t &value_len){
        hash_fname = *static_cast<uint64_t *>(buf + offset);
        value_len = *static_cast<uint32_t *>(buf + offset + 8);
    }

    pointer_t DecodeBufGetBptree(uint32_t offset) {
        return *static_cast<pointer_t *>(buf + offset);
    }

    void Flush(){
        node_allocator->nvm_persist(this, sizeof(LinkNode));
    }

    void SetTypeNodrain(DirNodeType t){
        node_allocator->nvm_memcpy_nodrain(&type, &t, 1);
    }
    void SetTypePersist(DirNodeType t){
        node_allocator->nvm_memcpy_persist(&type, &t, 1);
    }

    struct NumLen {
        uint16_t num;
        uint32_t len;

        NumLen(uint16_t a, uint32_t b) : num(a), len(b) {}
        ~NumLen() {}
    };

    void SetNumAndLenPersist(uint16_t a, uint32_t b){
        NumLen nl(a,b);
        node_allocator->nvm_memmove_persist(&num, &nl, 3);
    }

    void SetNumAndLenNodrain(uint16_t a, uint32_t b){
        NumLen nl(a,b);
        node_allocator->nvm_memcpy_nodrain(&num, &nl, 3);
    }

    void SetMinkeyPersist(inode_id_t key){
        node_allocator->nvm_memcpy_persist(&min_key, &key, sizeof(inode_id_t));
    }
    void SetMinkeyNodrain(inode_id_t key){
        node_allocator->nvm_memcpy_nodrain(&min_key, &key, sizeof(inode_id_t));
    }

    void SetMaxkeyPersist(inode_id_t key){
        node_allocator->nvm_memcpy_persist(&max_key, &key, sizeof(inode_id_t));
    }
    void SetMaxkeyNodrain(inode_id_t key){
        node_allocator->nvm_memcpy_nodrain(&max_key, &key, sizeof(inode_id_t));
    }

    void SetPrevPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&prev, &ptr, sizeof(pointer_t));
    }
    void SetPrevNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&prev, &ptr, sizeof(pointer_t));
    }

    void SetNextPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&next, &ptr, sizeof(pointer_t));
    }
    void SetNextNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&next, &ptr, sizeof(pointer_t));
    }

    void SetBufPersist(uint32_t offset, const void *ptr, uint32_t len){
        node_allocator->nvm_memmove_persist(buf + offset, ptr, len);
    }
    void SetBufNodrain(uint32_t offset, const void *ptr, uint32_t len){
        node_allocator->nvm_memmove_nodrain(buf + offset, ptr, len);
    }

};
struct IndexNodeEntry {
    uint64_t key;
    pointer_t pointer;
};

struct BptreeIndexNode {
    uint8_t type;
    uint8_t padding;
    uint16_t num;
    uint32_t len;
    uint64_t magic_number; //暂时没用，充当padding；
    pointer_t prev;
    pointer_t next;
    struct IndexNodeEntry entry[INDEX_NODE_CAPACITY];
};

struct BptreeLeafNode {
    uint8_t type;
    uint8_t padding;
    uint16_t num;
    uint32_t len;
    pointer_t prev;
    pointer_t next;
    char buf[LEAF_NODE_CAPACITY];      //key1,value1,key2,value2:   |--key1(8B)--|--value_len(4B)--|--value()--|--key2(8B)--|--value_len(4B)--|--value()--|
};

////// LinkList 操作
struct LinkListOp {
    pointer_t root;   //输入的root节点
    vector<pointer_t> free_list;   //待删除的节点
    pointer_t res;  //返回的节点
    uint32_t add_node_num; //新增的节点
    uint32_t delete_node_num;

    LinkListOp() : root(0), res(0), add_node_num(0), delete_node_num(0) {
        free_list.reserve(3);   //猜测大部分不超过3，
    }
    ~LinkListOp() {}
};

LinkNode *AllocLinkNode();
int LinkListInsert(LinkListOp &op, const inode_id_t key, const Slice &fname, const inode_id_t value);


//////


} // namespace name







#endif