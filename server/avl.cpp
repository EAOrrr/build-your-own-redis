#include <assert.h>
#include "avl.h"


static uint32_t max(uint32_t lhs, uint32_t rhs) {
    return lhs < rhs ? rhs : lhs;
}

// maintain the height and cnt field
static void avl_update(AVLNode *node) {
    node->height = 1 + max(avl_height(node->left), avl_height(node->right));
    node->cnt = 1 + avl_cnt(node->left) + avl_cnt(node->right);
}

static AVLNode *rot_left(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->right;
    AVLNode *inner = new_node->left;
    // node <-> inner
    node->right = inner;
    if (inner) {
        inner->parent = node;
    }
    // parent <- new_node
    new_node->parent = parent;
    // new_node <-> node
    new_node->left = node;
    node->parent = new_node;
    // auxiliary data
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rot_right(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->left;
    AVLNode *inner = new_node->right;
    // node <-> inner
    node->left = inner;
    if (inner) {
        inner->parent = node;
    }
    // parent <- new_node
    new_node->parent = parent;
    // new_node <-> node
    new_node->right = node;
    node->parent = new_node;
    // auxiliary data
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

// the left subtree is taller by 2
static AVLNode *avl_fix_left(AVLNode *node) {
    if (avl_height(node->left->left) < avl_height(node->left->right)) {
        node->left = rot_left(node->left);  // Transformation 2
    }
    return rot_right(node);                 // Transformation 1
}

// the right subtree is taller by 2
static AVLNode *avl_fix_right(AVLNode *node) {
    if (avl_height(node->right->right) < avl_height(node->right->left)) {
        node->right = rot_right(node->right);
    }
    return rot_left(node);
}

// fix imbalanced nodes and maintain invariants until the root is reached
AVLNode *avl_fix(AVLNode *node) {
    while (true) {
        AVLNode **from = &node; // save the fixed subtree here
        AVLNode *parent = node->parent;
        if (parent) {
            // attach the fixed subtree to the parent
            from = parent->left == node ? &parent->left : &parent->right;
        }   // else: save to the local variable `node`
        // auxiliary data
        avl_update(node);
        // fix the height difference of 2
        uint32_t l = avl_height(node->left);
        uint32_t r = avl_height(node->right);
        if (l == r + 2) {
            *from = avl_fix_left(node);
        } else if (l + 2 == r) {
            *from = avl_fix_right(node);
        }
        // root node, stop
        if (!parent) {
            return *from;
        }
        // continue to the parent node because its height may be changed
        node = parent;
    }
}

// detach a node where 1 of its children is empty
static AVLNode *avl_del_easy(AVLNode *node) {
    assert(!node->left || !node->right);    // at most 1 child
    AVLNode *child = node->left ? node->left : node->right; // can be NULL
    AVLNode *parent = node->parent;
    // update the child's parent pointer
    if (child) {
        child->parent = parent; // can be NULL
    }
    // attach the child to the grandparent
    if (!parent) {
        return child;   // removing the root node
    }
    AVLNode **from = parent->left == node ? &parent->left : &parent->right;
    *from = child;
    // rebalance the updated tree
    return avl_fix(parent);
}

// detach a node and returns the new root of the tree
AVLNode *avl_del(AVLNode *node) {
    // the easy case of 0 or 1 child
    if (!node->left || !node->right) {
        return avl_del_easy(node);
    }
    // find the successor
    AVLNode *victim = node->right;
    while (victim->left) {
        victim = victim->left;
    }
    // detach the successor
    AVLNode *root = avl_del_easy(victim);
    // swap with the successor
    *victim = *node;    // left, right, parent
    if (victim->left) {
        victim->left->parent = victim;
    }
    if (victim->right) {
        victim->right->parent = victim;
    }
    // attach the successor to the parent, or update the root pointer
    AVLNode **from = &root;
    AVLNode *parent = node->parent;
    if (parent) {
        from = parent->left == node ? &parent->left : &parent->right;
    }
    *from = victim;
    return root;
}

// offset into the succeeding or preceding node.
// note: the worst-case is O(log N) regardless of how long the offset is.
AVLNode *avl_offset(AVLNode *node, int64_t offset) {
    int64_t pos = 0;    // the rank difference from the starting node
    while (offset != pos) {
        if (pos < offset && pos + avl_cnt(node->right) >= offset) {
            // the target is inside the right subtree
            node = node->right;
            pos += avl_cnt(node->left) + 1;
        } else if (pos > offset && pos - avl_cnt(node->left) <= offset) {
            // the target is inside the left subtree
            node = node->left;
            pos -= avl_cnt(node->right) + 1;
        } else {
            // go to the parent
            AVLNode *parent = node->parent;
            if (!parent) {
                return NULL;
            }
            if (parent->right == node) {
                pos -= avl_cnt(node->left) + 1;
            } else {
                pos += avl_cnt(node->right) + 1;
            }
            node = parent;
        }
    }
    return node;
}
