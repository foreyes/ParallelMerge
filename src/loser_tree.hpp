#ifndef LOSER_TREE
#define LOSER_TREE

#include <vector>
#include <queue>
#include "datatype.h"

class LoserNode
{
public:
    LoserNode(LoserNode* left = NULL, LoserNode* right = NULL) :
        left(left), right(right)
    {
        if (left) left->parent = this;
        if (right) right->parent = this;
        available = false;
        parent = NULL;
    }

    bool available;
    RowInfo loser;
    RowInfo winner;
    LoserNode* left;
    LoserNode* right;
    LoserNode* parent;
};

class LoserTree
{
public:
    /* DO NOT deal with much things in constructor, use BuildTree() */
    LoserTree(std::vector<Channel*>& chans, RowCmpFuncPtr cmp) : chans(chans), cmp(cmp) {}
    /* TODO */
    ~LoserTree()
    {
        for (auto node : interNodes) delete node;
        for (auto node : leafNodes) delete node;
    }

    /* build the structure */
    void BuildTree()
    {
        leafNodes.resize(chans.size(), NULL);
        std::queue<LoserNode*> q;
        for (int i = 0; i < chans.size(); i++) {
            const Row* row = chans[i]->GetNext();
            LoserNode* curNode = new LoserNode();
            curNode->available = true;
            curNode->winner = std::make_pair(row, i);
            leafNodes[i] = curNode;
            q.push(curNode);
        }
        /* build internal nodes */
        while (!q.empty()) {
            LoserNode *left = q.front();
            q.pop();
            if (q.empty()) {
                root = left;
                break;
            }
            LoserNode* right = q.front();
            q.pop();
            LoserNode* curNode = new LoserNode(left, right);
            interNodes.push_back(curNode);
            q.push(curNode);
        }
        BuildUp(root);
    }

    /* build up the tree data */
    void BuildUp(LoserNode* curNode)
    {
        if (curNode == NULL || curNode->available)
            return; /* already have data */
        BuildUp(curNode->left);
        BuildUp(curNode->right);
        curNode->winner = curNode->left->winner;
        curNode->loser = curNode->right->winner;
        /* winner > loser, invalid */
        if (!cmp(curNode->winner.first, curNode->loser.first)) {
            swap(curNode->winner, curNode->loser);
        }
        curNode->available = true;
    }

    /* update the winner/loser from branch bottom to the top */
    void Update(LoserNode *curNode)
    {
        if (curNode->parent == NULL)
            return;
        /* parent winner must be invalid and need to update */
        if (cmp(curNode->winner.first, curNode->parent->loser.first)) {
            curNode->parent->winner = curNode->winner; /* new winner */
        } else {
            /* old parent loser become winner, and we are loser */
            curNode->parent->winner = curNode->parent->loser;
            curNode->parent->loser = curNode->winner;
        }
        Update(curNode->parent);
    }

    /* erase a leaf node from the tree */
    void Erase(LoserNode* curNode)
    {
        if (curNode->parent == NULL) {
            curNode->available = false;
            return;
        }
        LoserNode* sibling;
        LoserNode* curParent = curNode->parent;
        if (curNode == curParent->left) {
            sibling = curParent->right;
        } else {
            sibling = curParent->left;
        }
        /* truncate upper inter node */
        sibling->parent = curParent->parent;
        if (!sibling->parent) {
            root = sibling;
        } else {
            if (sibling->parent->left == curParent) {
                sibling->parent->left = sibling;
            } else {
                sibling->parent->right = sibling;
            }
        }
        Update(sibling);
    }

    int GetWinner()
    {
        if (!root->available)
            return -1;
        return root->winner.second;
    }

    void PopWinner()
    {
        if (!root->available)
            return;
        int idx = root->winner.second;
        const Row* nextRow = chans[idx]->GetNext();
        if (nextRow) {
            leafNodes[idx]->winner = std::make_pair(nextRow, idx);
            Update(leafNodes[idx]);
        } else {
            Erase(leafNodes[idx]);
        }
    }

    LoserNode* root;
    std::vector<LoserNode*> interNodes;
    std::vector<LoserNode*> leafNodes;

    RowCmpFuncPtr cmp;
    std::vector<Channel*>& chans;
};

#endif