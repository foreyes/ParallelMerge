#ifndef CHANNEL
#define CHANNEL

#include <iostream>
#include <vector>
#include <queue>
#include <functional>
#include <thread>
#include "datatype.h"
#include "loser_tree.hpp"

class RawChannel : public Channel
{
public:
    RawChannel(const Row* rows, int start, int end) : rows(rows), start(start), end(end)
    {
        cur = start;
        length = end - start;
    }

    /* fetch next and return */
    const Row* GetNext()
    {
        curRow = (cur < end ? &rows[cur++] : NULL);
        return GetCurrent();
    }
private:
    const Row* rows;
    int start, end, cur;
};

class MergeChannel : public Channel
{
public:
    MergeChannel(RowCmpFuncPtr cmp) : cmp(cmp) { length = 0; }
    ~MergeChannel()
    {
        /* release all children */
        for (auto child : children)
            delete child;
    }
    
    void AddChild(Channel* child)
    {
        if (!child || child->length == 0) {
            delete child; /* empty channel, just release it */
            return;
        }
        children.push_back(child);
        if (child->length >= 0) {
            length += child->length;
        } else {
            length = -1; /* unknown */
        }
    }

    const Row* GetNext()
    {
        if (!alreadyBuild) {
            BuildMerge();
            alreadyBuild = true;
        }
        int idx = GetMinCandidate();
        if (idx == -1)
            return curRow = NULL;
        curRow = children[idx]->GetCurrent();
        OnChildPop(idx);
        return GetCurrent();
    }

    RowCmpFuncPtr cmp;
protected:
    virtual void BuildMerge()
    {
        curValues.resize(children.size(), NULL);
        for (int i = 0; i < children.size(); i++) {
            curValues[i] = children[i]->GetNext();
        }
    }

    virtual int GetMinCandidate()
    {
        int minIdx = -1;
        const Row* minRow = NULL;
        for (int i = 0; i < children.size(); i++) {
            if (!curValues[i])
                continue;
            if (!minRow || !cmp(minRow, curValues[i])) {
                minRow = curValues[i];
                minIdx = i;
            }
        }
        return minIdx;
    }

    virtual void OnChildPop(int idx)
    {
        curValues[idx] = children[idx]->GetNext();
    }
protected:
    std::vector<Channel*> children;
private:
    bool alreadyBuild = false;
    std::vector<const Row*> curValues;
};

class HeapMergeChannel : public MergeChannel
{
public:
    HeapMergeChannel(RowCmpFuncPtr cmp) : MergeChannel(cmp), heap(Wrapper()) {}
private:
    void BuildMerge()
    {
        for (int i = 0; i < children.size(); i++) {
            const Row* row = children[i]->GetNext();
            if (row != NULL)
                heap.push(std::make_pair(row, i));
        }
    }

    int GetMinCandidate()
    {
        if (heap.empty())
            return -1;
        RowInfo minValue = heap.top();
        return minValue.second;
    }

    void OnChildPop(int idx) {
        if (!heap.empty())
            heap.pop();
        const Row* row = children[idx]->GetNext();
        if (row != NULL)
            heap.push(std::make_pair(row, idx));
    }

    std::function<bool(RowInfo&, RowInfo&)> Wrapper()
    {
        return [&](RowInfo& A, RowInfo& B)->bool {
            /* we don't care about the order of idx */
            return cmp(B.first, A.first); /* default is big top heap, we need small top */
        };
    }

    std::priority_queue<RowInfo, std::vector<RowInfo>, std::function<bool(RowInfo&, RowInfo&)>> heap;
};

class LoserMergeChannel : public MergeChannel
{
public:
    LoserMergeChannel(RowCmpFuncPtr cmp) : MergeChannel(cmp) {}
    ~LoserMergeChannel()
    {
        delete loserTree;
    }
private:
    void BuildMerge()
    {
        loserTree = new LoserTree(children, cmp);
        loserTree->BuildTree();
    }

    int GetMinCandidate()
    {
        return loserTree->GetWinner();
    }

    void OnChildPop(int idx) {
        loserTree->PopWinner();
    }

    LoserTree* loserTree;
};

class LinkedChannel : public Channel
{
public:
    LinkedChannel(Channel* chan1, Channel* chan2) : chan1(chan1), chan2(chan2)
    {
        /* if there is a NULL channel, we make it chan1 */
        /* TODO: PANIC if chan1 & chan2 both NULL */
        if (!chan2) {
            std::swap(chan1, chan2);
        }
        if (!chan1) {
            chan1Empty = true;
            length = chan2->length;
        } else if (chan1->length >= 0 && chan2->length >= 0) {
            length = chan1->length + chan2->length;
        } else {
            length = -1;
        }
    }
    ~LinkedChannel()
    {
        delete chan1;
        delete chan2;
    }

    const Row* GetNext()
    {
        if (chan1Empty)
            return curRow = chan2->GetNext();
        const Row* chan1Res = chan1->GetNext();
        if (!chan1Res) {
            chan1Empty = true;
            return curRow = chan2->GetNext();
        }
        return curRow = chan1Res;
    }
private:
    Channel* chan1;
    Channel* chan2;
    bool chan1Empty = false;
};

/* materialized channel with naive implementation, concurency is not suportted */
/* TODO: materialize with small buffer, and support concurrency (materialize & reading & discard) */
class SimpleMatChannel : public Channel
{
public:
    SimpleMatChannel(Channel* chan) : chan(chan)
    {
        length = chan->length;
        workThread = std::thread(&SimpleMatChannel::Materialize, this);
    }
    ~SimpleMatChannel()
    {
        if (localBuf)
            delete localBuf;
    }

    const Row* GetNext()
    {
        if (!matFinished) {
            workThread.join();
            matFinished = true;
        }
        if (length >= 0 && localBuf && cur < length)
            return curRow = &localBuf[cur++];
        if (cur < localVec.size())
            return curRow = &localVec[cur++];
        return curRow = NULL;
    }

private:
    void Materialize()
    {
        if (length >= 0) {
            localBuf = new Row[length];
            for (int i = 0; i < length; i++) {
                const Row* nextRow = chan->GetNext();
                if (!nextRow) {
                    length = i;
                    break;
                }
                else {
                    localBuf[i] = *nextRow;
                }
            }
        }
        else {
            const Row* nextRow = NULL;
            while (nextRow = chan->GetNext()) {
                localVec.push_back(*nextRow);
            }
        }
    }

    int cur = 0;
    Channel* chan;
    Row* localBuf = NULL;
    std::vector<Row> localVec;
    std::thread workThread;
    bool matFinished = false;
};

#endif