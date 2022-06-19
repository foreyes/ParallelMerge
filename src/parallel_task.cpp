#include <bits/stdc++.h>
#include <thread>
#include "datatype.h"
#include "channel.hpp"
#include "data_gen.hpp"
#include "task.h"
using namespace std;

const int WORKERS = 8;

/* workers deal with a in [1000...250000], [250000, 500000]..., then perform a final merge */
template<typename MergeChanType>
void NaiveShardingParallel(const Row* rows, int nrows)
{
    MergeChannel* finalMergeChan = new MergeChannel(&CmpByB);

    int valueStart = 1;
    const int valuesPerThread = VALUE_NUM / WORKERS;
    for (int id = 1; id <= WORKERS; id++) {
        MergeChanType* subMergeChan = new MergeChanType(&CmpByB);

        int nextStart = (id == WORKERS ? VALUE_NUM + 1 : valueStart + valuesPerThread);
        for (int i = valueStart; i < nextStart; i++) {
            int value = i * 1000;
            int start = lower_bound(rows, rows + nrows, Row{ value, 10 }, CmpByAB) - rows;
            if (start == nrows)
                break;
            int end = lower_bound(rows, rows + nrows, Row{ value, 50 }, CmpByAB) - rows;
            RawChannel* rawChan = new RawChannel(rows, start, end);
            subMergeChan->AddChild(rawChan);
        }
        SimpleMatChannel* matChan = new SimpleMatChannel(subMergeChan);
        finalMergeChan->AddChild(matChan);

        valueStart = nextStart;
    }

    const Row* resRow = NULL;
    while (resRow = finalMergeChan->GetNext()) {
        OuputSimple(resRow);
    }

    delete finalMergeChan;
}

/* workers deal with b in [10, 20), [20, 30), [30, 40), [40, 50), then linked to each other */
template<typename MergeChanType>
void VerticalShardingParallel(const Row* rows, int nrows)
{
    LinkedChannel* linkChan = NULL;

    int valueStart = 10;
    const int valuesPerThread = (50 - 10) / WORKERS;
    for (int id = 1; id <= WORKERS; id++) {
        MergeChanType* mergeChan = new MergeChanType(&CmpByB);

        int nextStart = (id == WORKERS ? 50 : valueStart + valuesPerThread);
        for (int i = 1; i <= VALUE_NUM; i++) {
            int value = i * 1000;
            int start = lower_bound(rows, rows + nrows, Row{ value, valueStart }, CmpByAB) - rows;
            if (start == nrows)
                break;
            int end = lower_bound(rows, rows + nrows, Row{ value, nextStart }, CmpByAB) - rows;
            RawChannel* rawChan = new RawChannel(rows, start, end);
            mergeChan->AddChild(rawChan);
        }
        SimpleMatChannel* matChan = new SimpleMatChannel(mergeChan);
        linkChan = new LinkedChannel(linkChan, matChan);

        valueStart = nextStart;
    }

    const Row* resRow = NULL;
    while (resRow = linkChan->GetNext()) {
        OuputSimple(resRow);
    }

    delete linkChan;
}


template<typename MergeChanType>
void task4Temp(const Row* rows, int nrows)
{
    /* use HeapMergeChannel instead of naive MergeChannel */
    MergeChanType mergeChan(&CmpByB);
    for (int i = 1; i <= VALUE_NUM; i++) {
      int value = i * 1000;
      int start = lower_bound(rows, rows + nrows, Row{ value, 10 }, CmpByAB) - rows;
      if (start == nrows)
          break;
      int end = lower_bound(rows, rows + nrows, Row{ value, 50 }, CmpByAB) - rows;
      RawChannel* rawChan = new RawChannel(rows, start, end);
      mergeChan.AddChild(rawChan);
    }
    const Row* curRow = nullptr;
    while (curRow = mergeChan.GetNext()) {
        OuputSimple(curRow);
    }
}


int main() {
    int nrows = 0;
    const Row* testSet = VectorToArrayType(GetTestSetNaive(300000000), &nrows);
    cerr << "finish data gen" << endl;

    time_t start = time(0);

    // /* test naive */
    // start = time(0);
    // task4Naive(testSet, nrows);
    // cerr << "naive time used: " << difftime(time(0), start) << endl;

    /* test heap */
    start = time(0);
    task4Temp<HeapMergeChannel>(testSet, nrows);
    cerr << "heap time used: " << difftime(time(0), start) << endl;

    /* test loser tree */
    start = time(0);
    task4Temp<LoserMergeChannel>(testSet, nrows);
    cerr << "loser tree time used: " << difftime(time(0), start) << endl;

    /* test heap sharding parallel */
    start = time(0);
    NaiveShardingParallel<HeapMergeChannel>(testSet, nrows);
    cerr << "heap sharding time used: " << difftime(time(0), start) << endl;

    /* test loser sharding parallel */
    start = time(0);
    NaiveShardingParallel<LoserMergeChannel>(testSet, nrows);
    cerr << "loser sharding time used: " << difftime(time(0), start) << endl;

    /* test heap vertical sharding parallel */
    start = time(0);
    VerticalShardingParallel<HeapMergeChannel>(testSet, nrows);
    cerr << "heap vertical sharding time used: " << difftime(time(0), start) << endl;

    /* test loser vertical sharding parallel */
    start = time(0);
    VerticalShardingParallel<LoserMergeChannel>(testSet, nrows);
    cerr << "loser vertical sharding time used: " << difftime(time(0), start) << endl;
}