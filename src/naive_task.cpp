#include <bits/stdc++.h>
#include "datatype.h"
#include "channel.hpp"
#include "data_gen.hpp"
#include "task.h"
using namespace std;

Row sortedRows[] = {
    {1000, 20},
    {1000, 30},
    {1050, 28},
    {2000, 25},
    {2000, 40},
    {2000, 50},
    {3000, 18},
    {11000, 20},
    {11000, 30},
    {11050, 28},
    {12000, 25},
    {12000, 40},
    {12000, 50},
    {13000, 18}
};

void OuputSimple(const Row* row)
{
    if (row->a < 0)
        cout << row->a << ", " << row->b << endl;
}

bool CmpByAB(const Row& A, const Row& B)
{
    return (A.a != B.a ? A.a < B.a : A.b < B.b);
}

bool CmpByB(const Row* A, const Row* B)
{
    return A->b < B->b;
}


void task1(const Row* rows, int nrows)
{
    for (int i = 0; i < nrows; i++) {
        const Row* row = &rows[i];
        if (row->a != 1000 && row->a != 2000 && row->a != 3000)
            continue;
        if (row->b >= 10 && row->b < 50)
            OuputSimple(row);
    }
}

void task2(const Row* rows, int nrows)
{
    static const vector<int> selectedValues{1000, 2000, 3000};
    for (int value : selectedValues) {
        int start = lower_bound(rows, rows + nrows, Row{ value, 10 }, CmpByAB) - rows;
        int end = lower_bound(rows, rows + nrows, Row{ value, 50 }, CmpByAB) - rows;
        for (int i = start; i < end; i++) {
            OuputSimple(&rows[i]);
        }
    }
}

void task3(const Row* rows, int nrows)
{
    MergeChannel mergeChan(&CmpByB);
    static const vector<int> selectedValues{1000, 2000, 3000};
    for (int value : selectedValues) {
        int start = lower_bound(rows, rows + nrows, Row{ value, 10 }, CmpByAB) - rows;
        int end = lower_bound(rows, rows + nrows, Row{ value, 50 }, CmpByAB) - rows;
        RawChannel* rawChan = new RawChannel(rows, start, end);
        mergeChan.AddChild(rawChan);
    }
    const Row* curRow = nullptr;
    while (curRow = mergeChan.GetNext()) {
        OuputSimple(curRow);
    }
}

void task4Naive(const Row* rows, int nrows)
{
    MergeChannel mergeChan(&CmpByB);
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

template<typename MergeChanType>
void task4(const Row* rows, int nrows)
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

// int main() {
//     // task3(sortedRows, 14);
//     // cout << endl << endl;
//     // task4Naive(sortedRows, 14);
//     // cout << endl << endl;
//     // task4(sortedRows, 14);
//     int nrows = 0;
//     const Row* testSet = VectorToArrayType(GetTestSetNaive(10000000), &nrows);

//     time_t start = time(0);
//     /* test heap */
//     start = time(0);
//     task4(testSet, nrows);
//     cerr << "Heap time used: " << difftime(time(0), start) << endl;

//     /* test naive */
//     start = time(0);
//     task4Naive(testSet, nrows);
//     cerr << "Naive time used: " << difftime(time(0), start) << endl;
// }