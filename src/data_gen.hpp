#ifndef DATA_GEN
#define DATA_GEN

#include <bits/stdc++.h>
#include "datatype.h"

const int VALUE_NUM = 1000;

inline std::vector<Row>* GetTestSetNaive(int size)
{
    std::vector<Row>* testSet = new std::vector<Row>(size);

    srand((unsigned)time(0));
    for (int i = 1; i <= VALUE_NUM; i++) {
        int value = i * 1000;

        double totalB = 100, curB = 0;
        for (int j = size / VALUE_NUM; j > 0; j--) {
            double remainB = totalB - curB;
            /* [0, remainB * 2] */
            curB += (1.0f * (rand() % 10001)) / 10000 * remainB / j * 2;
            testSet->push_back(Row{ value, (int)curB });
        }
    }
    return testSet;
}

inline const Row* VectorToArrayType(std::vector<Row>* vecSet, int* nsize)
{
    *nsize = vecSet->size();
    Row* testSet = new Row[vecSet->size()];
    if (!vecSet->empty()) {
        memcpy(testSet, &(*vecSet)[0], vecSet->size() * sizeof(Row));
    }
    delete vecSet;
    return testSet;
}

#endif