#ifndef TASK
#define TASK

#include "datatype.h"

extern void OuputSimple(const Row* row);
extern bool CmpByAB(const Row& A, const Row& B);
extern bool CmpByB(const Row* A, const Row* B);

extern void task4Naive(const Row* rows, int nrows);
extern void task4(const Row* rows, int nrows);

#endif