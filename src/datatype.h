#ifndef DATATYPE
#define DATATYPE

#include <utility>

typedef struct Row
{
    int a;
    int b;
} Row;

typedef std::pair<const Row*, int> RowInfo;
typedef bool (*RowCmpFuncPtr)(const Row*, const Row*);

class Channel
{
public:
    virtual ~Channel(){}
    virtual const Row* GetNext(){ return 0; }

    const Row* GetCurrent() { return curRow; }

    int length = -1; /* -1 means unknown or not sure */
protected:
    const Row* curRow = 0;
};

#endif

