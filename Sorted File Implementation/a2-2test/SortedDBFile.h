#ifndef SORTED_DBFILE_H
#define SORTED_DBFILE_H
//#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
//#include "Comparison.h"
//#include "ComparisonEngine.h"
//#include "Defs.h"
#include "GenericDBFile.h"
#include "BigQ.h"

#include <string>

struct SortInfo { 
	OrderMaker *myOrder; 
	int runLength; 
};

class SortedDBFile: public GenericDBFile{

	enum mode {reading, writing};
	mode currMode;
	string fPath;
	File m_file;
	Page m_CurrPage;
	off_t currPageIdx;

	int runLength;
	OrderMaker m_Order;
	OrderMaker* pQueryOrder;

	static const int pipeBuffSz = 100;
	Pipe *pInputPipe;
	Pipe *pOutputPipe;
	BigQ *m_BQ;

	bool queryChanged;

public:
	SortedDBFile();
	//~SortedDBFile();

	int Open (char *fpath);
	int Create (char *fpath, fType file_type, void *startup);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void MergeHelper();
	void Merge();
	void SafeCleanUp();
	int binarySearch(Record& fetchme, OrderMaker& queryOrder,
				Record& literal, OrderMaker& cnfOrder, ComparisonEngine& cmpEngine);
	Record* GetMatchingPage(Record& literal);
	int bsearch(OrderMaker *queryOrderMaker, Record& literal, int low, int high);


};
#endif

