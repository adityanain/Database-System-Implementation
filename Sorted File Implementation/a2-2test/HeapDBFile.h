#ifndef HEAP_DBFILE_H
#define HEAP_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
//#include "DBFile.h"

// stub HeapDBFile header..replace it with your own HeapDBFile.h 

//typedef enum {heap, sorted, tree} fType;

class HeapDBFile : public GenericDBFile{

	File* m_pFile = NULL;
	Page* m_pReadPage = NULL;
	Page* m_pWritePage = NULL;
	Record* m_pRecord = NULL;
	ComparisonEngine cmpE;
	int currIndex;

public:
	HeapDBFile (); 
	~HeapDBFile();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
