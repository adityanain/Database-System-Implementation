#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"


using namespace std;

struct BigQInfo{
   	Pipe *in;
	Pipe *out;
   	File m_File;
   	OrderMaker order;
   	int runLength;
};

struct RecordWrap{
	Record m_Record;
	int runNo;
};

class BigQ{

	struct worker_thread_args {                                                   		
		Pipe* input;
		Pipe* output;
		OrderMaker *sortOrder;
		int* run_length;	
	}worker_args;

	int no_runs;
	int page_idx;

	pthread_t worker_thread; 
	File myFile;

    void* TPMMS(void* arg);
	void sort_run(Page*, int, File&, int&, OrderMaker *);

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runlen);
	~BigQ ();
};

class CompareRecords{
    ComparisonEngine cmpEngine;
	OrderMaker* order;
public:
	CompareRecords(OrderMaker* sortOrder) {
		order = sortOrder;
	}

	bool operator()(Record *R1,Record *R2) {
		return cmpEngine.Compare(R1,R2,order)<0;
	}
};



class PQCompare{
    ComparisonEngine cmpEngine;
	OrderMaker* order;
public:
	PQCompare(OrderMaker *sortOrder){
		order = sortOrder;
	}

	bool operator()(RecordWrap *pLHSRun, RecordWrap *pRHSRun){
	    return cmpEngine.Compare(&(pLHSRun->m_Record), &(pRHSRun->m_Record), order) >= 0;
	}
};

class CustomSort{
	OrderMaker *sortOrder;

public:
	CustomSort(){};
	~CustomSort(){};

	CustomSort(OrderMaker *order){
		sortOrder = order;
	}

	bool operator()(Record *r_one, Record *r_two) const{
		ComparisonEngine *comp_engine;
		if(comp_engine->Compare(r_one,r_two, sortOrder) < 0)
			return true;
		else
			return false;
	}

	bool operator()(RecordWrap *r_one, RecordWrap *r_two) const{

		ComparisonEngine *comp_engine;
		if(comp_engine->Compare(&(r_one->m_Record),&(r_two->m_Record), sortOrder) < 0)
			return false;
		else
			return true;		
	}
};

#endif
