#include "SortedDBFile.h"

#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <cstdlib>

SortedDBFile::SortedDBFile() : currMode(reading), fPath(), m_file(), m_CurrPage(), currPageIdx(0), runLength(100), m_Order(), pInputPipe(NULL), pOutputPipe(NULL), m_BQ(NULL), queryChanged(true)
{}

/*SortedDBFile::~SortedDBFile(){
	if(pInputPipe)
		delete pInputPipe;
	if(pOutputPipe)
		delete pOutputPipe;
	if(m_BQ)
		delete m_BQ;
}*/


int SortedDBFile::Open(char *f_path){
	string metaDataFileName(f_path);
	metaDataFileName += ".meta";
	ifstream metaDataFile;
	metaDataFile.open(metaDataFileName.c_str());
	if(!metaDataFile){
		cerr<<"Error in opening the metadata file"<<endl;
		//exit(0);
		return 1;
	}

	int dbtype;
	metaDataFile >> dbtype; 
	metaDataFile >> runLength;
	metaDataFile >> m_Order;
	cout<<"Read the following metadata : "<<endl;
	cout<<"DB File type : "<<dbtype<<endl;
	cout<<"runLength : "<<runLength<<endl;
	cout<<"Sort Order is : "<<endl;
	m_Order.Print();

	fType dbFileType = (fType) dbtype;

	metaDataFile.close();

	fPath = f_path;
	m_file.Open(1, f_path);
	currMode = reading;
	MoveFirst();

	cout<<"File opened with "<< m_file.GetLength() << " pages" << endl;

	return 0;
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup){
	fPath = f_path;
	m_file.Open(0, f_path);
	return 1;
}

void SortedDBFile::Load(Schema &mySchema, char *loadpath){
	currMode = writing;

	if(m_BQ == NULL){
		pInputPipe = new Pipe(pipeBuffSz);
		pOutputPipe = new Pipe(pipeBuffSz);
		m_BQ = new BigQ(*pInputPipe, *pOutputPipe, m_Order, runLength);	
	}

	FILE *tblFile = fopen(loadpath, "r");
	if(!tblFile){
		cerr<<"Error in opening .tbl file"<<endl;
		exit(0);
	}

	Record temp_rec;
	int num_rec = 0;

	while(temp_rec.SuckNextRecord(&mySchema, tblFile)){
		++num_rec;
		pInputPipe->Insert(&temp_rec);
	}

	fclose(tblFile);

	cout<<"Number of record read from .tbl file is "<<num_rec<<endl;

	return;
}

void SortedDBFile::MoveFirst(){
	if(currMode == writing){
		Merge();
	}

	currPageIdx = (off_t) 0;
	if(m_file.GetLength() != 0){
		m_file.GetPage(&m_CurrPage, currPageIdx);
	}else{
		m_CurrPage.EmptyItOut();
	}
}

int SortedDBFile::Close(){
	if(m_file.GetLength() == 0){
		if(currMode == writing){
			if(m_BQ != NULL){
				pInputPipe->ShutDown();
				Record temp_rec;
				Page temp_page;
				while(pOutputPipe->Remove(&temp_rec) == 1){
					if(temp_page.Append(&temp_rec) == 0){
						if(m_file.GetLength() > 0){
							m_file.AddPage(&temp_page, m_file.GetLength() - 1);
							temp_page.EmptyItOut();
							temp_page.Append(&temp_rec);
						}else{
							m_file.AddPage(&temp_page, 0);
							temp_page.EmptyItOut();
							temp_page.Append(&temp_rec);
						}
					}
				}

				if(m_file.GetLength() > 0){
					m_file.AddPage(&temp_page, m_file.GetLength() - 1);
					temp_page.EmptyItOut();
				}else{
					m_file.AddPage(&temp_page, 0);
					temp_page.EmptyItOut();
				}

			SafeCleanUp();
		}
	}

	int retVal = m_file.Close();
	if(retVal >= 0){
		return 1;
	}else{
		return 0;
	}
	}else{
		if(currMode == writing){
			Merge();
		}else if(currMode == reading){
			//
		}

		int retVal = m_file.Close();

		if(retVal >= 0)
			return 1;
		else
			return 0;
	}
}

void SortedDBFile::Add(Record &addme){
	//cout<<"In SortedDBFile::Add"<<endl;
	if(currMode == writing){
		pInputPipe->Insert(&addme);
	}else if(currMode == reading){
		currMode = writing;
		if(m_BQ == NULL){
			pInputPipe = new Pipe(pipeBuffSz);
			pOutputPipe = new Pipe(pipeBuffSz);
			m_BQ = new BigQ(*pInputPipe, *pOutputPipe, m_Order, runLength);
			//cout<<"Created BigQ"<<endl;
		}

		pInputPipe->Insert(&addme);
		//cout<<"Inserted in input pipe"<<endl;
	}else{
		cerr<<"I guess the metadata file is corrupted. Shutting down"<<endl;
		exit(0);
	}
}

int SortedDBFile::GetNext(Record &fetchme){
	if(currMode == writing){
		Merge();
	}

	if(m_CurrPage.GetFirst(&fetchme) == 1){
		return 1;
	}else{
		++currPageIdx;
		if(currPageIdx + 1 <= m_file.GetLength() - 1){
			m_file.GetPage(&m_CurrPage, currPageIdx);
			int retVal = m_CurrPage.GetFirst(&fetchme);
			if(retVal != 1){
				// Error??
			}

			return 1;
		}else{
			return 0;
		}
	}

	return 0;
}

void SortedDBFile::MergeHelper(){

  Record temp_rec;
  int noRecordsInOrigFile = 0;

  MoveFirst();

  if (m_file.GetLength() != 0){
      while (GetNext(temp_rec) == 1){
          noRecordsInOrigFile++;
          pInputPipe->Insert(&temp_rec);
      }
  }

  pInputPipe->ShutDown();

  m_file.Close();
  char *file_path = new char[fPath.length() + 1];

  for(int i = 0; i < fPath.length(); ++i){
  	file_path[i] = fPath[i];
  }
  file_path[fPath.length()] = '\0';

  m_file.Open(0, file_path);
  currPageIdx = 0;

    int noBigQRecs = 0;
    int noBigQPages = 0;
    m_CurrPage.EmptyItOut();

    while (pOutputPipe->Remove(&temp_rec) == 1){
        noBigQRecs++;
        if (m_CurrPage.Append(&temp_rec) == 0){
            noBigQPages++;
            m_file.AddPage(&m_CurrPage,currPageIdx++);
            m_CurrPage.EmptyItOut();
            m_CurrPage.Append(&temp_rec);
          }
    }

    m_file.AddPage(&m_CurrPage,currPageIdx++);
    noBigQPages++;
    m_CurrPage.EmptyItOut();
}

void SortedDBFile::Merge(){
	currMode = reading;
	MergeHelper();
	SafeCleanUp();
	MoveFirst();
}

void SortedDBFile::SafeCleanUp(){
	delete pInputPipe;
	delete pOutputPipe;
	delete m_BQ;

	pInputPipe = NULL;
	pOutputPipe = NULL;
	m_BQ = NULL;
}

int SortedDBFile::binarySearch(Record& fetchme, OrderMaker& queryOrder,
					Record& literal, OrderMaker& cnfOrder, ComparisonEngine& cmpEngine){

	if(!GetNext(fetchme)) 
		return 0;

	int result = cmpEngine.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
	if(result > 0)
		return 0;
	else if(result == 0) 
		return 1;

	off_t low = currPageIdx, high = m_file.GetLastIndex();
	off_t mid = low + (high - low)/2;

	for(; low < mid ; mid = low + (high - low)/2 ){
		m_file.GetPage(&m_CurrPage, mid);
		if(!GetNext(fetchme)){
			cerr<<"Found an empty page"<<endl;
			cerr<<"Exit the program"<<endl;
			exit(-1);
		}
		result = cmpEngine.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
		if(result < 0) low = mid;
		else if(result > 0) high = mid - 1;
		else high = mid;
	} 

	m_file.GetPage(&m_CurrPage, low);
	do{
		if(!GetNext(fetchme)) return 0;
		result = cmpEngine.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
	} while(result < 0);
	return result == 0;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){
	if(currMode == writing){
		Merge();
	}

	if(queryChanged == true){
		//cout<<"Before making query"<<endl;
		//m_Order.Print();
		pQueryOrder = cnf.bSearchQueryMaker(m_Order);
		//cout<<"After query"<<endl;
		//m_Order.Print();
	}

	ComparisonEngine cmpEngine;

	if(pQueryOrder == NULL){
		while(GetNext(fetchme)){
			if(cmpEngine.Compare(&fetchme, &literal, &cnf)){
				return 1;
			}
		}
		return 0;
	}else{
		Record* pTemp_Rec;
		//cout<<"Using pQueryOrder"<<endl;
		pTemp_Rec = GetMatchingPage(literal);
		//cout<<"Got the matching page"<<endl;
		if(pTemp_Rec == NULL)
			return 0;

		fetchme.Consume(pTemp_Rec);
		if(cmpEngine.Compare(&fetchme, &literal, &cnf)){
			return 1;
		}

		while(GetNext(fetchme)){
			if(cmpEngine.Compare(&fetchme, &literal, pQueryOrder))
				return 0;
			else
				if(cmpEngine.Compare(&fetchme, &literal, &cnf))
					return 1;
		}
	}
/*	else{

	}*/

	return 0;
}

Record* SortedDBFile::GetMatchingPage(Record& literal){
	//cout<<"In GetMatchingPage"<<endl;
	if(queryChanged){
		//cout<<"Query Changed"<<endl;
		int low = currPageIdx;
		int high = m_file.GetLastIndex();
		//cout<<"My Order before"<<endl;
		//m_Order.Print();
		int matchPage = bsearch(pQueryOrder, literal, low, high);
		if(matchPage == -1) return NULL;
		if(matchPage != currPageIdx){
			m_CurrPage.EmptyItOut();
			m_file.GetPage(&m_CurrPage, matchPage);
			currPageIdx = matchPage + 1; // Check this carefully

		}
		queryChanged = false;
	}

	Record *pTmp_rec;
	ComparisonEngine cmpEngine;
	while(m_CurrPage.GetFirst(pTmp_rec)){
		if(cmpEngine.Compare(pTmp_rec, &literal, pQueryOrder) == 0){
			return pTmp_rec;
		}
	}

	if(currPageIdx <= m_file.GetLength() - 2){
		++currPageIdx;
		m_file.GetPage(&m_CurrPage, currPageIdx);
		while(m_CurrPage.GetFirst(pTmp_rec)){
			if(cmpEngine.Compare(pTmp_rec, &literal, pQueryOrder) == 0){
				return pTmp_rec;
			}
		}
	}
	//cout<<"Get Matching page returning NULL"<<endl;
	return NULL;
}

int SortedDBFile::bsearch(OrderMaker *queryOrderMaker, Record& literal, int low, int high){
	/*cout<<"Search OM"<<endl;
	queryOrderMaker->Print();
	cout<<"My Order : "<<endl;
	m_Order.Print();*/

	if(high < low) return -1;
	if(high == low ) return low;

	ComparisonEngine *pCmpEngine;
	Page temp_page;
	Record temp_rec;
	int mid = low + (high - low)/2;
	m_file.GetPage(&temp_page, (off_t) mid);

	int res;
	//Schema sch("catalog", "lineitem");

	temp_page.GetFirst(&temp_rec);
	//temp_rec.Print(&sch);

	res = pCmpEngine->Compare(&temp_rec, &m_Order, &literal, queryOrderMaker);
	if(res == -1){
		if(low == mid)
			return mid;

		return bsearch(queryOrderMaker, literal, low, mid-1);
	}else if(res == 0){
		return mid;
	}else{
		return bsearch(queryOrderMaker, literal, mid+1, high);
	}
}



