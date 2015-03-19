#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapDBFile.h"
#include "Defs.h"
#include <iostream>

// stub file .. replace it with your own HeapDBFile.cc

HeapDBFile::HeapDBFile () {

	m_pFile = new File();
	m_pReadPage = new Page();
	m_pWritePage = new Page();
	m_pRecord = new Record();

	currIndex = 0;
}

HeapDBFile::~HeapDBFile(){
	
	delete m_pFile;
	delete m_pReadPage;
	delete m_pWritePage;
	delete m_pRecord;

}

/*
	f_path : path to binary data file
*/
int HeapDBFile::Create (char *f_path, fType f_type, void *startup = NULL) {
	if(f_path == NULL){
		std::cout<<"File path is empty!! Please provide a valid file path."<<std::endl;
		return 0;
	}
	m_pFile->Open(0,f_path);
	return 1;
}

// tested 
/*void HeapDBFile::Load (Schema &f_schema, char *loadpath) {

	int recordCounter = 0;

	FILE *textFile = fopen(loadpath, "r");
	if(textFile == NULL){
		std::cerr<<"Error opening file!"<<std::endl;
		return;
	}

	while(m_pRecord->SuckNextRecord(&f_schema, textFile)){
		++recordCounter;
		if(!(m_pWritePage->Append(m_pRecord))){
			// Page is full, so add the page to file and flush its content
			//cout<<"Page is full"<<endl;
			m_pFile->AddPage(m_pWritePage, m_pFile->GetLength());
			m_pWritePage->EmptyItOut();
			//m_pWritePage->Append(m_pRecord);
		}
	}

	m_pFile->AddPage(m_pWritePage, m_pFile->GetLength());
	m_pWritePage->EmptyItOut();

	fclose(textFile);
	cout<<"No. of records read is : "<<recordCounter<<endl;

	return;
}*/

void HeapDBFile::Load (Schema &f_schema, char *loadpath) {

	int recordCounter = 0;

	FILE *textFile = fopen(loadpath, "r");
	if(textFile == NULL){
		std::cerr<<"Error opening file!"<<std::endl;
		return;
	}

	Record tempRecord;
	while(tempRecord.SuckNextRecord(&f_schema, textFile)){
		++recordCounter;
		Add(tempRecord);
	}

	fclose(textFile);
	cout<<"No. of records read is : "<<recordCounter<<endl;

	return;
}


// tested
// Open the existing db file
int HeapDBFile::Open (char *f_path) {
	if(f_path == NULL){
		std::cout<<"File path is empty!! Please provide a valid file path."<<std::endl;
		return 0;
	}
	m_pFile->Open(1,f_path);
	return 1;
}

void HeapDBFile::MoveFirst () {
	
	//if(m_pFile->GetLength() != 0) // We should instead exit, as would be done by GetPage call
	currIndex = 0;
	m_pFile->GetPage(m_pReadPage, off_t(0));
	return;
}

int HeapDBFile::Close () {
	m_pFile->Close();
	return 1;
}

// Buggy
/*void HeapDBFile::Add (Record &rec) {

	// Get the last page in the file and add to it
	if(&rec != NULL){
		m_pRecord->Consume(&rec);
		m_pFile->GetPage(m_pWritePage, off_t(m_pFile->GetLength()));
		if(!m_pWritePage->Append(m_pRecord)){
			std::cout<<"Error adding record. The page is full!!"<<std::endl;
		}
	}
}*/

void HeapDBFile::Add (Record &rec) {

	if(&rec == NULL)
		return;

	m_pRecord->Consume(&rec);

	// Case : File is empty
	if(m_pFile->GetLength() == 0){
		if(m_pWritePage->Append(m_pRecord) == 1){
			//Add the first page, which is actually the second page added in file since 
			//the first page contains no data.
			//cout<<"Appended!!"<<endl;
			m_pFile->AddPage(m_pWritePage,0);	
			//cout<<"Page Added!"<<endl;
		}
	}
	// Case : File is not empty
	else{
		// Get copy of the data in the last page
		m_pFile->GetPage(m_pWritePage, off_t(m_pFile->GetLength() - 2));
		if(m_pWritePage->Append(m_pRecord) == 1){
			m_pFile->AddPage(m_pWritePage, off_t(m_pFile->GetLength() - 2));
		}
		else{
			// The last page in the file is full. So add a fresh page to file.
			m_pWritePage->EmptyItOut();
			m_pWritePage->Append(m_pRecord);

			// Should I write this page now or when it's full?
			m_pFile->AddPage(m_pWritePage, off_t(m_pFile->GetLength() - 1));
		}
	}
}

int HeapDBFile::GetNext (Record &fetchme) {
	if(m_pReadPage->GetFirst(&fetchme) == 0){
		// Page empty. Try to  fetch the next page in file if not EOF.
		++currIndex;
		if(currIndex >= m_pFile->GetLength() - 1){
			// End of file
			--currIndex;
			return 0;
		}else{
			m_pFile->GetPage(m_pReadPage, currIndex);
			m_pReadPage->GetFirst(&fetchme);
		}
	}

	return 1;
}

int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	while(GetNext(fetchme)){
		if(cmpE.Compare(&fetchme, &literal, &cnf)){
			return 1;
		}
	}

	return 0;
}
