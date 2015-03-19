#include "DBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>

DBFile::DBFile(): gdbf(NULL){}

int DBFile::Open(char *f_path){
	string metaDataFileName;
	metaDataFileName.append(f_path);
	metaDataFileName += ".meta";

	ifstream metaDataFile;
	metaDataFile.open(metaDataFileName.c_str());
	if(!metaDataFile){
		cerr<<"Error in opening metadat file"<<endl;
		return 1;
	}

	int dbfile_type;
	metaDataFile >> dbfile_type;

	fType dbFileType = (fType) dbfile_type;
	metaDataFile.close();

	if(dbFileType == heap){
		cout<<"Opening a head db file"<<endl;
		gdbf = new HeapDBFile();
	}
	else if(dbFileType == sorted){
		cout<<"Opening a sorted db file"<<endl;
		gdbf = new SortedDBFile();
	}else{
		cout<<"Opening tree db file"<<endl;
		cout<<"But wait untill the next project :)"<<endl;
		exit(0);
	}

	return gdbf->Open(f_path);
}

int DBFile::Create(char *f_path, fType f_type, void *startup){
	string metaDataFileName(f_path);
	metaDataFileName += ".meta";

	ofstream metaDataFile;
	metaDataFile.open(metaDataFileName.c_str());
	if(!metaDataFile){
		cerr<<"Error in opening metadat file"<<endl;
		return 1;
	}

	metaDataFile<<f_type<<endl;
	if(f_type == sorted){
		SortInfo sInfo = *((SortInfo *)startup);
		metaDataFile<<sInfo.runLength<<endl;
		OrderMaker m_Order = *(OrderMaker *)sInfo.myOrder;
		metaDataFile << m_Order;
	}

	metaDataFile.close();
	cout<<"Wrote metadata to file : "<< f_path<<".meta"<<endl;

	if(f_type == heap){
		cout<<"Opening a head db file."<<endl;		
		gdbf = new HeapDBFile();
	}
	else if(f_type == sorted){
		cout<<"Opening a sorted db file" << endl;
		gdbf = new SortedDBFile();
	}else{
		cout<<"Wait to tree "<<endl;
		exit(0);
	}

	return gdbf->Create(f_path, f_type, startup);
}

void DBFile::Add(Record &rec){
	gdbf->Add(rec);
}

void DBFile::Load(Schema &mySchema, char *loadpath){
	gdbf->Load(mySchema, loadpath);
}

void DBFile::MoveFirst(){
	gdbf->MoveFirst();
}

int DBFile::Close(){
	return gdbf->Close();
}

int DBFile::GetNext(Record &fetchme){
	return gdbf->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){
	return gdbf->GetNext(fetchme, cnf, literal);
}

