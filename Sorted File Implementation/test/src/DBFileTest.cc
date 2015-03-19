#include <iostream>
#include <limits.h>
#include "gtest/gtest.h"
#include "DBFile.h"
#include <sys/stat.h>
#include <fstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

class DBFileTest : public ::testing::Test {
protected:
	virtual void SetUp(){
		struct stat fileInfo;
		if(stat(dbfile_path, &fileInfo) == -1){
			// File doesn't exist
			//cout<<"File doesn't exist"<<endl;
			dbfile.Create(dbfile_path, heap, NULL );
			sch = new Schema(catalog_path, lineitem);
			dbfile.Load(*sch , tpch_file );
			cout<<"File created & loaded"<<endl;
		}else{
			dbfile.Open(dbfile_path);
		}
	}
	virtual void TearDown(){
		dbfile.Close();
/*		if(!remove(dbfile_path)){
			cerr<<"DBFile could not be deleted."<<endl;
		}*/

	}

	DBFile dbfile;
	Schema *sch;
	char *lineitem = "lineitem";
	char *dbfile_dir = "";
	char *tpch_file = "../../DB/lineitem.tbl";
	char *dbfile_path = "../lineitem.bin";
	char *catalog_path = "../../DB/catalog";

};

TEST_F(DBFileTest, DBFileCreationTest ){
	struct stat fInfo;
	EXPECT_EQ(0u, stat(dbfile_path, &fInfo));
}

TEST_F(DBFileTest, DBFileOpenTest ){
	ifstream ifs(dbfile_path);
	EXPECT_TRUE(ifs.is_open());
}

TEST_F(DBFileTest, DBFileLoadTest){
	// check if file is loaded properly or not
	ifstream ifs(tpch_file);
	EXPECT_TRUE(ifs.is_open());

	dbfile.MoveFirst();
	Record temp;
	string recordS;
	int dbRecordCounter = 0;
	int tpchRecordCounter = 0;

	while(dbfile.GetNext(temp) == 1){
		++dbRecordCounter;		
	}

	while(getline(ifs, recordS)){
		++tpchRecordCounter;
	}

	EXPECT_EQ(dbRecordCounter, tpchRecordCounter);

	ifs.close();
}

//Test if the first record is correct in the DB File
TEST_F(DBFileTest, DBFileMoveFirstTest){
	
	FILE* textFile = fopen(tpch_file, "r");

	dbfile.MoveFirst();

	Record firstRecord;
	dbfile.GetNext(firstRecord);

	string dbRecStr;
	firstRecord.toString(sch, dbRecStr);

	string recStr;
	Record rec;
	rec.SuckNextRecord(sch, textFile);
	rec.toString(sch, recStr);

	EXPECT_EQ(dbRecStr.c_str(), recStr.c_str());

	fclose(textFile);

}

TEST_F(DBFileTest, NextRecordTest){
	FILE* textFile = fopen(tpch_file, "r");

	dbfile.MoveFirst();

	Record firstRecord;
	string dbRecStr;
	string recStr;
	Record rec;

	while(dbfile.GetNext(firstRecord) && rec.SuckNextRecord(sch, textFile)){
		firstRecord.toString(sch, dbRecStr);	
		rec.toString(sch, recStr);
		EXPECT_EQ(dbRecStr.c_str(), recStr.c_str());
	}

	fclose(textFile);
}
