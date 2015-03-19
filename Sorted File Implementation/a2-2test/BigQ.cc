#include "BigQ.h"
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>
#include <queue>
#include <time.h>

using namespace std;

void* worker_th_func(void* args);

//Role of the BigQ is to read data from input pipe
// sort the into runLen pages
// Merge the sorted data using TPMMS algorithm
// and write the merged data into output pipe
BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runlen) {

	BigQInfo *bQInfo = new BigQInfo();
	bQInfo->in = &in;
	bQInfo->out = &out;
	bQInfo->m_File = myFile;
	bQInfo->order = sortOrder;
	bQInfo->runLength = runlen;
	
	pthread_create(&worker_thread, NULL, &worker_th_func, (void*)bQInfo);

}

BigQ::~BigQ () {
}

void* worker_th_func(void* args){
    BigQInfo *pBQInfo = (BigQInfo*) args;
    Pipe *in = pBQInfo->in;
    Pipe *out = pBQInfo->out;
    File m_File = pBQInfo->m_File;
    OrderMaker sortOrder = pBQInfo->order;
    int runlen = pBQInfo->runLength;
    
    delete pBQInfo;
    pBQInfo = NULL;

    //Random file used for sorting the data
    //Deleted at the end. 
    //Better than using random_run_file
    int num = rand() % 10000;
	char fileName[10];
	sprintf(fileName, "%d", num);

	m_File.Open(0,fileName);

	Record temp_rec;
	Page temp_page;
	Page sort_page;
	vector<Record *> recVec;
	vector<int> runLensVec;

	int pageCounter = 0;
	int runCounter = 0;


	ComparisonEngine cmpEngine;

	//Read record from pipe till the input pipe is empty
	//append record temp_rec to page at page index and increment the record counter
	while(in->Remove(&temp_rec)){   
	    Record *pCRecord = new Record();
	    pCRecord->Copy(&temp_rec);

	    if(temp_page.Append(&temp_rec)){
	        recVec.push_back(pCRecord);
	    }else{
	        ++pageCounter;
	        if(pageCounter == runlen){
	            std::stable_sort(recVec.begin(), recVec.end(), CompareRecords(&sortOrder));
	            int runLength = 0;
                for(vector<Record *>::iterator it= recVec.begin();it != recVec.end(); ++it){
                    if(sort_page.Append(*it) == 0){	
                    	if(m_File.GetLength()==0){
                            m_File.AddPage(&sort_page, m_File.GetLength());
                            sort_page.EmptyItOut();
                            sort_page.Append(*it);
                            ++runLength;
                        }else{
                            m_File.AddPage(&sort_page, m_File.GetLength() - 1);
                            sort_page.EmptyItOut();
                            sort_page.Append(*it);
                            ++runLength;
                        }

                    }
                }

                if(sort_page.GetNumRecs() != 0) {	
                	 if(m_File.GetLength() == 0){
                        m_File.AddPage(&sort_page, m_File.GetLength());
                        sort_page.EmptyItOut();
                        ++runLength;
                    }else{
                        m_File.AddPage(&sort_page, m_File.GetLength()-1);
                        sort_page.EmptyItOut();
                        ++runLength;
                    }
                }

                recVec.clear();
                recVec.push_back(pCRecord);

                temp_page.EmptyItOut();
                temp_page.Append(&temp_rec);

                pageCounter=0;
                ++runCounter;

                runLensVec.push_back(runLength);
	        }else{
	            temp_page.EmptyItOut();
                temp_page.Append(&temp_rec);
                recVec.push_back(pCRecord);
	        }
	    }
	}

	temp_page.EmptyItOut();
	sort_page.EmptyItOut();

	if(!recVec.empty()){
		std::stable_sort(recVec.begin(), recVec.end(), CompareRecords(&sortOrder));
		int runLength = 0;
		for(vector<Record *>::iterator it = recVec.begin(); it != recVec.end(); ++it){
			if(sort_page.Append(*it) == 0){
				if(m_File.GetLength() == 0){
					m_File.AddPage(&sort_page, m_File.GetLength());
					sort_page.EmptyItOut();
					sort_page.Append(*it);
					++runLength;
				}else{
					m_File.AddPage(&sort_page, m_File.GetLength() - 1);
					sort_page.EmptyItOut();
					sort_page.Append(*it);
					++runLength;
				}
			}
		}

		if(sort_page.GetNumRecs() != 0){
			if(m_File.GetLength()==0){
				m_File.AddPage(&sort_page,m_File.GetLength());
				sort_page.EmptyItOut();
				++runLength;
			}else{
				m_File.AddPage(&sort_page,m_File.GetLength()-1);
				sort_page.EmptyItOut();
				++runLength;
			}

		}

		++runCounter;
		runLensVec.push_back(runLength);
	}

    for(vector<Record *>::iterator it = recVec.begin(); it != recVec.end(); ++it) delete *it;

    recVec.clear();
	m_File.Close();


	// Start the phase 2 of the TPMMS Algorithm
    //construct a priority queue over the sorted runs and dump sorted data into the out pipe

    m_File.Open(1,fileName);
	priority_queue<RecordWrap *, vector<RecordWrap *>, PQCompare> PQ(&sortOrder);
	Page inpPageBuff[runCounter];
	int pageIdxBuff[runCounter];

	vector<int> runNoVec;
	for(int i = 0;i < runCounter; ++i){
        int num = 0;
        for(vector<int>::iterator it = runLensVec.begin(); it != runLensVec.begin() + i; ++it){
            num += *it;
        }
        runNoVec.push_back(num);
	}

	//Get the first page of each run and then the first record of each first page into the PQ
	for(int i = 0;i < runCounter; ++i){
        m_File.GetPage(&(inpPageBuff[i]), runNoVec[i]);
		pageIdxBuff[i] = 0;
	}

	for(int i = 0;i < runCounter; ++i){
	    RecordWrap *pRecordWrap = new RecordWrap();
		pRecordWrap->runNo = i;
		inpPageBuff[i].GetFirst(&(pRecordWrap->m_Record));
		PQ.push(pRecordWrap);
	}

	//loop untill PQ is empty
	while(!PQ.empty()){
			RecordWrap *pRW = PQ.top();
			PQ.pop();
			int runNo = pRW->runNo;
			out->Insert(&(pRW->m_Record));
			RecordWrap *pRecWrap = new RecordWrap();

			if(inpPageBuff[runNo].GetFirst(&(pRecWrap->m_Record))){
			    pRecWrap->runNo=runNo;
				PQ.push(pRecWrap);
			}else{
				++pageIdxBuff[runNo];
				if(pageIdxBuff[runNo] < runLensVec[runNo]){
					m_File.GetPage(&(inpPageBuff[runNo]),pageIdxBuff[runNo]+runNoVec[runNo]);
				    inpPageBuff[runNo].GetFirst(&(pRecWrap->m_Record));
				    pRecWrap->runNo=runNo;
				    PQ.push(pRecWrap);
				}
			}
			
	}

    m_File.Close();
    remove(fileName);
	out->ShutDown ();
}

void BigQ::sort_run(Page *p_Page, int num_recs, File& new_file, int& group_idx, OrderMaker *sortOrder){

	Record** record_Buffer = new Record*[num_recs];

	int count = 0;
	int page_Idx =  0;

	do{	
		record_Buffer[count] = new Record();
		if((p_Page + page_Idx)->GetFirst(record_Buffer[count]) == 0){
			(p_Page + page_Idx)->EmptyItOut();	
			page_Idx++;
			(p_Page + page_Idx)->GetFirst(record_Buffer[count]);
			count++;
		}
		else
			count++;
		
	} while(count < num_recs);

	(p_Page + page_Idx)->EmptyItOut();

	std::sort(record_Buffer,record_Buffer + num_recs , CustomSort(sortOrder));

	int k = 0;
	int n_errors = 0, n_success = 0;
	Record *last = NULL, *prev = NULL;
	ComparisonEngine comp_engine;

	while (k < num_recs) {
		prev = last;
		last = record_Buffer[k];

		if (prev && last) {
			if (comp_engine.Compare (prev, last, sortOrder) == 1) {
				n_errors++;
			}
			else {
				n_success++;
			}
		}
		k++;
	}

	count = 0;
	Page *m_pPage = new Page();
	int isPageDirty = 0;		

	while(count < num_recs){

		if(m_pPage->Append(*(record_Buffer + count))==0){
			isPageDirty = 0;
			new_file.AddPage(m_pPage, (off_t) (group_idx++));
			m_pPage->EmptyItOut();
			m_pPage->Append(*(record_Buffer + count));
			count++;
		}
		else{
			isPageDirty = 1;
			count++;
		}						
	}

	if(isPageDirty == 1){
		new_file.AddPage(m_pPage, (off_t)(group_idx++));	
	}

	delete m_pPage;

	for(int i=0;i<num_recs;i++){
		delete *(record_Buffer + i);
	}

	delete record_Buffer;

}

// TPMMS phase 1 and phase 2 implementation
void* BigQ::TPMMS(void* arg){

	worker_thread_args *w_th_args = (worker_thread_args *)arg;

	char f_path[] = "random_run_file";

	File new_file;
	new_file.Open(0, f_path);

	Page *p_Page = new Page[*(w_th_args->run_length)]();	

	Record *temp_rec = new Record();

	int result = 1;
	int num_recs = 0;	
	int page_idx = 0;	
	int group_idx = 1;		
	int num_runs  = 1;

	//Read record from pipe till the input pipe is empty
	//append record temp_rec to page at page index and increment the record counter
	while(result != 0){

		result = w_th_args->input->Remove(temp_rec);
		//if(result == 0) cout<<"Result is 0"<<endl;
		if(result != 0){
			if(( p_Page + page_idx )->Append(temp_rec) == 1){
				num_recs++;
			}
			else if(++page_idx == *(w_th_args->run_length)){

				sort_run(p_Page,num_recs,new_file,group_idx,w_th_args->sortOrder);			

				num_runs++;

				page_idx=0;
				num_recs=0;
				delete [] p_Page;
				p_Page = new Page[*(w_th_args->run_length)](); 
				(p_Page+page_idx)->Append(temp_rec);
				num_recs++;
				delete temp_rec;			
				temp_rec = new Record();

			}
			else{
				(p_Page+page_idx)->Append(temp_rec);
				num_recs++;
				delete temp_rec;
				temp_rec = new Record();
			}
		}
		else{
			sort_run(p_Page,num_recs,new_file,group_idx,w_th_args->sortOrder);
		}
	}

	delete temp_rec;
	new_file.Close();

	// Start the phase 2 of the TPMMS Algorithm

	new_file.Open(1,f_path);
	priority_queue<RecordWrap* , vector<RecordWrap*> , CustomSort> PQ(CustomSort(w_th_args->sortOrder));

	Page *page_buff = new Page[num_runs];
	RecordWrap *p_rWrap = new RecordWrap();
	Record *p_Record = new Record();	

	for (int i = 0; i < num_runs; i++){

		new_file.GetPage((page_buff+i), (off_t)( 1 + (*w_th_args->run_length)*i));
		(page_buff + i)->GetFirst(p_Record);
		(p_rWrap->m_Record).Consume(p_Record);
		p_rWrap->runNo = i;		
		PQ.push(p_rWrap);	
		p_Record = new Record();
		p_rWrap = new RecordWrap;
	}

	int flags  = num_runs;
	int next  = 0;
	int start = 0;
	int end   = 1;
	int arr_a[num_runs];
	int arr_b[num_runs];	
	int index[num_runs][2];


	for(int i = 0;i < num_runs; i++){
		arr_a[i] = 0;
		arr_b[i] = 0;
	}
	
	for(int i = 0;i < num_runs - 1; i++){
		index[i][start] = 1 + ((*(w_th_args->run_length))*i);
		index[i][end]   = (*(w_th_args->run_length))*(i+1);
	}

	index[num_runs - 1][start] = 1 + ((*(w_th_args->run_length))*(num_runs - 1));
	index[num_runs - 1][end] = group_idx - 1;


	while(flags != 0){

		RecordWrap *p_rWrap;
		p_rWrap = PQ.top();
		PQ.pop();

		next = p_rWrap->runNo;

		w_th_args->output->Insert(&(p_rWrap->m_Record));

		RecordWrap *insert = new RecordWrap();		
		Record *p_Record = new Record();

		if(arr_a[next] == 0)
		{
			if((page_buff + next)->GetFirst(p_Record)==0){

				if( index[next][start]+ (++arr_b[next]) > index[next][end]){
					flags--;
					arr_a[next] = 1;				
				}
				else{
					new_file.GetPage(page_buff + next,(off_t)(index[next][start]+arr_b[next]));
					(page_buff+next)->GetFirst(p_Record);
					insert->m_Record = *p_Record;
					insert->runNo = next; 	
					PQ.push(insert);					
				}
			}
			else{
				insert->m_Record = *p_Record;
				insert->runNo = next; 	
				PQ.push(insert);
			}	
		}
	}

	while(!PQ.empty()){
		w_th_args->output->Insert(&((PQ.top())->m_Record));
		PQ.pop();
	}

	//pthread_exit(NULL); 
}
