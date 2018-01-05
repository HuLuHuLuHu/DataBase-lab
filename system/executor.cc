/**
 * @file    executor.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */
#include "executor.h"

int Executor::exec(SelectQuery *query, ResultTable *result)
{
	//---------write you code here--------------------------


	return 0;
}

// note: you should guarantee that col_types is useable as long as this ResultTable in use, maybe you can new from operate memory, the best method is to use g_memory.
int ResultTable::init(BasicType *col_types[], int col_num, int64_t capicity) {
	column_type = col_types;
	column_number = col_num;
	row_length = 0;
	buffer_size = g_memory.alloc (buffer, capicity);
	if(buffer_size != capicity) {
		printf ("[ResultTable][ERROR][init]: buffer allocate error!\n");
		return -1;
	}
	int allocate_size = 1;
	int require_size = sizeof(int)*column_number;
	while (allocate_size < require_size)
		allocate_size = allocate_size << 1;
	char *p = NULL;
	offset_size = g_memory.alloc(p, allocate_size);
	if (offset_size != allocate_size) {
		printf ("[ResultTable][ERROR][init]: offset allocate error!\n");
		return -2;
	}
	offset = (int*) p;
	for(int ii = 0;ii < column_number;ii ++) {
		offset[ii] = row_length;
		row_length += column_type[ii]->getTypeSize(); 
	}
	row_capicity = (int)(capicity / row_length);
	row_number   = 0;
	return 0;
}

int ResultTable::print (void) {
	int row = 0;
	int ii = 0;
	char buffer[1024];
	char *p = NULL; 
	while(row < row_number) {
		for( ; ii < column_number-1; ii++) {
			p = getRC(row, ii);
			column_type[ii]->formatTxt(buffer, p);
			printf("%s\t", buffer);
		}
		p = getRC(row, ii);
		column_type[ii]->formatTxt(buffer, p);
		printf("%s\n", buffer);
		row ++; ii=0;
	}
	return row;
}

int ResultTable::dump(FILE *fp) {
	// write to file
	int row = 0;
	int ii = 0;
	char buffer[1024];
	char *p = NULL; 
	while(row < row_number) {
		for( ; ii < column_number-1; ii++) {
			p = getRC(row, ii);
			column_type[ii]->formatTxt(buffer, p);
			fprintf(fp,"%s\t", buffer);
		}
		p = getRC(row, ii);
		column_type[ii]->formatTxt(buffer, p);
		fprintf(fp,"%s\n", buffer);
		row ++; ii=0;
	}
	return row;
}

// this include checks, may decrease its speed
char* ResultTable::getRC(int row, int column) {
	return buffer+ row*row_length+ offset[column];
}

int ResultTable::writeRC(int row, int column, void *data) {
	char *p = getRC (row,column);
	if (p==NULL) return 0;
	return column_type[column]->copy(p,data);
}

int ResultTable::shut (void) {
	// free memory
	g_memory.free (buffer, buffer_size);
	g_memory.free ((char*)offset, offset_size);
	return 0;
}

//---------------------operators implementation---------------------------

/*******************************Scan*************************************/
bool Scan::init(void){
	/* get the table pointer */
	ScanTable = (Table *)getObjByName(TableName);
	if(ScanTable == NULL)
		return false;
	return true;
}

bool Scan::getNext(ResultTable& ParentTempResult){
	/* get next tuple from the table */
	for(int i = 0; i < TempResultCols; i++){
		if(ScanTable->selectCol(ScanCounter,i,buffer) != true)
			return false;
		else
		   ParentTempResult.writeRC(0,i,buffer);
	}
	ScanCounter++;
	return true;
}
	 

bool Scan::isEnd(void){
	/* return true */
	return true;
}

/*******************************Filter*************************************/
bool Filter::init(void){
	/* init its child and allc memory */
	if(TempResult.init(TempResultType,TempResultCols) <= 0)
		return false;
	if(ChildOperator->init() != true)
		return false;
	return true;
}

bool Filter::getNext(ResultTable& ParentTempResult){
	/* get next result which satisfy the filter */
	while(ChildOperator->getNext(ResultTable)){
		BasicType compare(TempResultType[FilterCol]->b_type_code,TempResultType[FilterCol]->b_type_size);
		void *changed;
		compare.formatBin(changed,CompareValue);
		if(compareResult(ResultTable.getRC(0,FilterCol),changed,compare_method)){ 
			for(int i = 0; i < TempResultCols; i++){
				char * CopyData = TempResult.getRC(0,i);
				ParentTempResult.writeRC(0,i,CopyData);
			}
			return true;
		}
	}
	return false;
}

bool Filter::isEnd(void){
	/* free memory and its child */
	if(TempResult.shut() != 0)
		return false;
	if(ChildOperator->isEnd != true)
		return false;
	return true;
}

bool Filter::compareResult(void *col_data,void *value,CompareMethod compare_method){
	switch(compare_method){
	case LT:
		return compare.cmpLT(col_data,value);
	case LE:
		return compare.cmpLE(col_data,value);
	case EQ:
		return compare.cmpEQ(col_data,value);
	case NE:
		return !(compare.cmpEQ(col_data,value));
	case GT:
		return compare.cmpGT(col_data,value);
	case GE:
		return compare.cmpGE(col_data,calue);
	default:
		return -1;//ERROR 
	}

}

/*******************************Project*************************************/
bool Project::init(void){
	/* alloc memory and init its child */
	if(TempResult.init(TempResultType,TempResultCols) <= 0)
		return false;
	if(ChildOperator->init() != true)
		return false;
	return true;
}

bool Project::getNext(ResultTable& ParentTempResult){
	/* get next result of Project */
	 if(ChildOperator->getNext(TempResult) != true)
		return false;
	for(int i = 0; i < ProjectNumber; i++){
		char * CopyData = TempResult.getRC(0,ProjectCol[i]);
		ParentTempResult.writeRC(0,i,CopyData);
	}
	return true;
}

bool Project::isEnd(void){
	/* free memory and its child */
	if(TempResult.shut() != 0)
		return false;
	if(ChildOperator->isEnd != true)
		return false;
	return true;
}

/*******************************Join**************************************/
bool Join::init(void){
	/* init its childs & allc memory & get all the data */
	if(LTempResult.init(LTempResultType,LTempResultCols) <= 0)
		return false;
	if(LTable.init(LTempResultType,LTempResultCols) <= 0)
		return false;
	if(LChildOperator->init() != true)
		return false;
	if(RTempResult.init(RTempResultType,RTempResultCols) <= 0)
		return false;
	if(RTable.init(LTempResultType,LTempResultCols) <= 0)
		return false;
	if(RChildOperator->init() != true)
		return false;
	/* get all the data */
	for(int i = 0; LChildOperator->getNext(LTempResult); i++){
		for(int j = 0; j < LTempResultCols; j++){
			char * CopyData = LTempResult.getRC(0,j);
			LTable.writeRC(i,j,CopyData);
		}
	}
	for(int i = 0; RChildOperator->getNext(RTempResult); i++){
		for(int j = 0; j < RTempResultCols; j++){
			char * CopyData = RTempResult.getRC(0,j);
			RTable.writeRC(i,j,CopyData);
		}    
	}
	return true;
}

bool Join::getNext(ResultTable& ParentTempResult){
	/* user nest loop for a simple implement */
	while((LData = LTable.getRC(LCounter,LJoinCol)) != NULL;){
		while((RData = RTable.getRC(RCounter,RJoinCol)) != NULL;){
			/* if equal */
			if(LTempResultType[LJoinCol]->cmpEQ(LData,RData)){
				/* copy data */
				for(int i = 0; i < LTempResultCols; i++){
					char * CopyData = LTable.getRC(LCounter,i);
					ParentTempResult.writeRC(0,i,CopyData);
				}
				for(int j = 0; j < RTempResultCols; j++){
					char * CopyData = RTable.getRC(RCounter,j);
					ParentTempResult.writeRC(0,j+LTempResultCols,CopyData);
				}
				RCounter++;
				return true;
			}
			RCounter++;
		}
		LCounter++;
	}
	return false;
}

bool Join::isEnd(void){
	/* free memory and its child */
	if(LTempResult.shut() != 0)
		return false;
	if(LTable.shut() != 0)
		return false;
	if(LChildOperator->isEnd != true)
		return false;
	if(RTempResult.shut() != 0)
		return false;
	if(RTable.shut() != 0)
		return false;
	if(RChildOperator->isEnd != true)
		return false;
	return true;

}

/*******************************GroupBy*************************************/
bool GroupBy::init(void){
//for result_table init
	/* init its child and allc memory */
	if(TempResult.init(TempResultType,TempResultCols) <= 0)
		return false;
	if(GroupByTable.init(TempResultType,TempResultCols) <= 0)
		return false;
	if(ChildOperator->init() != true)
		return false;
	return true;
	TypeCharN Type;
	char data[Group_Number];
	int  col_count = Group_Number;
	int  avg_c = 0;
	int  avg[4];
//get all the data
	for(int i = 0; ChildOperator->getNext(TempResult); i++){
		bool find = TRUE;
		for(int j = 0; j < Group_Number; j++){
			char * CopyData = TempResult.getRC(0,Group_Col[j]);
			data[j] = CopyData;
		}
		int m;

		for(m=0;m<counter;m++){
			find = TRUE;
			for(int count=0;count<Group_Number;count++){
			if(find && Type.cmpEQ(char[count],GroupByTable.getRC(m,count)))
				find = TRUE;
			else{
				find = FALSE;
				break;
			}
		    }
		    if(find)
		    	break;
		} // 查找完成

		do_count(find,m);
		for(int c=0;c<aggrerate_Number;c++){
			col_count++;
			switch(aggrerate_col[c].aggrerate_method){
				case COUNT:
				break;

				case SUM:
				do_add(aggrerate_col[c].id,m,col_count);
				break;

				case MIN:
				do_min(aggrerate_col[c].id,m,col_count);
				break;

				case MAX:
				do_max(aggrerate_col[c].id,m,col_count);
				break;

				case AVG:
				do_add(aggrerate_col[c].id,m,col_count);
				avg_c++;
				avg[c-1] = col_count;
				break;
				
				default:
				break;
				}
			}
		}
		//deal with avg
		int* avg_temp;
		int* avg_count;
		for(int j=0;j<counter;j++){
			avg_count = GroupByTable.getRC(j,Group_Number);
			for(int a=0;a<avg_c;a++){
				avg_temp = GroupByTable.getRC(j,avg[avg_c]);
				*avg_temp /= *avg_count;
			}
		}
		return TRUE;
	}


bool GroupBy::getNext(ResultTable& ParentTempResult){
    /* get next result of OrderBy */
    if(ReqCounter == counter)
        return false;  
    for(int i = 0; i < TempResultCols; i++)
    {
        char * CopyData = GroupByTable(ReqCounter,i);
        ParentTempResult.writeRC(0,i,CopyData);
    }
    ReqCounter++;
    return true;

}

bool GroupBy::isEnd(void){
	/* free memory and its child */
	if(TempResult.shut() != 0)
		return false;
	if(GroupByTable.shut() != 0)
		return false;
	if(ChildOperator->isEnd != true)
		return false;
	free(OrderArray);
	return true;

}

void GroupBy::do_count(bool find, int array){
	    if(find){
			int* number = GroupByTable.getRC(array,Group_Number);
			*number ++;//记录数量+1
			}
		else{
			for(int k=0;k<Group_Number;k++)
			GroupByTable.writeRC(counter,k,data[k]);
			int *one;
			*one = 1;
			GroupByTable.writeRC(counter,Group_Number,one);
			counter ++; // 行数+1
			}//插入新的一行

}

void GroupBy::do_max(int agg_col, int array, int col){
			int* number = GroupByTable.getRC(array,col);
			int* temp = TempResult.getRC(0,agg_col);
			if(*temp > *number)
				GroupByTable.writeRC(array,col,temp);
}

void GroupBy::do_min(int agg_col, int array, int col){
			int* number = GroupByTable.getRC(array,col);
			int* temp = TempResult.getRC(0,agg_col);
			if(*temp < *number)
				GroupByTable.writeRC(array,col,temp);
}

void GroupBy::do_add(int agg_col, int array, int col){
			int* number = GroupByTable.getRC(array,col);
			int* temp = TempResult.getRC(0,agg_col);
			*number += *temp;
}



/*******************************OrderBy*************************************/
bool OrderBy::init(void){
	/* alloc memory & init its child & get all result & sort */
	if(TempResult.init(TempResultType,TempResultCols) <= 0)
		return false;
	if(OrderTable.init(TempResultType,TempResultCols) <= 0)
		return false;
	if(ChildOperator->init() != true)
		return false;

	/* get all the data */
	for(int i = 0; ChildOperator->getNext(TempResult); i++,OrderNumber++){
		for(int j = 0; j < TempResultCols; j++){
			char * CopyData = TempResult.getRC(0,j);
			OrderTable.writeRC(i,j,CopyData);
		}
	}

	/* sort */
	OrderArray = (struct OrderPair *)malloc(sizeof(struct OrderPair) * OrderNumber);
	for(int i = 0 ; i < OrderNumber; i++){
		OrderArray[i].OrderData = OrderTable.getRC(i,OrderCol);
		OrderArray[i].RowRank = i;
	}
	qsort(OrderArray,OrderNumber,sizeof(struct OrderPair),compare);
	return true;
}

                                                 
bool OrderBy::getNext(ResultTable& ParentTempResult){
    /* get next result of OrderBy */
    if(OrderCounter == OrderNumber)
        return false;
    for(int i = 0; i < TempResultCols; i++)
    {
        char * CopyData = OrderTable(OrderArray[OrderCounter].RowRank,i);
        ParentTempResult.writeRC(0,i,CopyData);
    }
    OrderCounter++;
    return true;
}

bool OrderBy::isEnd(void){
	/* free memory and its child */
	if(TempResult.shut() != 0)
		return false;
	if(OrderTable.shut() != 0)
		return false;
	if(ChildOperator->isEnd != true)
		return false;
	free(OrderArray);
	return true;
}

int OrderBy::compare(void * x,void * y){
	struct OrderPair * p1 = (struct OrderPair *)x;
	struct OrderPair * p2 = (struct OrderPair *)y;
	return TempResultType[OrderCol]->cmpLT(p1->OrderData,p2->OrderData);
}
