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
#define ERROR -1;

//------------structure of our operator tree-----------
/*                      result_table
                              |
                           orderby
                             |
                        having(filter)
                             |
                          groupby
                            |
                          project
                            |
                       join(maybe not)
                           |
                         filter
                           |
                          scan


*/
BasicType **Table::get_tb_type(void){
        int i = t_columns.size();
        int j;

        tc_type = new BasicType *[i];
        for(j=0;j<i;j++){
            tc_type[j] = ((Column *)g_catalog.getObjById(t_columns[j]))->getDataType();
        }
        return tc_type;
}

typedef struct temp_tb_info{
    Table *tb_temp;
    int    tb_col;
    int    top_col;
    BasicType **tb_type;
    Operator *top_op;
    BasicType **top_type;
}temp_tb_info;

typedef struct temp_join_info{
	int l_tb;
	int r_tb;
	int l_col;
	int r_col;
}temp_join_info;

int Executor::exec(SelectQuery *query, ResultTable &result)
{
    //---------write you code here--------------------------
if(query != NULL){

    int total_tb;                             //from tables numbers
    int total_select;                         //select column numbers
    int total_where;                          //where condtion numbers
    int total_join;                           //join numbers
    int total_having;                         //having condition numbers
    int i;

    Scan *scan_op[4]   = {NULL,NULL,NULL,NULL};                     //all of the scan operators, max is 4
    Filter *filter_op[4] = {NULL,NULL,NULL,NULL};                   //all of the where filter operators, max is 4
    Join *join_op[3]   = {NULL,NULL,NULL} ;                         //all of the join operators, max is 3
    Project *project_op   = {NULL};                                 //project operator, need 1
    GroupBy *groupby_op   = {NULL};                                 //groupby operator, need 1
    Filter *having_op[4] = {NULL,NULL,NULL,NULL};                   //all of the having operators,(actually the filter operator) max is 4
    OrderBy *orderby_op   = {NULL};                                 //orderby operator, need 1
    Table *stemp;
    //Operator *last_op;
    int temp_col;
    temp_tb_info temp_info[4];
    //initialize something
    total_tb = query->from_number;

    //now we start to build operator tree
    //bulid scan operator for each table
    int j;
    int m;

    for(i=0;i<total_tb;i++){
        stemp = (Table *)g_catalog.getObjByName(query->from_table[i].name);
        if(stemp == NULL) return ERROR;
        temp_col = stemp->getColumns().size();  //get coloumn number of the table

        temp_info[i].tb_temp    = stemp;
        temp_info[i].tb_col     = temp_col;
        temp_info[i].tb_type    = stemp->get_tb_type();

        temp_info[i].top_col    = temp_col;
        temp_info[i].top_type   = temp_info[i].tb_type;

        scan_op[i] = (Scan *)new Scan(query->from_table[i].name,temp_col);
        temp_info[i].top_op = scan_op[i];
    }
    //build filter operator
    int join_index[3];
    int64_t c_id,k;

    total_where = query->where.condition_num;
    for(total_join=0,i=0;i<total_where;i++){
        if(query->where.condition[i].compare==LINK){
        	join_index[total_join]=i;
        	total_join++;
        }
        else {
        	c_id = g_catalog.getObjByName(query->where.condition[i].column.name)->getOid();
        	for(j=0,k=-1;j<total_tb;j++){
        		k=temp_info[j].tb_temp->getColumnRank(c_id);
        		if(k>=0) break;
        	}
        	filter_op[i] = (Filter *)new Filter(k,
                                            query->where.condition[i].compare,
                                            query->where.condition[i].value,
                                            temp_info[j].top_op,
                                            temp_info[j].tb_col,
                                            temp_info[j].tb_type);

            temp_info[j].top_op = filter_op[i];
        }
    }
    //TODO:build join operator
    int join_loc;
    int l_cid,r_cid;
    int lk,lj,rk,rj;
    temp_join_info join_info[3];

    for(i=0;i<total_join;i++){
    	join_loc = join_index[i];
    	l_cid = g_catalog.getObjByName(query->where.condition[join_loc].column.name)->getOid();
        for(lj=0,lk=-1;lj<total_tb;lj++){
        	lk=temp_info[lj].tb_temp->getColumnRank(l_cid);
        	if(lk>=0) break;
        }
        r_cid = g_catalog.getObjByName(query->where.condition[join_loc].value)->getOid();
        for(rj=0,rk=-1;rj<total_tb;rj++){
        	rk=temp_info[rj].tb_temp->getColumnRank(r_cid);
        	if(rk>=0) break;
        }
        join_info[i].l_tb  = lj;
        join_info[i].l_col = lk;
        join_info[i].r_tb  = rj;
        join_info[i].r_col = rk;
    }

    int left_tb;
    int right_tb;
    int top_col_temp;
    int lcol,rcol;
    BasicType **top_type_temp;
    int join_final = 0;

    for(i=0;i<total_join;i++){
    	left_tb = join_info[i].l_tb;
    	right_tb = join_info[i].r_tb;
    	lcol = temp_info[left_tb].top_col;
    	rcol = temp_info[right_tb].top_col;

   		join_op[i] = new Join(join_info[i].l_col,
   			                  temp_info[left_tb].top_op,
   			                  lcol,
    			              temp_info[left_tb].top_type,
    			              join_info[i].r_col,
    			              temp_info[right_tb].top_op,
    			              rcol,
    			              temp_info[right_tb].top_type
    			              );
   		//update top select colomn
   		for(j=i+1;j<total_join;j++){
   			if(join_info[j].l_tb==right_tb) join_info[j].l_col +=lcol;
   			if(join_info[j].r_tb==right_tb) join_info[j].r_col +=lcol;
   		}
   		//update top operator
   		temp_info[left_tb].top_op = join_op[i];
   		temp_info[right_tb].top_op = join_op[i];
   		//update top total column
   		top_col_temp = lcol + rcol;
   		temp_info[right_tb].top_col = top_col_temp;
   		temp_info[left_tb].top_col = top_col_temp;
   		//update top column type
   		top_type_temp = new BasicType *[top_col_temp];
   		for(j=0;j<lcol;j++){
   			top_type_temp[j] = temp_info[left_tb].top_type[j];
   		}
   		for(j=0;j<rcol;j++){
   			top_type_temp[j+lcol] = temp_info[right_tb].top_type[j];
   		}
   		temp_info[left_tb].top_type = top_type_temp;
   		temp_info[right_tb].top_type = top_type_temp;
   		join_final = left_tb;
    }

    //build project operator
    total_select = query->select_number;
    final_col = total_select;
    //BasicType *final_type[4];
    int *pcol = new int[4];

    for(i=0;i<final_col;i++){
        final_type[i] = ((Column *)g_catalog.getObjByName(query->select_column[i].name))->getDataType();
        c_id = g_catalog.getObjByName(query->select_column[i].name)->getOid();
        for(j=0,k=-1;j<total_tb;j++){
        		k=temp_info[j].tb_temp->getColumnRank(c_id);
        		if(k>=0) break;
        	}
        pcol[i] = 0;

        for(m=0;m<j;m++){
        	pcol[i] += temp_info[m].tb_col;
        }
        pcol[i] += k;
    }

    if(total_join==0){
        project_op = (Project *)new Project(final_col,
    	                                pcol,
    	                                temp_info[0].top_op,
    	                                temp_info[0].top_col,
    	                                temp_info[0].top_type);
    }
    else{
        project_op = (Project *)new Project(final_col,
    	                                pcol,
    	                                join_op[total_join-1],
    	                                temp_info[join_final].top_col,
    	                                temp_info[join_final].top_type);
    }
    last_op = (Operator *)project_op;
    //build groupby operator
    int agg_num=0;
    AggrerateColumn agg_col[4];
    int group_col[4];

    for(i=0,k=0;i<final_col;i++){
    	if(query->select_column[i].aggrerate_method != NONE_AM){
    		agg_num++;
    		agg_col[k].id = i;
		agg_col[k].aggrerate_method = query->select_column[i].aggrerate_method;
    		k++;
    	}
    }

    for(i=0;i<query->groupby_number;i++){
    	for(j=0;j<final_col;j++){
    		if(!strcmp(query->select_column[j].name,query->groupby[i].name)) {group_col[i] = j;j+=10;}
    	}
    }

    if(query->groupby_number==0) ;
    else{
        groupby_op = (GroupBy *)new GroupBy(query->groupby_number,
        	                                group_col,
        	                                agg_num,
        	                                agg_col,
        	                                last_op,
        	                                final_col,
        	                                final_type);
        last_op = (Operator *)groupby_op;
    }

    //build having operator
    total_having = query->having.condition_num;
    for(i=0;i<total_having;i++){
    	for(k=0;k<final_col;k++){
    		if(!strcmp(query->select_column[k].name,query->having.condition[i].column.name)) break;
    	}
        having_op[i] = (Filter *)new Filter(k,
        	                                query->having.condition[i].compare,
        	                                query->having.condition[i].value,
        	                                last_op,
        	                                final_col,
        	                                final_type);
        last_op = (Operator *)having_op[i];
    }

    //build orderby operator
    int total_orderby;
    int orderby_index[4];
    total_orderby = query->orderby_number;

    for(i=0;i<total_orderby;i++){
        for(j=0;j<final_col;j++){
            if(strcmp(query->orderby[i].name,query->select_column[j].name)==0){
                orderby_index[i]=j;
                j+=10;
            }
        }
    }

    if(total_orderby==0) ;
    else{
        orderby_op = (OrderBy *)new OrderBy(total_orderby,
                                            orderby_index,
        	                                last_op,
        	                                final_col,
        	                                final_type);
        last_op = (Operator *)orderby_op;
    }

    last_op->init();
}

if(query == NULL){
    result.shut();
}
    //generate result table
    //last_op->init();
    result.init(final_type,final_col,1024);
    if(last_op->getNext(result)==true){
        return 1;
    }
    last_op->isEnd();
    return 0;
    /*ResultTable temp_re;
    char* copy_data;
    int counter = 0;
    result->init(final_type,final_col,(1<<20));
    temp_re.init(final_type,final_col);
    while(last_op->getNext(temp_re) == true){
    //temp_re.print();
    temp_re.dump(fp);
    for(int v=0;v<final_col;v++){
        copy_data = temp_re.getRC(0,v);
		result->writeRC(counter,v,copy_data);
    }
    result->row_number++;
    }
    printf("finish\n");
    if(last_op->isEnd()==true) return 1;
    return 0;*/
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
	int require_size = sizeof(int)*column_number;////2
	while (allocate_size < require_size)
		allocate_size = allocate_size << 1;
		if(allocate_size<8)
            allocate_size  *=2;
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
	ScanTable = (RowTable *)g_catalog.getObjByName(TableName);
    if(g_memory.alloc(buffer,512) <=0)
        return false;
	if(ScanTable == NULL)
		return false;
	return true;
}

bool Scan::getNext(ResultTable& ParentTempResult){
	/* get next tuple from the table */
    if(ScanCounter >= ScanTable->getRecordNum())
	return false;
	for(int i = 0; i < TempResultCols; i++){

		ScanTable->selectCol(ScanCounter,i,buffer);
        ParentTempResult.writeRC(0,i,buffer);
	}
//	ParentTempResult.row_number = 1;
//	ParentTempResult.print();
//printf("%d!!\n",mm);
    ScanCounter++;
	return true;
}


bool Scan::isEnd(void){
	/* return true */
	g_memory.free(buffer,128);
	return true;
}

/*******************************Filter*************************************/
bool Filter::init(void){
	/* init its child an allc memory */
	if(TempResult.init(TempResultType,TempResultCols) < 0)
		return false;
	if(ChildOperator->init() != true)
		return false;
	return true;
}

bool Filter::getNext(ResultTable& ParentTempResult){
    TempResult.row_number = 1;
 //   TempResult.print();
	/* get next result which satisfy the filter */
//	BasicType * compare = NULL;
        char* CopyData;
        int i;
        BasicType *compare;
		char changed = '\0';
		char* pointer = &changed;
		char* temp_value;
	while( ChildOperator->getNext(TempResult) == true){
		g_memory.alloc(pointer,32);
		TypeCode temp_Code = TempResultType[FilterCol]->getTypeCode();
        switch (temp_Code) {
        case INVID_C:
            printf("[Column][ERROR][init]: invid type! -1\n");
            return false;
        case INT8:
            compare = new TypeInt8();
            break;
        case INT16:
            compare = new TypeInt16();
            break;
        case INT32:
            compare = new TypeInt32();
            break;
        case INT64:
            compare = new TypeInt64();
            break;
        case FLOAT32:
            compare = new TypeFloat32();
            break;
        case FLOAT64:
            compare = new TypeFloat64();
            break;
        case CHARN:
            compare = new TypeCharN(TempResultType[FilterCol]->getTypeSize());
            break;
        case DATE:
            compare = new TypeDate();
            break;              // days from 1970-01-01 till current DATE
        case TIME:
            compare = new TypeTime();
            break;              // seconds from 00:00:00 till current TIME
        case DATETIME:
            compare = new TypeDateTime();
            break;              // seconŝds from 1970-01-01 00:00:00 till current DATETIME
        case MAXTYPE_C:
            printf("[Column][ERROR][init]: invid type! -1\n");
            return false;
        default :
            printf("[Column][ERROR][init]: invid type! -1\n");
            return false;
        }
        compare->formatBin((void*)pointer,(void*)CompareValue);
		temp_value = TempResult.getRC(0,FilterCol);
		if(compareResult(temp_value,pointer,compare_method,compare)){
			for(i = 0; i < TempResultCols; i++){
				CopyData = TempResult.getRC(0,i);
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
	if(ChildOperator->isEnd() != true)
		return false;
	return true;
}

bool Filter::compareResult(void *col_data,void *value,CompareMethod compare_method, BasicType *compare){
	switch(compare_method){
	case LT:
		return compare->cmpLT(col_data,value);
	case LE:
		return compare->cmpLE(col_data,value);
	case EQ:
		return compare->cmpEQ(col_data,value);
	case NE:
		return !(compare->cmpEQ(col_data,value));
	case GT:
		return compare->cmpGT(col_data,value);
	case GE:
		return compare->cmpGE(col_data,value);
	default:
		return -1;//ERROR
	}

}

/*******************************Project*************************************/
bool Project::init(void){
	/* alloc memory and init its child */
	if(TempResult.init(TempResultType,TempResultCols) < 0)
		return false;
	if(ChildOperator->init() != true)
		return false;
	return true;
}
//int procount = 0;
bool Project::getNext(ResultTable & ParentTempResult){
	/* get next result of Project */
	char *CopyData;
	//procount++;
	//printf("%d\n",procount);
	 if(ChildOperator->getNext(TempResult) != true)
		return false;
    ParentTempResult.row_number++;
	for(int i = 0; i < ProjectNumber; i++){
		CopyData = TempResult.getRC(0,ProjectCol[i]);
		ParentTempResult.writeRC(0,i,CopyData);
	}
	ParentTempResult.row_number = 1;
	//ParentTempResult.print();
	return true;
}

bool Project::isEnd(void){
	/* free memory and its child */
	if(TempResult.shut() != 0)
		return false;
	if(ChildOperator->isEnd() != true)
		return false;
	return true;
}

/*******************************Join**************************************/
bool Join::init(void){
	/* init its childs & allc memory & get all the data */
	if(LTempResult.init(LTempResultType,LTempResultCols) < 0)
		return false;
	if(LTable.init(LTempResultType,LTempResultCols,1<<24) < 0)
		return false;
	if(LChildOperator->init() != true)
		return false;
	if(RTempResult.init(RTempResultType,RTempResultCols) < 0)
		return false;
	if(RTable.init(RTempResultType,RTempResultCols,1<<26) < 0)
		return false;
	if(RChildOperator->init() != true)
		return false;
	/* get all the data */
	char *CopyData;
//	printf("%d,%d\n",LJoinCol,RJoinCol);
	for(int i = 0; LChildOperator->getNext(LTempResult); i++){
		for(int j = 0; j < LTempResultCols; j++){
			CopyData = LTempResult.getRC(0,j);
			LTable.writeRC(i,j,CopyData);
		}
		LTable.row_number++;
	}
	for(int i = 0; RChildOperator->getNext(RTempResult); i++){
		for(int j = 0; j < RTempResultCols; j++){
               // printf("%d\n",j);
            CopyData = RTempResult.getRC(0,j);
			RTable.writeRC(i,j,CopyData);
		}
		RTable.row_number++;
	}
//	printf("asdasdasdasdasdasd!!\n");
	return true;
}
//int countt = 0;
bool Join::getNext(ResultTable& ParentTempResult){
//    printf("fuck you !!!");
char* CopyData;
char* LData;
char* RData;
//countt++;
//printf("%d\n",countt);
//char zero = 0;
	/* user nest loop for a simple implement */
	while(LCounter< LTable.row_number){
            if(RCounter >= RTable.row_number){
            RCounter = 0;
            if(findit)
              //  if(++LCounter>= LTable.row_number)
              //      return false;
              LCounter++;
            }
		while(RCounter< RTable.row_number){
			/* if equal */
            LData = LTable.getRC(LCounter,LJoinCol);
			RData = RTable.getRC(RCounter,RJoinCol);
			if(LTempResultType[LJoinCol]->cmpEQ((void*)LData,(void*)RData)){
   //                 countt++;
				/* copy data */
			//	printf("%s,%s,%d,%d\n",LData,RData,RCounter,LCounter);
				for(int i = 0; i < LTempResultCols; i++){
					CopyData = LTable.getRC(LCounter,i);
					ParentTempResult.writeRC(0,i,CopyData);
				}
				for(int j = 0; j < RTempResultCols; j++){
					CopyData = RTable.getRC(RCounter,j);
					ParentTempResult.writeRC(0,j+LTempResultCols,CopyData);
				}
	//			ParentTempResult.row_number = 1;
	//			ParentTempResult.print();
				RCounter++;
				findit = true;
				return true;
			}
			RCounter++;
		}
		findit = false;
		LCounter++;
	//	printf("%d######\n",LCounter);
	}
	return false;
}

bool Join::isEnd(void){
	/* free memory and its child */
	if(LTempResult.shut() != 0)
		return false;
	if(LTable.shut() != 0)
		return false;
	if(LChildOperator->isEnd() != true)
		return false;
	if(RTempResult.shut() != 0)
		return false;
	if(RTable.shut() != 0)
		return false;
	if(RChildOperator->isEnd() != true)
		return false;
	return true;

}

/*******************************GroupBy*************************************/
bool GroupBy::init(void){

//for result_table init
	/* init its child and allc memory */
	BasicType **GroupByType;
	int GroupByCol = Group_Number + aggrerate_Number + 1;
	int col = Group_Number + 1;
	for(int i=0;i<Group_Number;i++)
    GroupByType[i] = TempResultType[Group_Col[i]];
    GroupByType[Group_Number] = new TypeInt16(); //count
    for(int i=0;i<aggrerate_Number;i++){
        switch(aggrerate_col[i].aggrerate_method){
            case COUNT:
            break;
            case MIN:
            case MAX:
            case SUM:
            case AVG:
            GroupByType[col] = TempResultType[aggrerate_col[i].id];
            col++;
            break;
            default:
                break;
        }
    }//for  calculate groupbytype
	if(TempResult.init(TempResultType,TempResultCols) < 0)
		return false;
//		printf("nimabinimabi!!!\n");
	if(GroupByTable.init(GroupByType,GroupByCol,1<<20) < 0)
		return false;
		//printf("nimabinimabi!!!\n");printf("nimabinimabi!!!%d\n",GroupByTable.row_number);
	if(ChildOperator->init() != true)
		return false;

	char *data[Group_Number];
	int  avg_c = 0;
	int  avg[4];
	int  m = 0;
	bool find = false;
//get all the data
	while (1){
        if(ChildOperator->getNext(TempResult) != true)
            break;
       // TempResult.row_number = 1;
       // TempResult.print();
		    find = false;
		for(int j = 0; j < Group_Number; j++){
			char * CopyData = TempResult.getRC(0,Group_Col[j]);
			data[j] = CopyData;
		}// copy group data

		for(m=0;m<GroupTableCounter;m++){
			find = true;
			for(int i=0;i<Group_Number;i++){
                 //   char *mm = GroupByTable.getRC(m,i);
			if(find && GroupByType[i]->cmpEQ((void*)data[i],(void*)GroupByTable.getRC(m,i)))
				find = true;
			else{
				find = false;
				break;
			}
		    }
		    if(find)
		    	break;
		} // 查找完成
    //printf("nimabinimabi!!!%d\n",GroupByTable.row_number);
		do_count(find,m,data);
		//printf("nimabinimabi!!!%d\n",GroupByTable.row_number);
		col = Group_Number;
		for(int i=0;i<aggrerate_Number;i++){
            col++;
        //    printf("%d\n",col);
			switch(aggrerate_col[i].aggrerate_method){
				case COUNT:
				    have_count = i;
				break;

				case SUM:
				do_add(find,aggrerate_col[i].id,m,col);
				break;

				case MIN:
				do_min(find,aggrerate_col[i].id,m,col);
				break;

				case MAX:
				do_max(find,aggrerate_col[i].id,m,col);
				break;

				case AVG:
				do_add(find,aggrerate_col[i].id,m,col);
				avg[avg_c] = col;
                avg_c++;
				break;

				default:
				break;
				} //switch
			} // for

		} //while(1)
    //printf("nimabinimabi!!!%d\n",GroupByTable.row_number);
		//deal with avg
		char* sumresult;
        char* avg_count;
		for(int i=0;i<GroupTableCounter;i++){ //do for every row
			avg_count = GroupByTable.getRC(i,Group_Number);
			for(int j=0;j<avg_c;j++){
				sumresult = GroupByTable.getRC(i,avg[avg_c]);
				*sumresult /= *avg_count;
			}
		}
	//	printf("nimabinimabi!!!%d\n",GroupByTable.row_number);
        GroupByTable.print();
		return true;
	}


bool GroupBy::getNext(ResultTable& ParentTempResult){
    /* get next result of OrderBy */
    char *CopyData;
    if(ReqCounter == GroupTableCounter)
        return false;
    for(int i = 0; i < Group_Number; i++){
        CopyData = GroupByTable.getRC(ReqCounter,i);
        ParentTempResult.writeRC(0,i,CopyData);
    }
    for(int j = 0;j < aggrerate_Number;j++){
        if(j == have_count){
        CopyData = GroupByTable.getRC(ReqCounter,Group_Number);
        ParentTempResult.writeRC(0,j+Group_Number,CopyData);
        }
        else if(j < have_count){
        CopyData = GroupByTable.getRC(ReqCounter,j+Group_Number+1);
        ParentTempResult.writeRC(0,j+Group_Number,CopyData);
        }
        else if(j > have_count && have_count>=0){
        CopyData = GroupByTable.getRC(ReqCounter,j+Group_Number);
        ParentTempResult.writeRC(0,j+Group_Number,CopyData);
        }
        else{
        CopyData = GroupByTable.getRC(ReqCounter,j+Group_Number+1);
        ParentTempResult.writeRC(0,j+Group_Number,CopyData);
        }
    }
    //printf("nimabinimabi!!!\n");
    ParentTempResult.row_number = 1;
    //ParentTempResult.print();
    ReqCounter++;
    return true;

}

bool GroupBy::isEnd(void){
	/* free memory and its child */
	if(TempResult.shut() != 0)
		return false;
		//     printf("IM SUPER MAN111\n");
	if(GroupByTable.shut() != 0)
		return false;
	// 	   printf("IM SUPER MAN2222\n");
	if(ChildOperator->isEnd() != true)
		return false;
		 //   printf("IM SUPER MAN!!!!!!!!!!!1\n");
	return true;

}

int a = 1;
int * b =  &a;

void GroupBy::do_count(bool find, int array,char** data){
	    if(find){
			char* number = GroupByTable.getRC(array,Group_Number);
			int temp = *number;
			temp++;
			GroupByTable.writeRC(array,Group_Number,((char*)&temp));//记录数量+1
			}
		else{
			for(int k=0;k<Group_Number;k++)
			GroupByTable.writeRC(GroupTableCounter,k,data[k]);
			char * one = (char *)b;
			GroupByTable.writeRC(array,Group_Number, one);
			GroupTableCounter++; // 行数+1
			GroupByTable.row_number++;
			}//插入新的一行

}

void GroupBy::do_max(bool find,int agg_col, int array, int col){
        char* number = GroupByTable.getRC(array,col);
        char* compare = TempResult.getRC(0,agg_col);
        char s[1024];
	    if(find){
			if(TempResultType[agg_col]->cmpGT((void*)compare,(void*)number))
                TempResultType[agg_col]->formatTxt(s,compare);
                TempResultType[agg_col]->formatBin(number,s);
			}
		else{
                TempResultType[agg_col]->formatTxt(s,compare);
                TempResultType[agg_col]->formatBin(number,s);
		}
}

void GroupBy::do_min(bool find,int agg_col, int array, int col){
        char* number = GroupByTable.getRC(array,col);
        char* compare = TempResult.getRC(0,agg_col);
        char s[1024];
	    if(find){
			if(TempResultType[agg_col]->cmpLT((void*)compare,(void*)number))
                TempResultType[agg_col]->formatTxt(s,compare);
                TempResultType[agg_col]->formatBin(number,s);
			}
		else{
                TempResultType[agg_col]->formatTxt(s,compare);
                TempResultType[agg_col]->formatBin(number,s);
		}
}

void GroupBy::do_add(bool find,int agg_col, int array, int col){
        char* number = GroupByTable.getRC(array, col);
        char* compare = TempResult.getRC(0, agg_col);
        char s[1024];

	    if(find){
                *(float*)number += *(float*)compare;
               // TempResultType[agg_col]->formatTxt(s,number);
               // TempResultType[agg_col]->formatBin(number,s);
                //GroupByTable.writeRC(array,col,&t);
			}
		else
           // printf("%d\n",*compare);
        GroupByTable.writeRC(array,col,compare);
}

/*******************************OrderBy*************************************/
bool OrderBy::init(void){
	/* alloc memory & init its child & get all result & sort */
	counter = 0;
	total = 0;
	int i;

	if(ChildOperator->init()==false) return false;
	temp = new ResultTable *[64];

	//temp = (ResultTable *)new ResultTable[64];
	rec[total].init(TempResultType,TempResultCols,1024);
	while(ChildOperator->getNext(rec[total])==true){
        total++;
        rec[total].init(TempResultType,TempResultCols,1024);
	}
	for(i=0;i<total;i++){
        temp[i] = &rec[i];
	}

	quicksort(temp,0,total-1,orderby_index,TempResultType,OrderCol);

	return true;
}


bool OrderBy::getNext(ResultTable& ParentTempResult){
    /* get next result of OrderBy */
    if(counter >= total) return false;

    ParentTempResult = *temp[counter];
    ParentTempResult.row_number=1;
    counter++;
    return true;
}

bool OrderBy::isEnd(void){
	/* free memory and its child */
	for(int i=0;i<total;i++){
        temp[i]->shut();
	}
	return true;
}

bool cmple(ResultTable *a,ResultTable *b,int *index,BasicType **type,int ordercol){
//if a<=b,return true
    char *data_a;
    char *data_b;
    int i;

    for(i=0;i<ordercol;i++){
        data_a = a->getRC(0,index[i]);
        data_b = b->getRC(0,index[i]);
        if(type[i]->cmpEQ((void *)data_a,(void *)data_b)==false)
            return type[i]->cmpLT((void *)data_a,(void *)data_b);
    }

    return true;
}

bool cmpless(ResultTable *a,ResultTable *b,int *index,BasicType **type,int ordercol){
//if a<b,return true
    char *data_a;
    char *data_b;
    int i;

    for(i=0;i<ordercol;i++){
        data_a = a->getRC(0,index[i]);
        data_b = b->getRC(0,index[i]);
        if(type[i]->cmpEQ((void *)data_a,(void *)data_b)==false)
            return type[i]->cmpLT((void *)data_a,(void *)data_b);
    }

    return false;
}

int quicksort(ResultTable **temp,int low,int high,int *index,BasicType **type,int ordercol){
    if(low>high) return 0;

    int first;
    int last;
    ResultTable *key;
    first = low;
    last = high;
    key = temp[first];

    while(first<last){
        while(first<last && cmpless(temp[last],key,index,type,ordercol)==false){
            last--;
        }
        temp[first] = temp[last];
        while(first<last && cmple(temp[first],key,index,type,ordercol)==true){
            first++;
        }
        temp[last] = temp[first];
    }
    temp[first]=key;
    quicksort(temp,low,first-1,index,type,ordercol);
    quicksort(temp,first+1,high,index,type,ordercol);

    return 0;
}
