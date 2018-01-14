/**
 * @file    executor.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * definition of executor
 *
 */

#ifndef _EXECUTOR_H
#define _EXECUTOR_H

#include "catalog.h"
#include "mymemory.h"

/** aggrerate method. */
enum AggrerateMethod {
    NONE_AM = 0, /**< none */
    COUNT,       /**< count of rows */
    SUM,         /**< sum of data */
    AVG,         /**< average of data */
    MAX,         /**< maximum of data */
    MIN,         /**< minimum of data */
    MAX_AM
};

/** compare method. */
enum CompareMethod {
    NONE_CM = 0,
    LT,        /**< less than */
    LE,        /**< less than or equal to */
    EQ,        /**< equal to */
    NE,        /**< not equal than */
    GT,        /**< greater than */
    GE,        /**< greater than or equal to */
    LINK,      /**< join */
    MAX_CM
};

/** definition of request column. */
struct RequestColumn {
    char name[128];    /**< name of column */
    AggrerateMethod aggrerate_method;  /** aggrerate method, could be NONE_AM  */
};
/** definition of Aggrerate column. */
struct AggrerateColumn{
	int id;
	AggrerateMethod aggrerate_method;
};
/** definition of request table. */
struct RequestTable {
    char name[128];    /** name of table */
};

/** definition of compare condition. */
struct Condition {
    RequestColumn column;   /**< which column */
    CompareMethod compare;  /**< which method */
    char value[128];        /**< the value to compare with, if compare==LINK,value is another column's name; else it's the column's value*/
};

/** definition of conditions. */
struct Conditions {
    int condition_num;      /**< number of condition in use */
    Condition condition[4]; /**< support maximum 4 & conditions */
};

/** definition of selectquery.  */
class SelectQuery {
  public:
    int64_t database_id;           /**< database to execute */
    int select_number;             /**< number of column to select */
    RequestColumn select_column[4];/**< columns to select, maximum 4 */
    int from_number;               /**< number of tables to select from */
    RequestTable from_table[4];    /**< tables to select from, maximum 4 */
    Conditions where;              /**< where meets conditions, maximum 4 & conditions */
    int groupby_number;            /**< number of columns to groupby */
    RequestColumn groupby[4];      /**< columns to groupby */
    Conditions having;             /**< groupby conditions */
    int orderby_number;            /**< number of columns to orderby */
    RequestColumn orderby[4];      /**< columns to orderby */
};  // class SelectQuery

/** definition of result table.  */
class ResultTable {
  public:
    int column_number;       /**< columns number that a result row consist of */
    BasicType **column_type; /**< each column data type */
    char *buffer;         /**< pointer of buffer alloced from g_memory */
    int64_t buffer_size;  /**< size of buffer, power of 2 */
    int row_length;       /**< length per result row */
    int row_number;       /**< current usage of rows */
    int row_capicity;     /**< maximum capicity of rows according to buffer size and length of row */
    int *offset;
    int offset_size;

    /**
     * init alloc memory and set initial value
     * @col_types array of column type pointers
     * @col_num   number of columns in this ResultTable
     * @param  capicity buffer_size, power of 2
     * @retval >0  success
     * @retval <=0  failure
     */
    int init(BasicType *col_types[],int col_num,int64_t capicity = 1024);
    /**
     * calculate the char pointer of data spcified by row and column id
     * you should set up column_type,then call init function
     * @param row    row id in result table
     * @param column column id in result table
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    char* getRC(int row, int column);
    /**
     * write data to position row,column
     * @param row    row id in result table
     * @param column column id in result table
     * @data data pointer of a column
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    int writeRC(int row, int column, void *data);
    /**
     * print result table, split by '\t', output a line per row
     * @retval the number of rows printed
     */
    int print(void);
    /**
     * write to file with FILE *fp
     */
    int dump(FILE *fp);
    /**
     * free memory of this result table to g_memory
     */
    int shut(void);
};  // class ResultTable

/** definition of class Operator. */
class Operator {
	public:
		virtual bool	init    ()=0;
		virtual	bool	getNext (ResultTable &ParentTempResult)=0;
		virtual	bool	isEnd   ()=0;
};

/** definition of class Scan Operator. */
class Scan : public Operator{
	private:
        char * TableName;             /**< the table name to scan                   */
	    int TempResultCols;           /**< col number of TempResult                 */

    public:
        /** constrcut method of class Project. */
        Scan(char * TableName,int TempResultCols){
        	this->TableName = TableName;
        	this->TempResultCols = TempResultCols;
        };

        /**
        * init the Operator. Allocate memory and init its child
        * @retval > 0 init success
        * @retval < 0 init failed
        */
        bool init();

        /**
        * get the Next Result of the Operator.
        * @result parent's TempResult table
        * @retval > 0 getNext success
        * @retval < 0 getNext failed
        */
        bool getNext(ResultTable& ParentTempResult);

        /**
        * free the memory and finish the Operator.
        * @retval > 0 isEnd success
        * @retval < 0 isEnd failed
        */
        bool isEnd();
    protected:
        Table * ScanTable;            /**< table pointer to table to scan           */
        int ScanCounter = 0;          /**< scan counter                             */
        char *buffer;              /**< buffer to put temp result                */


};

/** definition of class executor.  */
class Executor {
  private:
    SelectQuery *current_query;  /**< selectquery to iterately execute */
    Operator *last_op;
    int final_col;
    BasicType *final_type[4];

  public:
    /**
     * exec function.
     * @param  query to execute, if NULL, execute query at last time
     * @result result table generated by an execution, store result in pattern defined by the result table
     * @retval >0  number of result rows stored in result
     * @retval <=0 no more result
     */
    virtual int exec(SelectQuery *query, ResultTable &result);
    //--------------------------------------
    //  ...
    //  ...
};


/** definition of class Filter Operator. */
class Filter : public Operator {
	    private:
        int FilterCol;                /**< which Col to Filter                      */
        CompareMethod compare_method; /**< compare method                           */
        char * CompareValue;          /**< compare value                            */
        Operator * ChildOperator;     /**< the child operator of this operator      */
        int TempResultCols;           /**< col number of TempResult                 */
        BasicType ** TempResultType;  /**< datatype of TempResult                   */
    public:
        /** constrcut method of class Project. */
        Filter(int FilterCol,CompareMethod compare_method,
                char * CompareValue,Operator * ChildOperator,
                int TempResultCols,BasicType ** TempResultType){
            this->FilterCol = FilterCol;
            this->compare_method = compare_method;
            this->CompareValue = CompareValue;
            this->ChildOperator = ChildOperator;
            this->TempResultCols = TempResultCols;
            this->TempResultType = TempResultType;
        };

        /**
        * init the Operator. Allocate memory and init its child
        * @retval > 0 init success
        * @retval < 0 init failed
        */
        bool init();

        /**
        * get the Next Result of the Operator.
        * @result parent's TempResult table
        * @retval > 0 getNext success
        * @retval < 0 getNext failed
        */
        bool getNext(ResultTable& ParentTempResult);

        /**
        * free the memory and finish the Operator.
        * @retval > 0 isEnd success
        * @retval < 0 isEnd failed
        */
        bool isEnd();

        bool compareResult(void *col_data,void* value,CompareMethod compare_method, BasicType *compare);

    protected:
        int FilterCounter = 0;        /**< Filter counter                           */
        ResultTable TempResult;       /**< a temporary table to store middle result */

};

/** definition of class Project Operator. */
class Project : public Operator{
    private:
        int ProjectNumber;            /**< total number of cols to project          */
        int * ProjectCol;             /**< id of cols to be projected               */
        Operator * ChildOperator;     /**< the child operator of this operator      */
        int TempResultCols;           /**< col number of TempResult                 */
        BasicType ** TempResultType;  /**< datatype of TempResult                   */
    public:
        /** constrcut method of class Project. */
        Project(int ProjectNumber,int * ProjectCol,Operator * ChildOperator,
                int TempResultCols,BasicType ** TempResultType){
            this->ProjectNumber = ProjectNumber;
            this->ProjectCol = ProjectCol;
            this->ChildOperator = ChildOperator;
            this->TempResultCols = TempResultCols;
            this->TempResultType = TempResultType;
        };

        /**
        * init the Operator. Allocate memory and init its child
        * @retval > 0 init success
        * @retval < 0 init failed
        */
        bool init();

        /**
        * get the Next Result of the Operator.
        * @result parent's TempResult table
        * @retval > 0 getNext success
        * @retval < 0 getNext failed
        */
        bool getNext(ResultTable& ParentTempResult);

        /**
        * free the memory and finish the Operator.
        * @retval > 0 isEnd success
        * @retval < 0 isEnd failed
        */
        bool isEnd();
    protected:
        ResultTable TempResult;       /**< a temporary table to store middle result */
};

/** definetion of class Join Operator. */
class Join : public Operator {
	    private:
        int LJoinCol;                  /**< which col to join in LTable              */
        int RJoinCol;                  /**< which col to join in RTable              */
        Operator * LChildOperator;     /**< the child operator of this operator      */
        Operator * RChildOperator;     /**< the child operator of this operator      */
        int LTempResultCols;           /**< col number of TempResult                 */
        int RTempResultCols;           /**< col number of TempResult                 */
        BasicType ** LTempResultType;  /**< datatype of TempResult                   */
        BasicType ** RTempResultType;  /**< datatype of TempResult                   */
    public:
        /** constrcut method of class Project. */
        Join(int LJoinCol,Operator * LChildOperator,
                int LTempResultCols,BasicType ** LTempResultType,
                int RJoinCol,Operator * RChildOperator,
                int RTempResultCols,BasicType ** RTempResultType){
            this->LJoinCol = LJoinCol;
            this->LChildOperator = LChildOperator;
            this->LTempResultCols = LTempResultCols;
            this->LTempResultType = LTempResultType;
            this->RJoinCol = RJoinCol;
            this->RChildOperator = RChildOperator;
            this->RTempResultCols = RTempResultCols;
            this->RTempResultType = RTempResultType;
        };

        /**
        * init the Operator. Allocate memory and init its child
        * @retval > 0 init success
        * @retval < 0 init failed
        */
        bool init();

        /**
        * get the Next Result of the Operator.
        * @result parent's TempResult table
        * @retval > 0 getNext success
        * @retval < 0 getNext failed
        */
        bool getNext(ResultTable& ParentTempResult);

        /**ss
        * free the memory and finish the Operator.
        * @retval > 0 isEnd success
        * @retval < 0 isEnd failed
        */
        bool isEnd();
    protected:
        ResultTable LTable;            /**< LTable to store all result               */
        ResultTable RTable;            /**< RTable to store all result               */
        int LCounter = 0;              /**< counter                                  */
        int RCounter = 0;              /**< counter                                  */
        ResultTable LTempResult;       /**< a temporary table to store middle result */
        ResultTable RTempResult;       /**< a temporary table to store middle result */
        bool findit = false;


};

/** definition of class GroupBy Operator. */
class GroupBy : public Operator {
	    private:
        int Group_Number;         /**< number of colum need to group by      */
	    int *Group_Col;           /**< id of colum need to be group by   */
	    int aggrerate_Number;	  /**< number need to aggrerate*/
	  	AggrerateColumn *aggrerate_col; /**< id of cololum need to be aggrerate and aggrerate mechod*/
        Operator * ChildOperator;     /**< the child operator of this operator      */
        int TempResultCols;           /**< col number of TempResult                 */
        BasicType ** TempResultType;  /**< datatype of TempResult                   */
    public:
        /** constrcut method of class Project. */
         GroupBy(int Group_Number, int *Group_Col, int aggrerate_Number,AggrerateColumn *aggrerate_col,
                Operator * ChildOperator, int TempResultCols,BasicType ** TempResultType){
            this->Group_Number = Group_Number;
            this->Group_Col = Group_Col;
            this->aggrerate_Number = aggrerate_Number;
            this->aggrerate_col = aggrerate_col;
            this->ChildOperator = ChildOperator;
            this->TempResultCols = TempResultCols;
            this->TempResultType = TempResultType;
        };

        /**
        * init the Operator. Allocate memory and init its child
        * @retval > 0 init success
        * @retval < 0 init failed
        */
        bool init();

        /**
        * get the Next Result of the Operator.
        * @result parent's TempResult table
        * @retval > 0 getNext success
        * @retval < 0 getNext failed
        */
        bool getNext(ResultTable& ParentTempResult);

        /**
        * free the memory and finish the Operator.
        * @retval > 0 isEnd success
        * @retval < 0 isEnd failed
        */
        bool isEnd();


    protected:
    	ResultTable  TempResult;
    	ResultTable  GroupByTable;
    	int GroupTableCounter = 0;
    	int have_count = -1; // have count or not
    	int ReqCounter = 0;
    	void do_count(bool find,int array, char** data);
    	void do_max(bool find,int agg_col, int array, int col);
    	void do_min(bool find,int agg_col, int array, int col);
    	void do_add(bool find,int agg_col, int array, int col);
};



/** definition of class OrderBy Operator. */
class OrderBy : public Operator{
    private:
        int counter;
        int total;
        ResultTable rec[64];
        ResultTable **temp;

        int OrderCol;
        int *orderby_index;
        Operator * ChildOperator;     /**< the child operator of this operator      */
        int TempResultCols;           /**< col number of TempResult                 */
        BasicType ** TempResultType;  /**< datatype of TempResult                   */
    public:
        /** constrcut method of class Project. */
        OrderBy(int OrderCol,int *orderby_index,Operator * ChildOperator,
                int TempResultCols,BasicType ** TempResultType){
            this->OrderCol = OrderCol;
            this->orderby_index = orderby_index;
            this->ChildOperator = ChildOperator;
            this->TempResultCols = TempResultCols;
            this->TempResultType = TempResultType;
        };

        /**
        * init the Operator. Allocate memory and init its child
        * @retval > 0 init success
        * @retval < 0 init failed
        */
        bool init();

        /**
        * get the Next Result of the Operator.
        * @result parent's TempResult table
        * @retval > 0 getNext success
        * @retval < 0 getNext failed
        */
        bool getNext(ResultTable& ParentTempResult);

        /**
        * free the memory and finish the Operator.
        * @retval > 0 isEnd success
        * @retval < 0 isEnd failed
        */
        bool isEnd();

};

bool cmple(ResultTable *a,ResultTable *b,int *index,BasicType **type,int ordercol);
bool cmpless(ResultTable *a,ResultTable *b,int *index,BasicType **type,int ordercol);
int quicksort(ResultTable **temp,int low,int high,int *index,BasicType **type,int ordercol);
#endif
