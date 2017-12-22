/**
 * @file    runaimdb.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * the main entrance of AIMDB 
 *
 */

#include "global.h"
#include "executor.h"

#include <stdio.h>
#include <stdlib.h>
#include <vector>

const char *table_name[] = {
    "example_table_1",
    "example_table_2"
};

int print_flag = false;
int load_schema(const char *filename);
int load_data(const char *tablename[],const char *data_dir, int number);
int test(void);

int main(int argc, char *argv[])
{
    if (argc < 3) {
        printf ("usage- ./runaimdb schema_file data_dir [-v]\n");
        printf ("note-  option -v: print more infomation\n");
        return 0;
    }
    if (argc == 4 && !strcmp(argv[3], "-v")) {
        print_flag = true;
    }
    if (global_init()) {
        printf ("[runaimdb][ERROR][main]: global init error!\n");
        return -1;
    }
    if (load_schema(argv[1])) {
        printf ("[runaimdb][ERROR][main]: load schema error!\n");
        return -2;
    }
    if (load_data(table_name,argv[2],2)) {
        printf ("[runaimdb][ERROR][main]: load data error!\n");
        return -3;
    }
    if (print_flag)
        printf ("start test!\n");

    // here start to test your code by call executor
    test();

    global_shut();
    if (print_flag)
        printf ("finish all test!\n");

    return 0;

}

int test(void)
{
    // here we run some test with random data
    Executor executor;
    //------------------------------------------------------------------------
   
    /*
       example query:
       select example_column_3,example_column_5,example_column_14
       from example_table_1,exampel_table_2
       where example_column_4 == example_column_12
     */
    SelectQuery query = { 1,  // database_id
                          3,  // select_number
                          {   // select_column
                            { "example_column_3", NONE_AM  },
                            { "example_column_5", NONE_AM  },
                            { "example_column_14",NONE_AM  },
                            {}   
                          },
                          2,  // from_number
                          {   // from_table
                              "example_table_1","example_table_2","",""
                          },
                          {     // where
                            1,
                            {   //conditions 
                              { {"example_column_4",NONE_AM},LINK,"example_column_12"},
                              {},
                              {},
                              {}
                            }
                          },
                          0,  // groupby_number
                          {}, // greoup_by
                          {}, // having
                          0,  // orderby_number
                          {}  // orderby
                        };
    ResultTable result;
    int stat = executor.exec(&query, &result);
    while (stat > 0) {
        result.print ();
        stat = executor.exec(NULL, &result);
    }
    
    return 0;
}

//----------------------------------------------------------------------------
/**
 * load a database schema from a txt file.
 * @param filename name of schema file, the schema must meet the folowing condition
 * @retval 0  success
 * #retval <0 faliure 
 *
 * schema format 
 * (1) split by one '\t', a row ends with '\n'
 * (2) claim Database,Table,column,index in order
 * (3) no empty row
**/
int load_schema(const char *filename)
{
    int64_t cur_db_id = -1;
    int64_t cur_tb_id;
    int64_t cur_col_id;
    int64_t cur_ix_id;
    Database *cur_db_ptr = NULL;
    Table *cur_tb_ptr = NULL;
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("[load_schema][ERROR]: filename error!\n");
        return -1;
    }
    char buffer[1024];
    while (fgets(buffer, 1024, fp)) {
        int pos = 0, num = 0;
        char *row[16];
        char *ptr = NULL;
        while (buffer[pos] == '\t')
            pos++;
        ptr = buffer + pos;
        while (num < 16) {
            while (buffer[pos] != '\t' && buffer[pos] != '\n')
                pos++;
            row[num++] = ptr;
            ptr = buffer + pos + 1;
            if (buffer[pos] == '\n') {
                buffer[pos] = '\0';
                break;
            }
            buffer[pos] = '\0';
            pos++;
        }
        /*
           //  debug
           for (int ii=0; ii< num; ii++)
           printf ("%s\t", row[ii]);
           printf ("\n");
         */
        if (num >= 16) {
            printf("[load_schema][ERROR]: row with too many field!\n");
            return -2;
        }
        if (!strcmp(row[0], "DATABASE")) {
            if (cur_db_id != -1)
                g_catalog.initDatabase(cur_db_id);
            g_catalog.createDatabase((const char *) row[1], cur_db_id);
            cur_db_ptr = (Database *) g_catalog.getObjById(cur_db_id);
        } else if (!strcmp(row[0], "TABLE")) {
            TableType type;
            if (!strcmp(row[2], "ROWTABLE"))
                type = ROWTABLE;
            else if (!strcmp(row[2], "COLTABLE"))
                type = COLTABLE;
            else {
                printf("[load_schema][ERROR]: table type error!\n");
                return -4;
            }
            g_catalog.createTable((const char *) row[1], type, cur_tb_id);
            cur_tb_ptr = (Table *) g_catalog.getObjById(cur_tb_id);
            cur_db_ptr->addTable(cur_tb_id);
        } else if (!strcmp(row[0], "COLUMN")) {
            ColumnType type;
            int64_t len = 0;
            if (!strcmp(row[2], "INT8"))
                type = INT8;
            else if (!strcmp(row[2], "INT16"))
                type = INT16;
            else if (!strcmp(row[2], "INT32"))
                type = INT32;
            else if (!strcmp(row[2], "INT64"))
                type = INT64;
            else if (!strcmp(row[2], "FLOAT32"))
                type = FLOAT32;
            else if (!strcmp(row[2], "FLOAT64"))
                type = FLOAT64;
            else if (!strcmp(row[2], "DATE"))
                type = DATE;
            else if (!strcmp(row[2], "TIME"))
                type = TIME;
            else if (!strcmp(row[2], "DATETIME"))
                type = DATETIME;
            else if (!strcmp(row[2], "CHARN")) {
                type = CHARN;
                len = atoi(row[3]);
            } else {
                printf("[load_schema][ERROR]: column type error!\n");
                return -5;
            }
            g_catalog.createColumn((const char *) row[1], type, len,
                                   cur_col_id);
            cur_tb_ptr->addColumn(cur_col_id);
        } else if (!strcmp(row[0], "INDEX")) {
            IndexType type = INVID_I;
            std::vector < int64_t > cols;
            if (!strcmp(row[2], "HASHINDEX"))
                type = HASHINDEX;
            else if (!strcmp(row[2], "BPTREEINDEX"))
                type = BPTREEINDEX;
            else if (!strcmp(row[2], "ARTTREEINDEX"))
                type = ARTTREEINDEX;
            for (int ii = 3; ii < num; ii++) {
                int64_t oid = g_catalog.getObjByName(row[ii])->getOid();
                cols.push_back(oid);
            }
            Key key;
            key.set(cols);
            g_catalog.createIndex(row[1], type, key, cur_ix_id);
            cur_tb_ptr->addIndex(cur_ix_id);
        } else {
            printf("[load_schema][ERROR]: o_type error!\n");
            return -3;
        }
    }
    g_catalog.initDatabase(cur_db_id);
    if (print_flag)
        g_catalog.print();
    return 0;
}

/**
 * load table data from txt files
 * @param tablename names of tables
 * @param data_dir the dir of data files corresponding to the table, end with '/'
 * @param number number of tables to load data
 * @retval 0  success
 * @retval <0 faliure
 *
 * naming rule of data files
 * 
 * each data file should be named "table_name.tab" and meet the following requirement
 *
 * data format
 *
 * (1) split by one '\t', a row ends with '\n'
 * (2) no empty row
 *
**/
int load_data(const char *tablename[],const char *data_dir, int number)
{
    for (int ii = 0; ii < number; ii++) {
        char filename[1024];
        strcpy (filename, data_dir);
        strcat (filename, tablename[ii]);
        strcat (filename, ".tab");
        FILE *fp = fopen(filename, "r");
        if (fp == NULL) {
            printf("[load_data][ERROR]: filename error!\n");
            return -1;
        }
        Table *tp =
            (Table *) g_catalog.getObjByName((char *) tablename[ii]);
        if (tp == NULL) {
            printf("[load_data][ERROR]: tablename error!\n");
            return -2;
        }
        int colnum = tp->getColumns().size();
        BasicType *dtype[colnum];
        for (int ii = 0; ii < colnum; ii++)
            dtype[ii] =
                ((Column *) g_catalog.getObjById(tp->getColumns()[ii]))->
                getDataType();
        int indexnum = tp->getIndexs().size();
        Index *index[indexnum];
        for (int ii = 0; ii < indexnum; ii++)
            index[ii] =
                (Index *) g_catalog.getObjById(tp->getIndexs()[ii]);

        char buffer[2048];
        char *columns[colnum];
        char data[colnum][1024];
        while (fgets(buffer, 2048, fp)) {
            // insert table
            columns[0] = buffer;
            int64_t pos = 0;
            for (int64_t ii = 1; ii < colnum; ii++) {
                for (int64_t jj = 0; jj < 2048; jj++) {
                    if (buffer[pos] == '\t') {
                        buffer[pos] = '\0';
                        columns[ii] = buffer + pos + 1;
                        pos++;
                        break;
                    } else
                        pos++;
                }
            }
            /*
               // debug
               for (int ii= 0; ii< colnum; ii++) {
               printf ("%s\t", columns[ii]);
               }
               printf ("\n");
             */
            while (buffer[pos] != '\n')
                pos++;
            buffer[pos] = '\0';
            for (int64_t ii = 0; ii < colnum; ii++) {
                BasicType *p = dtype[ii];
                p->formatBin(data[ii], columns[ii]);
                columns[ii] = data[ii];
            }
            tp->insert(columns);
            void *ptr = tp->getRecordPtr(tp->getRecordNum() - 1);
            // insert index
            for (int ii = 0; ii < indexnum; ii++) {
                int indexsz = index[ii]->getIKey().getKey().size();
                for (int jj = 0; jj < indexsz; jj++)
                    columns[jj] =
                        data[tp->getColumnRank
                             (index[ii]->getIKey().getKey()[jj])];
                index[ii]->insert(columns, ptr);
            }
        }
        if (print_flag)
            tp->printData();
    }
    return 0;
}
