/*-------------------------------------------------------------------------
 *
 * binary_search.c
 *	  PostgreSQL type definitions for BINARY_SEARCHs
 *
 * Author:	Boris Glavic
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/binary_search/binary_search.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "spi_bootstrap.h"

#include "c.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"
#include "utils/varbit.h"
#include "postgres.h"
#include <limits.h>
#include "catalog/pg_type.h"
#include <ctype.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"


#include "access/heapam.h"
#include "utils/typcache.h"
#include <ctype.h>
#include "executor/spi.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "miscadmin.h"
#include "access/printtup.h"
//#include "/home/oracle/datasets/postgres11ps/postgres-pbds/contrib/intarray/_int.h"

PG_MODULE_MAGIC;

typedef struct {
    int l_suppkey;
    int l_returnflag_int;
    float4 *quantities;
    int count;
    int capacity;
} MyGroup;

typedef struct {
    MyGroup *groups;
    int numGroups;
    int capacity;
} GroupsContext;

// Utility function declarations
static void prepTuplestoreResult(FunctionCallInfo fcinfo);
static MyGroup* findOrCreateGroup(GroupsContext *context, int l_suppkey, int l_returnflag_int);
static void addQuantityToGroup(MyGroup *group, float4 quantity);
static float4 calculateRandomSampleAverage(float4 *quantities, int count);


static void
prepTuplestoreResult(FunctionCallInfo fcinfo)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    /* check to see if query supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not allowed in this context")));

    /* let the executor know we're sending back a tuplestore */
    rsinfo->returnMode = SFRM_Materialize;

    /* caller must fill these to return a non-empty result */
    rsinfo->setResult = NULL;
    rsinfo->setDesc = NULL;
}
// 寻找或创建新的分组
static MyGroup* findOrCreateGroup(GroupsContext *context, int l_suppkey, int l_returnflag_int) {
    // 搜索现有分组
    int i;
    for (i = 0; i < context->numGroups; ++i) {
        if (context->groups[i].l_suppkey == l_suppkey && context->groups[i].l_returnflag_int == l_returnflag_int) {
            return &context->groups[i];
        }
    }

    // 需要创建新分组
    if (context->numGroups >= context->capacity) {
        // 动态扩容
        context->capacity *= 2;
        context->groups = (MyGroup *) repalloc(context->groups, sizeof(MyGroup) * context->capacity);
    }

    MyGroup *newGroup = &context->groups[context->numGroups++];
    newGroup->l_suppkey = l_suppkey;
    newGroup->l_returnflag_int = l_returnflag_int;
    newGroup->quantities = (float4 *) palloc(sizeof(float4) * 100); // 初始容量
    newGroup->count = 0;
    newGroup->capacity = 100;

    return newGroup;
}

// 向分组添加数量
static void addQuantityToGroup(MyGroup *group, float4 quantity) {
    if (group->count >= group->capacity) {
        // 动态扩容
        group->capacity *= 2;
        group->quantities = (float4 *) repalloc(group->quantities, sizeof(float4) * group->capacity);
    }
    group->quantities[group->count++] = quantity;
}

// 从分组中随机抽样并计算平均值
static float4 calculateRandomSampleAverage(float4 *quantities, int count) {
    int sampleSize = 1000;
    float4 sum = 0;
    int i;
    for (i = 0; i < sampleSize; ++i) {
        int idx = rand() % count; // 注意：对于非常大的数量，这里可能需要更好的随机数生成方法
        sum += quantities[idx];
    }
    return sum / sampleSize;
}



PG_FUNCTION_INFO_V1(spi_bootstrap2);

Datum spi_bootstrap2(PG_FUNCTION_ARGS) {
    int ret;
    int i;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext oldcontext;

    // Connect to SPI
    if (SPI_connect() != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("SPI_connect failed")));
    }

    // Prepare and execute the SQL query
    char sql[1024];
    char* sampleSize = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* tablename = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* otherAttribue = text_to_cstring(PG_GETARG_TEXT_PP(2));
    char* groupby = text_to_cstring(PG_GETARG_TEXT_PP(3));
    prepTuplestoreResult(fcinfo);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    snprintf(sql, sizeof(sql), "select * from reservoir_sampler_tpch(%s,'%s','%s','%s');",sampleSize,tablename,otherAttribue,groupby);
    elog(INFO, "SPI query -- %s", sql);
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT) {
        SPI_finish();
        ereport(ERROR, (errmsg("SPI_execute failed")));
    }

    // Prepare for tuplestore use
    tupdesc = CreateTemplateTupleDesc(3, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_suppkey", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "l_returnflag_int", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "avg_l_quantity", FLOAT4OID, -1, 0);
    oldcontext = MemoryContextSwitchTo(CurrentMemoryContext);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    rsinfo->returnMode = SFRM_Materialize;
    MemoryContextSwitchTo(oldcontext);
    tupdesc = BlessTupleDesc(tupdesc);

    // Initialize GroupsContext
    GroupsContext groupsContext;
    groupsContext.groups = (MyGroup *)palloc(sizeof(MyGroup) * 30000); // Initial capacity
    groupsContext.numGroups = 0;
    groupsContext.capacity = 30000;

    // Process SPI results
   
    for (i = 0; i < SPI_processed; i++) {
        //HeapTuple tuple = SPI_tuptable->vals[i];
        //TupleDesc tupdesc = SPI_tuptable->tupdesc;
        //elog(INFO, "SPI current id is -- %d", i);

        int attnum1 = SPI_fnumber(SPI_tuptable->tupdesc, "l_suppkey");
        int attnum2 = SPI_fnumber(SPI_tuptable->tupdesc, "l_returnflag_int");
        int attnum3 = SPI_fnumber(SPI_tuptable->tupdesc, "l_quantity");
        char* value1 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum1);
        char* value2 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum2);
        char* value3 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum3);
        //elog(INFO, "SPI l_suppkey -- %d", attnum1);
        //elog(INFO, "SPI l_returnflag_int -- %d", attnum2);
        //elog(INFO, "SPI quantity -- %d", attnum3);
        //elog(INFO, "SPI l_suppkey -- %s", value1);
        //elog(INFO, "SPI l_returnflag_int -- %s", value2);
        //elog(INFO, "SPI quantity -- %s", value3);
        int l_suppkey = atoi(value1);
        int l_returnflag_int = atoi(value2);
        int quantity = atoi(value3);

        //int l_suppkey = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, NULL));
        //int l_returnflag_int = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, NULL));
        //int quantity = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, NULL));
        //elog(INFO, "SPI l_suppkey -- %d", l_suppkey);
        //elog(INFO, "SPI l_returnflag_int -- %d", l_returnflag_int);
        //elog(INFO, "SPI quantity -- %d", quantity);
      


        MyGroup *group = findOrCreateGroup(&groupsContext, l_suppkey, l_returnflag_int);
        addQuantityToGroup(group, quantity);

        //elog(INFO, "group l_suppkey is %d", group->l_suppkey);
        //elog(INFO, "group l_returnflag_int is %d", group->l_returnflag_int); 
    }
    elog(INFO, "Finish adding");
    // Process each group: calculate random sample average and store results
    srand(time(NULL)); // Initialize random seed
    int j;
    for (j = 0; j < groupsContext.numGroups; j++) {
        elog(INFO, "SPI j is -- %d", j);
        /*MyGroup *group = &groupsContext.groups[j];
        
        float4 avg_l_quantity = calculateRandomSampleAverage(group->quantities, group->count);

        Datum values[3];
        bool nulls[3] = {false, false, false};

        values[0] = Int32GetDatum(group->l_suppkey);
        values[1] = Int32GetDatum(group->l_returnflag_int);
        values[2] = Float4GetDatum(avg_l_quantity);
        //values[0] = group->l_suppkey;
        //values[1] = group->l_returnflag_int;
        //values[2] = avg_quantity;
        elog(INFO, "l_suppkey is %d",values[0]);
        elog(INFO, "l_returnflag_int is %d",values[1]);
        elog(INFO, "avg_l_quantity is %f",avg_l_quantity);
        */
        

        //tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    Datum values[3]; // 假设我们的tuplestore预期有三列
    bool nulls[3] = {false, false, false}; // 假设这些列的值都不是NULL

        // 为简化，我们使用固定的值填充这一行数据
    values[0] = Int32GetDatum(1); // 假设第一列是int4类型，并设其值为1
    values[1] = Int32GetDatum(2); // 假设第二列是int4类型，并设其值为2
    values[2] = Float4GetDatum(3.14); // 假设第三列是float4类型，并设其值为3.14
    elog(INFO, "here");
    elog(INFO, "l_suppkey is %d",values[0]);
    elog(INFO, "l_returnflag_int is %d",values[1]);
    elog(INFO, "avg_l_quantity is %f",3.14);
    //tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    tuplestore_puttuple(tupstore, tuple);

    tuplestore_donestoring(tupstore);
    // Cleanup
    SPI_finish();

    PG_RETURN_NULL();
}

// Definitions of utility functions...



/*
PG_FUNCTION_INFO_V1(spi_bootstrap);
Datum
spi_bootstrap(PG_FUNCTION_ARGS)
{
    int ret;
    int row;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext oldcontext;
   
  
    int64 sampleSize = PG_GETARG_INT64(0);
    //int64 groupby_id = PG_GETARG_INT64(1);// ARRAY LATER
    char* tablename = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* otherAttribue = text_to_cstring(PG_GETARG_TEXT_PP(2));
    char* groupby = text_to_cstring(PG_GETARG_TEXT_PP(3));
    prepTuplestoreResult(fcinfo);

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    if ((ret = SPI_connect()) < 0)
    
        elog(ERROR, "range_window: SPI_connect returned %d", ret);

    // elog(NOTICE, "range_window: SPI connected.");
    char    sql[8192];
    //snprintf(sql, sizeof(sql),"SELECT b, c, a FROM (SELECT b, c, a, ROW_NUMBER() OVER (PARTITION BY b, c ORDER BY random()) AS rn FROM test2) t WHERE rn <= 3 ORDER BY b, c, a;");


    //snprintf(sql, sizeof(sql),"select %s,%s from %s order by %s",groupby,otherAttribue, tablename,groupby); //generate string from array
    snprintf(sql, sizeof(sql),"select %s,%s from %s",groupby,otherAttribue, tablename);
    //elog(INFO, "SPI query -- %s", sql);

    ret = SPI_execute(sql, true, 0);

    if (ret < 0)
     
        elog(ERROR, "range_window: SPI_exec returned %d", ret);


    //tupdesc = SPI_tuptable->tupdesc;
    tupdesc = CreateTemplateTupleDesc(3, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_suppkey", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_returnflag_int", INT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_tax", NUMERICOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_quantity", INT4OID, -1, 0);
    // TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_partkey", INT4OID, -1, 0);
    // TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_orderkey", INT4OID, -1, 0);
    // TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_extendedprice", INT4OID, -1, 0);
    // TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_discount", NUMERICOID, -1, 0);
    // TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_linenumber", INT4OID, -1, 0);
    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);
    tupdesc = BlessTupleDesc(tupdesc);

    HeapTuple reservoir[sampleSize];
    char *last_group = ""; 
    int poscnt;
    bool initialized = false;
    for(row = 0; row < SPI_processed; row++){
     
        int attnum1 = SPI_fnumber(SPI_tuptable->tupdesc, "l_suppkey");
        int attnum2 = SPI_fnumber(SPI_tuptable->tupdesc, "l_returnflag_int");
        char* value1 = SPI_getvalue((SPI_tuptable->vals)[row], SPI_tuptable->tupdesc, attnum1);
        char* value2 = SPI_getvalue((SPI_tuptable->vals)[row], SPI_tuptable->tupdesc, attnum2);
        char *current_group = strcat(value1, ",");
            current_group = strcat(current_group,value2);
        
        
        if(strcmp(current_group, last_group)){
            //check whether is different groups
            //elog(INFO, "different group");
            last_group = current_group;
            poscnt = 0;
            int i;
            if(initialized){
                for (i = 0;i<sampleSize;i++){
                    if(reservoir[i]!= 0){
                        tuplestore_puttuple(tupstore, reservoir[i]);
                    } else {
                        break;
                    }
                }
            } //if not the first group, store the reservior and renew the reservoir 
            memset(reservoir, 0, sizeof(HeapTuple) * sampleSize);
            initialized = true;
        } 
        if (poscnt < sampleSize) {
                //HeapTuple tuple = SPI_copytuple((SPI_tuptable->vals)[row]);
                HeapTuple tuple = SPI_tuptable->vals[row];
                reservoir[poscnt] = tuple;
                poscnt ++;
        } else {
            int pos = rand() % (poscnt+1);
            if (pos < sampleSize){
                //HeapTuple tuple = SPI_copytuple((SPI_tuptable->vals)[row]);
                HeapTuple tuple = SPI_tuptable->vals[row];
                reservoir[pos] = tuple; 
            }
            poscnt ++;
        }

        


    }
    int j;
    for (j = 0;j<sampleSize;j++){
        if(reservoir[j]!= 0){
            tuplestore_puttuple(tupstore, reservoir[j]);
        } else {                
            break;
        }
    } //last group
    //tuplestore_puttuple(tupstore, reservoir[0]);
   

   
    tuplestore_donestoring(tupstore);

    SPI_finish();
    //PG_RETURN_ARRAYTYPE_P(a_values);
    PG_RETURN_NULL();
}
*/
