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
} Group;

typedef struct {
    Group *groups;
    int numGroups;
    int capacity;
} GroupsContext;

// Utility function declarations
static void prepTuplestoreResult(FunctionCallInfo fcinfo);
static Group* findOrCreateGroup(GroupsContext *context, int l_suppkey, int l_returnflag_int);
static void addQuantityToGroup(Group *group, float4 quantity);
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
static Group* findOrCreateGroup(GroupsContext *context, int l_suppkey, int l_returnflag_int) {
    // 搜索现有分组
    for (int i = 0; i < context->numGroups; ++i) {
        if (context->groups[i].l_suppkey == l_suppkey && context->groups[i].l_returnflag_int == l_returnflag_int) {
            return &context->groups[i];
        }
    }

    // 需要创建新分组
    if (context->numGroups >= context->capacity) {
        // 动态扩容
        context->capacity *= 2;
        context->groups = (Group *) repalloc(context->groups, sizeof(Group) * context->capacity);
    }

    Group *newGroup = &context->groups[context->numGroups++];
    newGroup->l_suppkey = l_suppkey;
    newGroup->l_returnflag_int = l_returnflag_int;
    newGroup->quantities = (float4 *) palloc(sizeof(float4) * 100); // 初始容量
    newGroup->count = 0;
    newGroup->capacity = 100;

    return newGroup;
}

// 向分组添加数量
static void addQuantityToGroup(Group *group, float4 quantity) {
    if (group->count >= group->capacity) {
        // 动态扩容
        group->capacity *= 2;
        group->quantities = (float4 *) repalloc(group->quantities, sizeof(float4) * group->capacity);
    }
    group->quantities[group->count++] = quantity;
}

// 从分组中随机抽样并计算平均值
static float4 calculateRandomSampleAverage(float4 *quantities, int count) {
    int sampleSize = 1000 < count ? 1000 : count;
    float4 sum = 0;

    for (int i = 0; i < sampleSize; ++i) {
        int idx = rand() % count; // 注意：对于非常大的数量，这里可能需要更好的随机数生成方法
        sum += quantities[idx];
    }

    return sampleSize > 0 ? sum / sampleSize : 0;
}



PG_FUNCTION_INFO_V1(spi_bootstrap2);

Datum spi_bootstrap2(PG_FUNCTION_ARGS) {
    // Connect to SPI
    if (SPI_connect() != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("SPI_connect failed")));
    }

    // Prepare and execute the SQL query
    char sql[1024];
    snprintf(sql, sizeof(sql), "SELECT l_suppkey, l_returnflag_int, L_QUANTITY FROM your_table ORDER BY l_suppkey, l_returnflag_int");
    int ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT) {
        SPI_finish();
        ereport(ERROR, (errmsg("SPI_execute failed")));
    }

    // Prepare for tuplestore use
    TupleDesc tupdesc = CreateTemplateTupleDesc(3, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_suppkey", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "l_returnflag_int", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "avg_quantity", FLOAT8OID, -1, 0);
    MemoryContext oldcontext = MemoryContextSwitchTo(CurrentMemoryContext);
    Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
    MemoryContextSwitchTo(oldcontext);

    // Initialize GroupsContext
    GroupsContext groupsContext;
    groupsContext.groups = (Group *)palloc(sizeof(Group) * 10); // Initial capacity
    groupsContext.numGroups = 0;
    groupsContext.capacity = 10;

    // Process SPI results
    for (int i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = SPI_tuptable->vals[i];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;

        int l_suppkey = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, NULL));
        int l_returnflag_int = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, NULL));
        int quantity = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, NULL));

        Group *group = findOrCreateGroup(&groupsContext, l_suppkey, l_returnflag_int);
        addQuantityToGroup(group, quantity);
    }

    // Process each group: calculate random sample average and store results
    srand(time(NULL)); // Initialize random seed
    for (int i = 0; i < groupsContext.numGroups; i++) {
        Group *group = &groupsContext.groups[i];
        float8 avg_quantity = calculateRandomSampleAverage(group->quantities, group->count);

        Datum values[3];
        bool nulls[3] = {false, false, false};

        values[0] = Int32GetDatum(group->l_suppkey);
        values[1] = Int32GetDatum(group->l_returnflag_int);
        values[2] = Float8GetDatum(avg_quantity);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    // Cleanup
    SPI_finish();
    tuplestore_donestoring(tupstore);

    // Set up to return the tuplestore as a set
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    rsinfo->returnMode = SFRM_Materialize;

    PG_RETURN_NULL();
}

// Definitions of utility functions...




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

        

        //elog(INFO, "range_window: row %d,current_group %s,last_group %s ",row, current_group,last_group);
        /*
        bool isnull;
        int a = SPI_getbinval((SPI_tuptable->vals)[row], SPI_tuptable->tupdesc, attnum3, &isnull);
        
        a_values[0] = 0;
        a_values[1] = 0;
        Datum values[3];
        bool nulls[3] = { false, false, false };
        elog(INFO, "value: a %d, b %d, c %d",a, b, c);
        values[0] = Int32GetDatum(a);
        values[1] = Int32GetDatum(b);
        values[2] = Int32GetDatum(c);
        HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);*/
        //elog(INFO, "New tuple: %s", heap_tuple_to_string(tuple, tupdesc));
        //HeapTuple tuple = SPI_copytuple((SPI_tuptable->vals)[row]);
        //reservoir[0] = tuple;

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

