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
    char* l_suppkey; 
    char* l_partkey; 
    float4 *quantities;
    float4 *orderkeys;
    float4 *extendedprices;
    //float4 discounts[MAX_QUANTITIES];
    float4 *linenumbers;
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
static int findOrCreateGroup(GroupsContext *context, char* l_suppkey, char* l_partkey);
static void addAttributeToGroup(MyGroup *group, float4 quantity, float4 orderkey, float4 extendedprice,float4 linenumber);
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

static int findOrCreateGroup(GroupsContext *context, char* l_suppkey, char* l_partkey) {
 
    static char* last_l_suppkey = NULL; 
    static char* last_l_partkey = NULL;
    static int last_groupIndex = -1;
    
    // 检查上一个值是否相同（这里使用 strcmp 比较字符串）
    if ((last_l_suppkey != NULL && strcmp(l_suppkey, last_l_suppkey) == 0) &&
        (last_l_partkey != NULL && strcmp(l_partkey, last_l_partkey) == 0)) {
        return last_groupIndex;
    }


    if (context->numGroups >= context->capacity) {
        
        context->capacity *= 2;
        context->groups = (MyGroup *) repalloc(context->groups, sizeof(MyGroup) * context->capacity);
    }

    int newIndex = context->numGroups;

    MyGroup *newGroup = &context->groups[newIndex];
    newGroup->l_suppkey = strdup(l_suppkey);
    newGroup->l_partkey = strdup(l_partkey);
    newGroup->quantities = (float4 *) palloc(sizeof(float4) * 2); 
    newGroup->orderkeys = (float4 *) palloc(sizeof(float4) * 2); 
    newGroup->extendedprices = (float4 *) palloc(sizeof(float4) * 2); 
    newGroup->linenumbers = (float4 *) palloc(sizeof(float4) * 2); 
    newGroup->count = 0;
    newGroup->capacity = 2;
    context->numGroups++;
    return newIndex;
}


static void addAttributeToGroup(MyGroup *group, float4 quantity, float4 orderkey, float4 extendedprice,float4 linenumber) {
    if (group->count >= group->capacity) {
        
        group->capacity *= 2;
        group->quantities = (float4 *) repalloc(group->quantities, sizeof(float4) * group->capacity);
        group->orderkeys = (float4 *) repalloc(group->orderkeys, sizeof(float4) * group->capacity);
        group->extendedprices = (float4 *) repalloc(group->extendedprices, sizeof(float4) * group->capacity);
        group->linenumbers = (float4 *) repalloc(group->linenumbers, sizeof(float4) * group->capacity);
    }
    group->quantities[group->count] = quantity;
    group->orderkeys[group->count] = orderkey;
    group->extendedprices[group->count] = extendedprice;
    group->linenumbers[group->count] = linenumber;
    group->count++;
}


static float4 calculateRandomSampleAverage(float4 *quantities, int count) {
    int sampleSize = 100;
    float4 sum = 0;
    int i;
    for (i = 0; i < sampleSize; ++i) {
        int idx = rand() % count; 
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
    MemoryContext per_query_ctx;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
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
    //prepTuplestoreResult(fcinfo);
    
    //oldcontext = MemoryContextSwitchTo(CurrentMemoryContext);
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    MemoryContextSwitchTo(oldcontext); //test


    snprintf(sql, sizeof(sql), "select * from reservoir_sampler_tpch(%s,'%s','%s','%s');",sampleSize,tablename,otherAttribue,groupby);
    elog(INFO, "SPI query -- %s", sql);
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT) {
        SPI_finish();
        ereport(ERROR, (errmsg("SPI_execute failed")));
    }

    // Prepare for tuplestore use
    tupdesc = CreateTemplateTupleDesc(6, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "l_suppkey", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "l_partkey", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "avg_l_quantity", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 4, "std_l_quantity", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 3, "avg_l_partkey", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 4, "std_l_partkey", FLOAT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "avg_l_orderkey", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 8, "std_l_orderkey", FLOAT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "avg_l_extendedprice", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 6, "std_l_extendedprice", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 6, "avg_l_discount", FLOAT4OID, -1, 0);
    //TupleDescInitEntry(tupdesc, (AttrNumber) 12, "std_l_discount", FLOAT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "avg_l_linenumber", FLOAT4OID, -1, 0);

    
    

    // Initialize GroupsContext
    GroupsContext groupsContext;
    groupsContext.groups = (MyGroup *)palloc(sizeof(MyGroup) * 799541); // problem 1Initial capacity
    groupsContext.numGroups = 0;
    groupsContext.capacity = 799541;

    // Process SPI results
   
    for (i = 0; i < SPI_processed; i++) {
        //HeapTuple tuple = SPI_tuptable->vals[i];
        //TupleDesc tupdesc = SPI_tuptable->tupdesc;
        //elog(INFO, "SPI current id is -- %d", i);

       int attnum1 = SPI_fnumber(SPI_tuptable->tupdesc, "l_suppkey");
        int attnum2 = SPI_fnumber(SPI_tuptable->tupdesc, "l_partkey");
        int attnum3 = SPI_fnumber(SPI_tuptable->tupdesc, "l_quantity");
        //int attnum4 = SPI_fnumber(SPI_tuptable->tupdesc, "l_partkey");
        int attnum5 = SPI_fnumber(SPI_tuptable->tupdesc, "l_orderkey");
        int attnum6 = SPI_fnumber(SPI_tuptable->tupdesc, "l_extendedprice");
        //int attnum7 = SPI_fnumber(SPI_tuptable->tupdesc, "l_discount");
        int attnum8 = SPI_fnumber(SPI_tuptable->tupdesc, "l_linenumber");

        char* value1 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum1);
        char* value2 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum2);
        char* value3 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum3);
        //char* value4 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum4);
        char* value5 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum5);
        char* value6 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum6);
        //char* value7 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum7);
        char* value8 = SPI_getvalue((SPI_tuptable->vals)[i], SPI_tuptable->tupdesc, attnum8);

        //int l_suppkey = atoi(value1);
        //int l_returnflag_int = atoi(value2);
        //double l_tax = strtod(value2, NULL); 
        int quantity = atoi(value3);
        //int partkey = atoi(value3);
        int orderkey = atoi(value5);
        int extendedprice = atoi(value6);
        //double discount = strtod(value7, NULL);
        int linenumber = atoi(value8);
        //elog(INFO, "l_suppkey is %d",atoi(value1));
        //elog(INFO, "l_linenumber is %d",atoi(value2));
      
        int groupIndex = findOrCreateGroup(&groupsContext, value1, value2);
        if (groupIndex != -1) { 
            addAttributeToGroup(&groupsContext.groups[groupIndex],quantity,orderkey,extendedprice,linenumber);
            //addAttributeToGroup(&groupsContext.groups[groupIndex],quantity);
        }

        //elog(INFO, "group l_suppkey is %d", group->l_suppkey);
        //elog(INFO, "group l_returnflag_int is %d", group->l_returnflag_int); 
    }
    //elog(INFO, "Finish adding");
    // Process each group: calculate random sample average and store results
    srand(time(NULL)); // Initialize random seed
    int j;
    for (j = 0; j < groupsContext.numGroups; j++) {
        //elog(INFO, "SPI j is -- %d", j);
        
        MyGroup *group = &groupsContext.groups[j];
        
        float4 avg_l_quantity = calculateRandomSampleAverage(group->quantities, group->count);
        //float4 stddev_l_quantity = calculateStandardDeviation(group->quantities, group->count, avg_l_quantity);
        //float4 avg_l_partkey = calculateRandomSampleAverage(group->partkeys, group->count);
        //float4 stddev_l_partkey = calculateStandardDeviation(group->partkeys, group->count, avg_l_partkey);
        float4 avg_l_orderkey = calculateRandomSampleAverage(group->orderkeys, group->count);
        //float4 stddev_l_orderkey = calculateStandardDeviation(group->orderkeys, group->count, avg_l_orderkey);
        float4 avg_l_extendedprice = calculateRandomSampleAverage(group->extendedprices, group->count);
        //float4 stddev_l_extendedprice = calculateStandardDeviation(group->extendedprices, group->count, avg_l_extendedprice);
        //float4 avg_l_discount = calculateRandomSampleAverage(group->discounts, group->count);
        //float4 stddev_l_discount = calculateStandardDeviation(group->discounts, group->count, avg_l_discount);
        float4 avg_l_linenumber = calculateRandomSampleAverage(group->linenumbers, group->count);
        //float4 stddev_l_linenumber = calculateStandardDeviation(group->linenumbers, group->count, avg_l_linenumber);
        

        Datum values[6];
        bool nulls[6] = {false, false, false, false,false, false};
        //elog(INFO, "l_suppkey 0 is %s", group->l_suppkey);
        //elog(INFO, "l_linenumber 0 is %s", group->l_linenumber);
        //values[0] = Int32GetDatum(group->l_suppkey);
        //values[1] = DirectFunctionCall1(float8_numeric, Float8GetDatum(group->l_tax));
        values[0] = Int32GetDatum(atoi(group->l_suppkey));
        //values[1] = DirectFunctionCall3(numeric_in, CStringGetDatum(group->l_discount), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        values[1] = Int32GetDatum(atoi(group->l_partkey));
        values[2] = Float4GetDatum(avg_l_quantity);
        //values[3] = Float4GetDatum(stddev_l_quantity);
        //values[2] = Float4GetDatum(avg_l_partkey);
        //values[3] = Float4GetDatum(stddev_l_partkey);
        values[3] = Float4GetDatum(avg_l_orderkey);
        //values[7] = Float4GetDatum(stddev_l_orderkey);
        values[4] = Float4GetDatum(avg_l_extendedprice);
        //values[5] = Float4GetDatum(stddev_l_extendedprice);
        //values[5] = Float4GetDatum(avg_l_discount);
        //values[11] = Float4GetDatum(stddev_l_discount);
        values[5] = Float4GetDatum(avg_l_linenumber);
        //values[13] = Float4GetDatum(stddev_l_linenumber);
        //elog(INFO, "l_suppkey is %d", values[0]);
        //elog(INFO, "l_linenumber is %d", values[1]);
        

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
        
    }
    /*
    Datum values[3]; 
    bool nulls[3] = {false, false, false}; 

        
    values[0] = Int32GetDatum(1); 
    values[1] = Int32GetDatum(2); 
    //values[2] = Int32GetDatum(3);
    values[2] = Float4GetDatum(3.14); 
    elog(INFO, "here");
    elog(INFO, "l_suppkey is %d",values[0]);
    elog(INFO, "l_returnflag_int is %d",values[1]);
    elog(INFO, "avg_l_quantity is %f",3.14);
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);*/
    //HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    //tuplestore_puttuple(tupstore, tuple);

    tuplestore_donestoring(tupstore);
    // Cleanup

    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    rsinfo->returnMode = SFRM_Materialize;
    
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
