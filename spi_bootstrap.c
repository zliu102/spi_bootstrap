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

typedef struct state_c
{
	ArrayType *reservoir;
        int32 poscnt;
        int32 reservoir_size; 
} state_c;

#define MAX_GROUPS 787
#define RES_SIZE 100
static void prepTuplestoreResult(FunctionCallInfo fcinfo);


//static bool initialized = false;
//static int lastgroup = -1;
//static state_c **states = NULL;
//static state_c *states_2[MAX_GROUPS];
//static int poscounts[MAX_GROUPS];
//static int64 *results = NULL;
//static int64 results[MAX_GROUPS][RES_SIZE] = {0};


/*
PG_FUNCTION_INFO_V1(res_trans_crimes_spi);
Datum
res_trans_crimes_spi(PG_FUNCTION_ARGS)
{
    
    //bytea  *addr = (bytea *) PG_GETARG_BYTEA_P(0);
    //elog(INFO, "addr is %p",addr);
    int64 lastgroup = PG_GETARG_INT64(0);
    int64 group_index = PG_GETARG_INT64(1);
    int64 newsample = PG_GETARG_INT64(2);
    //elog(INFO, "lastgroup is %ld",lastgroup);
    //elog(INFO, "group_index is %ld",group_index);
    //elog(INFO, "newsample is %ld",newsample);   
    //state_c *s = palloc (sizeof(state_c));
    //if (states == NULL) {
     //   states = (state_c **) palloc0(MAX_GROUPS * sizeof(state_c *));
      //  elog(INFO, "states is %p",states); 
    //}
    if (!initialized) {
        
        for (int i = 0;i<MAX_GROUPS;i++){
            states_2[i]= (state_c *) palloc0(sizeof(state_c));
            poscounts[i] = 1;
            
            
        }
        //elog(INFO, "states_2 is %p",states_2); 
        //elog(INFO, "poscounts is %p",poscounts); 
        initialized = true;
        //elog(INFO, "states_2 is %p",states_2); 
    }

        int poscnt = poscounts[group_index - 1];
        //elog(INFO, "poscnt is %d",poscnt);//0 - postcnt -1
        if(poscnt <= RES_SIZE){
            //elog(INFO, "case 1");
            results[group_index-1][poscnt - 1] = newsample;
            
            poscounts[group_index - 1] ++;
        }else{
           // elog(INFO, "case 2");
            int32 pos = rand() % poscnt;
            //elog(INFO, "pos is %d",pos);//0 - postcnt -1
            if(pos < RES_SIZE){
                results[group_index-1][pos] = newsample;
            }
            poscounts[group_index - 1] ++;
        }
       
        PG_RETURN_INT64(group_index);
}

PG_FUNCTION_INFO_V1(finalize_trans_crimes_spi);
Datum
finalize_trans_crimes_spi(PG_FUNCTION_ARGS)
{               

                ArrayType *result;
                Datum *elems;
                int i;
                //int num;
                //int64 *dr;

                //state_c *st = palloc (sizeof(state_c));
                //bytea  *addr = (bytea *) PG_GETARG_BYTEA_P(0);
                //void **new_ptr = (void **) VARDATA(addr);
                int64 group_index = PG_GETARG_INT64(0);
                //elog(INFO, "group_index is %ld",group_index);
                //st= (state_c *) states[group_index - 1];
                //state_c *st= (state_c *) states_2[group_index - 1];
                //elog(INFO, "inner_array 0,1,2 is %ld,%ld,%ld",results[group_index-1][0],results[group_index-1][1],results[group_index-1][2]);
                //elog(INFO, "st is %p",st);
                //elog(INFO, "st poscnt is %d,reservoir_size is %d",st->poscnt,st->reservoir_size);
                //num = st->reservoir_size;
                //dr = (int64 *) ARR_DATA_PTR(st->reservoir); 
                
                elems = (Datum *)palloc(RES_SIZE * sizeof(Datum));
                
                for (i = 0; i < RES_SIZE; i++) {
                    elems[i] = Int64GetDatum(results[group_index-1][i]); 
                    //elog(INFO, "dr[%d] is %ld",i,dr[i]);
                    //elog(INFO, "elems[%d] is %ld",i,elems[i]);
                }
                //int nbytes = ARR_OVERHEAD_NONULLS(1) + sizeof(int) * num;
                //result = (ArrayType *) palloc0(nbytes);
                
                result = construct_array((Datum *)elems, RES_SIZE , INT8OID, sizeof(int64), true, 'i');
                
                //pfree(addr);
                //initialized = false;
                //elog(INFO, "before retrun initialized is %d",initialized);
                initialized = false;
                PG_RETURN_ARRAYTYPE_P(result);
                //PG_RETURN_ARRAYTYPE_P(st->reservoir); 
}

PG_FUNCTION_INFO_V1(spi_test2);
Datum
spi_test2(PG_FUNCTION_ARGS)
{
    // elog(NOTICE, "range_window: start.");

    int ret;
    int row;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext oldcontext;
   
    prepTuplestoreResult(fcinfo);

    // elog(NOTICE, "range_window: preped result.");

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    if ((ret = SPI_connect()) < 0)
    
        elog(ERROR, "range_window: SPI_connect returned %d", ret);

    // elog(NOTICE, "range_window: SPI connected.");
    char        sql[8192];
   // snprintf(sql, sizeof(sql),"SELECT b, c, a FROM (SELECT b, c, a, ROW_NUMBER() OVER (PARTITION BY b, c ORDER BY random()) AS rn FROM test2) t WHERE rn <= 3 ORDER BY b, c, a;");
    snprintf(sql, sizeof(sql),"SELECT beat, ward, array_agg(zip_codes ORDER BY 1) AS random_zip_codes FROM (SELECT beat, ward, zip_codes, ROW_NUMBER() OVER (PARTITION BY beat, ward ORDER BY random()) AS rn FROM crimes1) t WHERE rn <= 100 GROUP BY beat, ward;");
    // elog(NOTICE, "range_window: SPI query -- %s", sql);

    ret = SPI_execute(sql, true, 0);

    if (ret < 0)
     
        elog(ERROR, "range_window: SPI_exec returned %d", ret);

    // elog(NOTICE, "range_window: number of tuples -- %d", (int)SPI_processed);

    //tuple store
   

   

    //tupdesc = CreateTemplateTupleDesc(1);
    tupdesc = SPI_tuptable->tupdesc;
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "beat", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "ward", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "zip_codes", INT8OID, -1, 0);
    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);



  
    for(row = 0; row < SPI_processed; row++){
        HeapTuple tuple;
        //HeapTuple tuplec;
        //elog(NOTICE, "range_window: row %d", row);
        tuple = SPI_copytuple((SPI_tuptable->vals)[row]);
       //tuplec = SPI_copytuple((SPI_tuptable->vals)[row]);

        tuplestore_puttuple(tupstore, tuple);

        
    }
 
   

   
    tuplestore_donestoring(tupstore);

    SPI_finish();

    PG_RETURN_NULL();
}*/

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

