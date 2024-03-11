/* 
CREATE FUNCTION res_trans_crimes_spi(bigint, bigint, bigint)
        RETURNS bigint
        AS 'MODULE_PATHNAME', 'res_trans_crimes_spi'
        LANGUAGE C
        IMMUTABLE 
        PARALLEL SAFE;

CREATE FUNCTION finalize_trans_crimes_spi(bigint)
        RETURNS bigint[]
        AS 'MODULE_PATHNAME','finalize_trans_crimes_spi'
        LANGUAGE C
        IMMUTABLE 
        PARALLEL SAFE;

CREATE AGGREGATE reservoir_sampling_spi(bigint,bigint)
(
        sfunc = res_trans_crimes_spi,
        stype = bigint,
        FINALFUNC = finalize_trans_crimes_spi,
        INITCOND=-1
);

CREATE FUNCTION spi_test2() 
RETURNS TABLE (beat bigint, ward bigint,zip_codes bigint[]) 
AS 'MODULE_PATHNAME','spi_test2' 
LANGUAGE C STRICT;
*/



/*
CREATE FUNCTION reservoir_sampler_tpch(sampleSize bigint, tablename text, otherAttribue text,groupby text) 
RETURNS TABLE (l_suppkey int, l_returnflag_int int, l_tax numeric, l_quantity int, l_partkey int, l_orderkey int, l_extendedprice int, l_discount numeric, l_linenumber int) 
AS 'MODULE_PATHNAME','reservoir_sampler_tpch' 
LANGUAGE C STRICT;
*/
/*
CREATE FUNCTION spi_bootstrap(sampleSize bigint, tablename text, otherAttribue text,groupby text) 
RETURNS TABLE (l_suppkey int, l_returnflag_int int, l_quantity int) 
AS 'MODULE_PATHNAME','spi_bootstrap' 
LANGUAGE C STRICT;
*/

CREATE FUNCTION spi_bootstrap2(sampleSize text, tablename text, otherAttribue text,groupby text) 
RETURNS TABLE (l_suppkey int32, l_returnflag_int int32, avg_l_quantity float4) 
AS 'MODULE_PATHNAME','spi_bootstrap2' 
LANGUAGE C STRICT;

/*


CREATE FUNCTION res_tras_crimes2_c(Datum, int64)
        RETURNS Datum
        AS 'MODULE_PATHNAME', 'res_tras_crimes'
        LANGUAGE C
        IMMUTABLE 
        PARALLEL SAFE;

CREATE FUNCTION finalize_trans_crimes2_c(Datum)
        RETURNS ArrayType
        AS 'MODULE_PATHNAME'
        LANGUAGE C
        IMMUTABLE 
        PARALLEL SAFE;

CREATE AGGREGATE reservoir_sampling2_c(int64)
(
        sfunc = res_tras_crimes2_c,
        stype = state_c,
        FINALFUNC = finalize_trans_crimes2_c,
);*/