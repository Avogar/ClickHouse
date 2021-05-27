#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_maps"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
SET allow_experimental_map_type = 1;
CREATE TABLE parquet_maps (m1 Map(UInt32, UInt32), m2 Map(String, String), m3 Map(UInt32, Tuple(UInt32, UInt32)), m4 Map(UInt32, Array(UInt32)), m5 Array(Map(UInt32, UInt32)), m6 Tuple(Map(UInt32, UInt32), Map(String, String)), m7 Array(Map(UInt32, Array(Tuple(Map(UInt32, UInt32), Tuple(UInt32)))))) ENGINE=Memory();
EOF

${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_maps VALUES ({1 : 2, 2 : 3}, {'1' : 'a', '2' : 'b'}, {1 : (1, 2), 2 : (3, 4)}, {1 : [1, 2], 2 : [3, 4]}, [{1 : 2, 2 : 3}, {3 : 4, 4 : 5}], ({1 : 2, 2 : 3}, {'a' : 'b', 'c' : 'd'}), [{1 : [({1 : 2}, (1)), ({2 : 3}, (2))]}, {2 : [({3 : 4}, (3)), ({4 : 5}, (4))]}])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_maps FORMAT Parquet" > "${CLICKHOUSE_TMP}"/maps.parquet

cat "${CLICKHOUSE_TMP}"/maps.parquet | ${CLICKHOUSE_CLIENT} -q "INSERT INTO parquet_maps FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_maps"
${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_maps"
