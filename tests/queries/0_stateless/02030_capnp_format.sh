#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CURDIR/format_schemas
USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
CAPN_PROTO_FILE=$USER_FILES_PATH/data.capnp

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS capnp_simple_types";
$CLICKHOUSE_CLIENT --query="CREATE TABLE capnp_simple_types (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, date Date, datetime DateTime, datetime64 DateTime64(3)) ENGINE=Memory"

$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_simple_types values (-1, 1, -1000, 1000, -10000000, 1000000, -1000000000, 1000000000, 123.123, 123123123.123123123, 'Some string', '2000-01-06', '2000-06-01 19:42:42', '2000-04-01 11:21:33.123')"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_simple_types FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_simple_types:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_simple_types FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_simple_types:Message'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_simple_types" 

$CLICKHOUSE_CLIENT --query="DROP TABLE capnp_simple_types"


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS capnp_tuples"

$CLICKHOUSE_CLIENT --query="CREATE TABLE capnp_tuples (value UInt64, tuple1 Tuple(one UInt64, two Tuple(three UInt64, four UInt64)), tuple2 Tuple(nested1 Tuple(nested2 Tuple(x UInt64)))) ENGINE=Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_tuples VALUES (1, (2, (3, 4)), (((5))))"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_tuples FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_tuples:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_tuples FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_tuples:Message'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_tuples" 

$CLICKHOUSE_CLIENT --query="DROP TABLE capnp_tuples"


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS capnp_lists"

$CLICKHOUSE_CLIENT --query="CREATE TABLE capnp_lists (value UInt64, list1 Array(UInt64), list2 Array(Array(Array(UInt64)))) ENGINE=Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_lists VALUES (1, [1, 2, 3], [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], []], []])"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_lists FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_lists:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_lists FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_lists:Message'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_lists" 

$CLICKHOUSE_CLIENT --query="DROP TABLE capnp_lists"


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS capnp_nested_lists_and_tuples"

$CLICKHOUSE_CLIENT --query="CREATE TABLE capnp_nested_lists_and_tuples (value UInt64, nested Tuple(a Tuple(b UInt64, c Array(Array(UInt64))), d Array(Tuple(e Array(Array(Tuple(f UInt64, g UInt64))), h Array(Tuple(k Array(UInt64))))))) ENGINE=Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_lists_and_tuples VALUES (1, ((2, [[3, 4], [5, 6], []]), [([[(7, 8), (9, 10)], [(11, 12), (13, 14)], []], [([15, 16, 17]), ([])])]))"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_lists_and_tuples FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_nested_lists_and_tuples:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_lists_and_tuples FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_nested_lists_and_tuples:Message'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_lists_and_tuples"

$CLICKHOUSE_CLIENT --query="DROP TABLE capnp_nested_lists_and_tuples"


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS capnp_nested_table"

$CLICKHOUSE_CLIENT --query="CREATE TABLE capnp_nested_table (nested Nested(value UInt64, array Array(UInt64), tuple Tuple(one UInt64, two UInt64))) ENGINE=Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_table VALUES ([1, 2, 3], [[4, 5, 6], [], [7, 8]], [(9, 10), (11, 12), (13, 14)])"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_table FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_nested_table:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nested_table FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_nested_table:Message'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nested_table" 

$CLICKHOUSE_CLIENT --query="DROP TABLE capnp_nested_table"


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS capnp_nullable"

$CLICKHOUSE_CLIENT --query="CREATE TABLE capnp_nullable (nullable Nullable(UInt64), array Array(Nullable(UInt64)), tuple Tuple(nullable Nullable(UInt64))) ENGINE=Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nullable VALUES (1, [1, Null, 2], (1)), (Null, [Null, Null, 42], (Null))"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nullable FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_nullable:Message'" |  $CLICKHOUSE_CLIENT --query="INSERT INTO capnp_nullable FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_nullable:Message'"

$CLICKHOUSE_CLIENT --query="SELECT * FROM capnp_nullable" 

$CLICKHOUSE_CLIENT --query="DROP TABLE capnp_nullable"


$CLICKHOUSE_CLIENT --query="SELECT CAST(0, 'Enum(\'one\' = 0, \'two\' = 1, \'tHrEe\' = 2)') FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/02030_capnp_enum:Message'" > $CAPN_PROTO_FILE

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('$CAPN_PROTO_FILE', 'CapnProto', 'Enum(\'one\' = 0, \'two\' = 1, \'tHrEe\' = 2)') SETTINGS format_schema='$SCHEMADIR/02030_capnp_enum:Message'"


rm $CAPN_PROTO_FILE

