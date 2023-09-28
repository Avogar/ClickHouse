#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select number as x from numbers(5) format JSONEachRow" > $CLICKHOUSE_TEST_UNIQUE_NAME.json
$CLICKHOUSE_LOCAL -n -q "
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME.json');
set input_format_json_try_infer_numbers_from_strings=0;
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME.json');"

rm  $CLICKHOUSE_TEST_UNIQUE_NAME.json

