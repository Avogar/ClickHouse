#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS file"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE file (x UInt64) ENGINE = File(Native, '${CLICKHOUSE_DATABASE}/data.lz4')"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE file"
${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers(100000)"
${CLICKHOUSE_CLIENT} --query "SELECT max(x) FROM file"
${CLICKHOUSE_CLIENT} --query "DROP TABLE file"
