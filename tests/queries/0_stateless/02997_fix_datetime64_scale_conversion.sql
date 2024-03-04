DROP TABLE IF EXISTS test_0;
CREATE TABLE IF NOT EXISTS test_0 (a DateTime64(0)) engine = MergeTree order by a;
INSERT INTO test_0 VALUES (toDateTime64('2023-01-01 00:00:00', 0));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-01 00:00:00.123456789', 0));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-01 01:01:01', 1));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-01 01:01:01.123456789', 1));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-02 02:02:02', 2));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-02 02:02:02.123456789', 2));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-03 03:03:03', 3));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-03 03:03:03.123456789', 3));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-04 04:04:04', 4));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-04 04:04:04.123456789', 4));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-05 05:05:05', 5));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-05 05:05:05.123456789', 5));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-06 06:06:06', 6));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-06 06:06:06.123456789', 6));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-07 07:07:07', 7));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-07 07:07:07.123456789', 7));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-08 08:08:08', 8));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-08 08:08:08.123456789', 8));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-09 09:09:09', 9));
INSERT INTO test_0 VALUES (toDateTime64('2023-01-09 09:09:09.123456789', 9));
SELECT * FROM test_0 ORDER BY a;
DROP TABLE test_0;

DROP TABLE IF EXISTS test_2;
CREATE TABLE IF NOT EXISTS test_2 (a DateTime64(2)) engine = MergeTree order by a;
INSERT INTO test_2 VALUES (toDateTime64('2023-01-01 00:00:00', 0));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-01 00:00:00.123456789', 0));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-01 01:01:01', 1));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-01 01:01:01.123456789', 1));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-02 02:02:02', 2));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-02 02:02:02.123456789', 2));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-03 03:03:03', 3));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-03 03:03:03.123456789', 3));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-04 04:04:04', 4));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-04 04:04:04.123456789', 4));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-05 05:05:05', 5));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-05 05:05:05.123456789', 5));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-06 06:06:06', 6));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-06 06:06:06.123456789', 6));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-07 07:07:07', 7));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-07 07:07:07.123456789', 7));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-08 08:08:08', 8));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-08 08:08:08.123456789', 8));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-09 09:09:09', 9));
INSERT INTO test_2 VALUES (toDateTime64('2023-01-09 09:09:09.123456789', 9));
SELECT * FROM test_2 ORDER BY a;
DROP TABLE test_2;

DROP TABLE IF EXISTS test_3;
CREATE TABLE IF NOT EXISTS test_3 (a DateTime64(3)) engine = MergeTree order by a;
INSERT INTO test_3 VALUES (toDateTime64('2023-01-01 00:00:00', 0));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-01 00:00:00.123456789', 0));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-01 01:01:01', 1));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-01 01:01:01.123456789', 1));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-02 02:02:02', 2));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-02 02:02:02.123456789', 2));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-03 03:03:03', 3));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-03 03:03:03.123456789', 3));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-04 04:04:04', 4));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-04 04:04:04.123456789', 4));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-05 05:05:05', 5));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-05 05:05:05.123456789', 5));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-06 06:06:06', 6));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-06 06:06:06.123456789', 6));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-07 07:07:07', 7));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-07 07:07:07.123456789', 7));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-08 08:08:08', 8));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-08 08:08:08.123456789', 8));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-09 09:09:09', 9));
INSERT INTO test_3 VALUES (toDateTime64('2023-01-09 09:09:09.123456789', 9));
SELECT * FROM test_3 ORDER BY a;
DROP TABLE test_3;

DROP TABLE IF EXISTS test_6;
CREATE TABLE IF NOT EXISTS test_6 (a DateTime64(6)) engine = MergeTree order by a;
INSERT INTO test_6 VALUES (toDateTime64('2023-01-01 00:00:00', 0));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-01 00:00:00.123456789', 0));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-01 01:01:01', 1));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-01 01:01:01.123456789', 1));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-02 02:02:02', 2));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-02 02:02:02.123456789', 2));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-03 03:03:03', 3));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-03 03:03:03.123456789', 3));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-04 04:04:04', 4));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-04 04:04:04.123456789', 4));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-05 05:05:05', 5));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-05 05:05:05.123456789', 5));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-06 06:06:06', 6));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-06 06:06:06.123456789', 6));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-07 07:07:07', 7));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-07 07:07:07.123456789', 7));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-08 08:08:08', 8));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-08 08:08:08.123456789', 8));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-09 09:09:09', 9));
INSERT INTO test_6 VALUES (toDateTime64('2023-01-09 09:09:09.123456789', 9));
SELECT * FROM test_6 ORDER BY a;
DROP TABLE test_6;

DROP TABLE IF EXISTS test_9;
CREATE TABLE IF NOT EXISTS test_9 (a DateTime64(6)) engine = MergeTree order by a;
INSERT INTO test_9 VALUES (toDateTime64('2023-01-01 00:00:00', 0));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-01 00:00:00.123456789', 0));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-01 01:01:01', 1));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-01 01:01:01.123456789', 1));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-02 02:02:02', 2));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-02 02:02:02.123456789', 2));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-03 03:03:03', 3));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-03 03:03:03.123456789', 3));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-04 04:04:04', 4));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-04 04:04:04.123456789', 4));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-05 05:05:05', 5));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-05 05:05:05.123456789', 5));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-06 06:06:06', 6));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-06 06:06:06.123456789', 6));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-07 07:07:07', 7));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-07 07:07:07.123456789', 7));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-08 08:08:08', 8));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-08 08:08:08.123456789', 8));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-09 09:09:09', 9));
INSERT INTO test_9 VALUES (toDateTime64('2023-01-09 09:09:09.123456789', 9));
SELECT * FROM test_9 ORDER BY a;
DROP TABLE test_9;
