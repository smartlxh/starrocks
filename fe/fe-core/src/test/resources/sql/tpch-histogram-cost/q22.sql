[fragment statistics]
PLAN FRAGMENT 0(F09)
Output Exprs:32: substring | 33: count | 34: sum
Input Partition: UNPARTITIONED
RESULT SINK

18:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 150000
column statistics:
* substring-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
* count-->[0.0, 1500000.0, 0.0, 8.0, 150000.0] ESTIMATE
* sum-->[-1091.382358719141, 10913.921812585948, 0.0, 8.0, 137439.0] ESTIMATE

PLAN FRAGMENT 1(F08)

Input Partition: HASH_PARTITIONED: 32: substring
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 18

17:SORT
|  order by: [32, VARCHAR, true] ASC
|  offset: 0
|  cardinality: 150000
|  column statistics:
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * count-->[0.0, 1500000.0, 0.0, 8.0, 150000.0] ESTIMATE
|  * sum-->[-1091.382358719141, 10913.921812585948, 0.0, 8.0, 137439.0] ESTIMATE
|
16:AGGREGATE (merge finalize)
|  aggregate: count[([33: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false], sum[([34: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [32: substring, VARCHAR, true]
|  cardinality: 150000
|  column statistics:
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * count-->[0.0, 1500000.0, 0.0, 8.0, 150000.0] ESTIMATE
|  * sum-->[-1091.382358719141, 10913.921812585948, 0.0, 8.0, 137439.0] ESTIMATE
|
15:EXCHANGE
distribution type: SHUFFLE
partition exprs: [32: substring, VARCHAR, true]
cardinality: 150000

PLAN FRAGMENT 2(F07)

Input Partition: HASH_PARTITIONED: 22: O_CUSTKEY
OutPut Partition: HASH_PARTITIONED: 32: substring
OutPut Exchange Id: 15

14:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false], sum[([6: C_ACCTBAL, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [32: substring, VARCHAR, true]
|  cardinality: 150000
|  column statistics:
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * count-->[0.0, 1500000.0, 0.0, 8.0, 150000.0] ESTIMATE
|  * sum-->[-1091.382358719141, 10913.921812585948, 0.0, 8.0, 137439.0] ESTIMATE
|
13:Project
|  output columns:
|  6 <-> [6: C_ACCTBAL, DOUBLE, false]
|  32 <-> substring[([5: C_PHONE, VARCHAR, false], 1, 2); args: VARCHAR,INT,INT; result: VARCHAR; args nullable: false; result nullable: true]
|  cardinality: 1500000
|  column statistics:
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][-101.79:400][1237.93:400][5209.06:400]] ESTIMATE
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|
12:HASH JOIN
|  join op: RIGHT ANTI JOIN (PARTITIONED)
|  equal join conjunct: [22: O_CUSTKEY, INT, false] = [1: C_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (1: C_CUSTKEY), remote = true
|  output columns: 5, 6
|  cardinality: 1500000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][-101.79:400][1237.93:400][5209.06:400]] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|
|----11:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [1: C_CUSTKEY, INT, false]
|       cardinality: 3750000
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [22: O_CUSTKEY, INT, false]
cardinality: 150000000

PLAN FRAGMENT 3(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: C_CUSTKEY
OutPut Exchange Id: 11

10:Project
|  output columns:
|  1 <-> [1: C_CUSTKEY, INT, false]
|  5 <-> [5: C_PHONE, CHAR, false]
|  6 <-> [6: C_ACCTBAL, DOUBLE, false]
|  cardinality: 3750000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][-101.79:400][1237.93:400][5209.06:400]] ESTIMATE
|
9:NESTLOOP JOIN
|  join op: INNER JOIN
|  other join predicates: [6: C_ACCTBAL, DOUBLE, false] > [19: avg, DOUBLE, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (19: avg), remote = false
|  cardinality: 3750000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][-101.79:400][1237.93:400][5209.06:400]] ESTIMATE
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
2:OlapScanNode
table: customer, rollup: customer
preAggregation: on
Predicates: substring(5: C_PHONE, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=31.0
cardinality: 7500000
probe runtime filters:
- filter_id = 0, probe_expr = (6: C_ACCTBAL)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 7500000.0] ESTIMATE
* C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
* C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][-101.79:400][1237.93:400][5209.06:400]] ESTIMATE

PLAN FRAGMENT 4(F04)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:AGGREGATE (merge finalize)
|  aggregate: avg[([19: avg, VARBINARY, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
6:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 5(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:AGGREGATE (update serialize)
|  aggregate: avg[([15: C_ACCTBAL, DOUBLE, false]); args: DOUBLE; result: VARBINARY; args nullable: false; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
4:Project
|  output columns:
|  15 <-> [15: C_ACCTBAL, DOUBLE, false]
|  cardinality: 6815795
|  column statistics:
|  * C_ACCTBAL-->[0.0, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][3123.67:400][1237.93:400][487.64:400]] ESTIMATE
|
3:OlapScanNode
table: customer, rollup: customer
preAggregation: on
Predicates: [15: C_ACCTBAL, DOUBLE, false] > 0.0, substring(14: C_PHONE, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=23.0
cardinality: 6815795
column statistics:
* C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
* C_ACCTBAL-->[0.0, 9999.99, 0.0, 8.0, 137439.0] MCV: [[3863.78:400][5610.32:400][3123.67:400][1237.93:400][487.64:400]] ESTIMATE

PLAN FRAGMENT 6(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 22: O_CUSTKEY
OutPut Exchange Id: 01

0:OlapScanNode
table: orders, rollup: orders
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=8.0
cardinality: 150000000
probe runtime filters:
- filter_id = 1, probe_expr = (22: O_CUSTKEY)
column statistics:
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
[end]

