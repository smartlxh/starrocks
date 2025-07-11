// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.delta;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.qe.ConnectContext;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBinaryVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultMapVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class DeltaLakeMetadataTest {
    private HiveMetaClient client;
    private DeltaLakeMetadata deltaLakeMetadata;

    @BeforeEach
    public void setUp() throws Exception {
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(Maps.newHashMap());

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);

        HMSBackedDeltaMetastore hmsBackedDeltaMetastore = new HMSBackedDeltaMetastore("delta0", hiveMetastore,
                new Configuration(), new DeltaLakeCatalogProperties(Maps.newHashMap()));
        DeltaMetastoreOperations deltaOps = new DeltaMetastoreOperations(
                CachingDeltaLakeMetastore.createQueryLevelInstance(hmsBackedDeltaMetastore, 10000), false,
                MetastoreType.HMS);

        deltaLakeMetadata = new DeltaLakeMetadata(hdfsEnvironment, "delta0", deltaOps, null,
                new ConnectorProperties(ConnectorType.DELTALAKE));
    }

    @Test
    public void testListDbNames() {
        List<String> dbNames = deltaLakeMetadata.listDbNames(new ConnectContext());
        Assertions.assertEquals(2, dbNames.size());
        Assertions.assertEquals("db1", dbNames.get(0));
        Assertions.assertEquals("db2", dbNames.get(1));
    }

    @Test
    public void testListTableNames() {
        List<String> tableNames = deltaLakeMetadata.listTableNames(new ConnectContext(), "db1");
        Assertions.assertEquals(2, tableNames.size());
        Assertions.assertEquals("table1", tableNames.get(0));
        Assertions.assertEquals("table2", tableNames.get(1));
    }

    @Test
    public void testListPartitionNames(@Mocked SnapshotImpl snapshot, @Mocked ScanBuilder scanBuilder,
                                       @Mocked Scan scan) {
        new MockUp<DeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getLatestSnapshot(String dbName, String tableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table",
                                123));
            }
        };

        new MockUp<DeltaUtils>() {
            @Mock
            public DeltaLakeTable convertDeltaSnapshotToSRTable(String catalog, DeltaLakeSnapshot deltaLakeSnapshot) {
                return new DeltaLakeTable(1, "delta0", "db1", "table1",
                        Lists.newArrayList(), Lists.newArrayList("ts"), snapshot, null,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table",
                                123));
            }
        };

        // mock schema:
        // struct<add:struct<path:string,partitionValues:map<string,string>>>
        List<FilteredColumnarBatch> filteredColumnarBatches = Lists.newArrayList();

        ColumnVector[] addFileCols = new ColumnVector[2];
        addFileCols[0] = new DefaultBinaryVector(BasePrimitiveType.createPrimitive("string"),
                3, new byte[][] {new byte[] {'0', '0', '0', '0'},
                    new byte[] {'0', '0', '0', '1'}, new byte[] {'0', '0', '0', '2'}});

        int[] offsets = new int[] {0, 1, 2, 3};
        DataType mapType = new MapType(StringType.STRING, StringType.STRING, true);
        addFileCols[1] = new DefaultMapVector(3, mapType, Optional.empty(), offsets,
                new DefaultBinaryVector(BasePrimitiveType.createPrimitive("string"),
                        3, new byte[][] {new byte[] {'t', 's'}, new byte[] {'t', 's'}, new byte[] {'t', 's'}}),
                new DefaultBinaryVector(BasePrimitiveType.createPrimitive("string"),
                        3, new byte[][] {new byte[] {'1', '9', '9', '9'}, new byte[] {'2', '0', '0', '0'},
                            new byte[] {'2', '0', '0', '1'}})
        );
        // addFile schema, here we only care about the partitionValues, so not use all fields
        StructType addFileSchema = new StructType(Lists.newArrayList(
                new StructField("path", BasePrimitiveType.createPrimitive("string"), true, null),
                new StructField("partitionValues", mapType, true, null)));
        DefaultStructVector addFile = new DefaultStructVector(3, addFileSchema, Optional.empty(), addFileCols);
        // construct a columnar batch which only contains addFile
        ColumnarBatch columnarBatch = new DefaultColumnarBatch(3,
                new StructType(Lists.newArrayList(new StructField("add", addFileSchema, true))),
                new DefaultStructVector[] {addFile});

        FilteredColumnarBatch filteredColumnarBatch = new FilteredColumnarBatch(columnarBatch, Optional.empty());
        filteredColumnarBatches.add(filteredColumnarBatch);
        CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = new CloseableIterator<FilteredColumnarBatch>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < filteredColumnarBatches.size();
            }

            @Override
            public FilteredColumnarBatch next() {
                return filteredColumnarBatches.get(index++);
            }

            @Override
            public void close() {
            }
        };

        new Expectations() {
            {
                snapshot.getScanBuilder((Engine) any);
                result = scanBuilder;
                minTimes = 0;

                scanBuilder.build();
                result = scan;
                minTimes = 0;

                scan.getScanFiles((Engine) any);
                result = scanFilesAsBatches;
                minTimes = 0;
            }
        };
        List<String> partitionNames =
                deltaLakeMetadata.listPartitionNames("db1", "table1", ConnectorMetadatRequestContext.DEFAULT);
        Assertions.assertEquals(3, partitionNames.size());
        Assertions.assertEquals("ts=1999", partitionNames.get(0));
        Assertions.assertEquals("ts=2000", partitionNames.get(1));
        Assertions.assertEquals("ts=2001", partitionNames.get(2));
    }

    @Test
    public void testTableExists() {
        Assertions.assertTrue(deltaLakeMetadata.tableExists(new ConnectContext(), "db1", "table1"));
    }

    @Test
    public void testGetTable() {
        new MockUp<CachingDeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getCachedSnapshot(DatabaseTableName databaseTableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        new MetastoreTable("db1", "table1", "path/to/table",
                                123));
            }
        };

        new MockUp<DeltaUtils>() {
            @mockit.Mock
            public DeltaLakeTable convertDeltaSnapshotToSRTable(String catalog, DeltaLakeSnapshot snapshot) {
                return new DeltaLakeTable(1, "delta0", "db1", "table1", Lists.newArrayList(),
                        Lists.newArrayList("col1"), null, null,
                        new MetastoreTable("db1", "table1", "path/to/table",
                        123));
            }
        };
        DeltaLakeTable deltaTable = (DeltaLakeTable) deltaLakeMetadata.getTable(new ConnectContext(), "db1", "table1");
        Assertions.assertNotNull(deltaTable);
        Assertions.assertEquals("table1", deltaTable.getName());
        Assertions.assertEquals(Table.TableType.DELTALAKE, deltaTable.getType());
        Assertions.assertEquals("path/to/table", deltaTable.getTableLocation());
    }
}
