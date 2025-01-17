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

package com.starrocks.connector.paimon;

import com.google.common.base.Strings;
import com.starrocks.common.util.DlfUtil;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aliyun.datalake.core.constant.DataLakeConfig.CATALOG_ID;
import static com.aliyun.datalake.core.constant.DataLakeConfig.CATALOG_INSTANCE_ID;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DLF_AUTH_USER_NAME;
import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonConnector implements Connector {
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String DLF_CATALOG_ID = "dlf.catalog.id";
    private final HdfsEnvironment hdfsEnvironment;
    private Catalog paimonNativeCatalog;
    private final String catalogName;
    private final String catalogType;
    private final Options paimonOptions;
    private String ramUser = "";

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.catalogType = properties.get(PAIMON_CATALOG_TYPE);
        String metastoreUris = properties.get(HIVE_METASTORE_URIS);
        String warehousePath = properties.get(PAIMON_CATALOG_WAREHOUSE);

        this.paimonOptions = new Options();
        if (Strings.isNullOrEmpty(catalogType)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_TYPE);
        }
        this.paimonOptions.setString(METASTORE.key(), catalogType);
        if (catalogType.equals("hive")) {
            if (!Strings.isNullOrEmpty(metastoreUris)) {
                this.paimonOptions.setString(URI.key(), metastoreUris);
            } else {
                throw new StarRocksConnectorException("The property %s must be set if paimon catalog is hive.",
                        HIVE_METASTORE_URIS);
            }
        } else if (catalogType.equalsIgnoreCase("dlf") || catalogType.equalsIgnoreCase("dlf-hive")) {
            String dlfCatalogId = properties.get(DLF_CATALOG_ID);
            if (null != dlfCatalogId && !dlfCatalogId.isEmpty()) {
                this.paimonOptions.setString(DLF_CATALOG_ID, dlfCatalogId);
            }
            // By default, dlf-sdk-assembly uses hive2 to access dlf 1.0, however StarRocks only include hive3 in its
            // dependency, so we set this config to let dlf-sdk-assembly use hive3 manually.
            this.paimonOptions.setString("hive.dlf.imetastoreclient.class",
                    "com.aliyun.datalake.metastore.hive3.ProxyMetaStoreClient");
        } else if (catalogType.equalsIgnoreCase("dlf-paimon")) {
            if (Strings.isNullOrEmpty(properties.get(CATALOG_ID))) {
                // CATALOG_INSTANCE_ID is deprecated
                if (null != properties.get(CATALOG_INSTANCE_ID)) {
                    properties.put(CATALOG_ID, properties.get(CATALOG_INSTANCE_ID));
                    properties.remove(CATALOG_INSTANCE_ID);
                } else {
                    throw new StarRocksConnectorException("The property %s must be set.", CATALOG_ID);
                }
            }
            properties.keySet().stream()
                    .filter(k -> k.startsWith("dlf.") && !k.equals(DLF_AUTH_USER_NAME))
                    .forEach(k -> paimonOptions.setString(k, properties.get(k)));
        }
        if (Strings.isNullOrEmpty(warehousePath)
                && !catalogType.equals("hive")
                && !catalogType.equalsIgnoreCase("dlf")
                && !catalogType.equalsIgnoreCase("dlf-hive")
                && !catalogType.equalsIgnoreCase("dlf-paimon")) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_WAREHOUSE);
        }
        if (!Strings.isNullOrEmpty(warehousePath)) {
            // use only for oss-hdfs
            if (warehousePath.charAt(warehousePath.length() - 1) != '/') {
                warehousePath += "/";
            }
        }
        this.paimonOptions.setString(WAREHOUSE.key(), warehousePath);
        initFsOption(cloudConfiguration);
        // cache expire time, set to 2h
        this.paimonOptions.set("cache.expiration-interval", "7200s");
        // max num of cached partitions of a Paimon catalog
        this.paimonOptions.set("cache.partition.max-num", "1000");
        // max size of cached manifest files, 10m means cache all since files usually no more than 8m
        this.paimonOptions.set("cache.manifest.small-file-threshold", "10m");
        // max size of memory manifest cache uses
        this.paimonOptions.set("cache.manifest.small-file-memory", "1g");

        String keyPrefix = "paimon.option.";
        Set<String> optionKeys = properties.keySet().stream().filter(k -> k.startsWith(keyPrefix)).collect(Collectors.toSet());
        for (String k : optionKeys) {
            String key = k.substring(keyPrefix.length());
            this.paimonOptions.setString(key, properties.get(k));
        }
    }

    public void initFsOption(CloudConfiguration cloudConfiguration) {
        if (cloudConfiguration.getCloudType() == CloudType.AWS) {
            AwsCloudConfiguration awsCloudConfiguration = (AwsCloudConfiguration) cloudConfiguration;
            this.paimonOptions.set("s3.connection.ssl.enabled", String.valueOf(awsCloudConfiguration.getEnableSSL()));
            this.paimonOptions.set("s3.path.style.access", String.valueOf(awsCloudConfiguration.getEnablePathStyleAccess()));
            AwsCloudCredential awsCloudCredential = awsCloudConfiguration.getAwsCloudCredential();
            if (!awsCloudCredential.getEndpoint().isEmpty()) {
                this.paimonOptions.set("s3.endpoint", awsCloudCredential.getEndpoint());
            }
            if (!awsCloudCredential.getAccessKey().isEmpty()) {
                this.paimonOptions.set("s3.access-key", awsCloudCredential.getAccessKey());
            }
            if (!awsCloudCredential.getSecretKey().isEmpty()) {
                this.paimonOptions.set("s3.secret-key", awsCloudCredential.getSecretKey());
            }
        }
        if (cloudConfiguration.getCloudType() == CloudType.ALIYUN) {
            AliyunCloudConfiguration aliyunCloudConfiguration = (AliyunCloudConfiguration) cloudConfiguration;
            AliyunCloudCredential aliyunCloudCredential = aliyunCloudConfiguration.getAliyunCloudCredential();
            if (!aliyunCloudCredential.getEndpoint().isEmpty()) {
                this.paimonOptions.set("fs.oss.endpoint", aliyunCloudCredential.getEndpoint());
            }
            if (!aliyunCloudCredential.getAccessKey().isEmpty()) {
                this.paimonOptions.set("fs.oss.accessKeyId", aliyunCloudCredential.getAccessKey());
            }
            if (!aliyunCloudCredential.getSecretKey().isEmpty()) {
                this.paimonOptions.set("fs.oss.accessKeySecret", aliyunCloudCredential.getSecretKey());
            }
        }
    }

    public Options getPaimonOptions() {
        return this.paimonOptions;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public void setRamUser(String ramUser) {
        paimonOptions.set(DLF_AUTH_USER_NAME, ramUser);
    }

    public Catalog getPaimonNativeCatalog() {
        try {
            if (catalogType.equalsIgnoreCase("dlf-paimon")) {
                // For DLF 2.0, we should judge ramUser to see if catalog can be cached
                String ramUser = DlfUtil.getRamUser();
                // When reading information_schema, we should keep ramUser
                if (this.ramUser == null || this.ramUser.isEmpty()
                        || (!ramUser.isEmpty() && !this.ramUser.equals(ramUser))) {
                    this.ramUser = ramUser;
                    setRamUser(ramUser);
                } else if (paimonOptions.get(DLF_AUTH_USER_NAME).equals(ramUser) && paimonNativeCatalog != null) {
                    return paimonNativeCatalog;
                } else {
                    setRamUser(this.ramUser);
                }
            } else if (paimonNativeCatalog != null) {
                // For non DLF 2.0, keep the old method
                return paimonNativeCatalog;
            }
            Configuration configuration = new Configuration();
            hdfsEnvironment.getCloudConfiguration().applyToConfiguration(configuration);
            this.paimonNativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(getPaimonOptions(), configuration));
        } catch (Exception e) {
            if (e instanceof NullPointerException ||
                    (e.getMessage() != null && e.getMessage().contains(DLF_AUTH_USER_NAME))) {
                throw new StarRocksConnectorException("Current user is not a ram user.");
            }
            throw new StarRocksConnectorException("Error creating a paimon catalog. " + e.getMessage());
        }
        return paimonNativeCatalog;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new PaimonMetadata(catalogName, hdfsEnvironment, getPaimonNativeCatalog());
    }

    @Override
    public void shutdown() {
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterPaimonCatalog(catalogName);
    }
}
