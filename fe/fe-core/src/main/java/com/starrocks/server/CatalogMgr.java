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


package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Resource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.DbsProcDir;
import com.starrocks.common.proc.ExternalDbsProcDir;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.persist.AlterCatalogLog;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AlterCatalogStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.catalog.ResourceMgr.NEED_MAPPING_CATALOG_RESOURCES;
import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class CatalogMgr {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);
    private final Map<String, Catalog> catalogs = Maps.newConcurrentMap();
    private final ConnectorMgr connectorMgr;
    private final ReadWriteLock catalogLock = new ReentrantReadWriteLock();

    public static final ImmutableList<String> CATALOG_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Catalog").add("Type").add("Comment")
            .build();

    private final CatalogProcNode procNode = new CatalogProcNode();

    public CatalogMgr(ConnectorMgr connectorMgr) {
        Preconditions.checkNotNull(connectorMgr, "ConnectorMgr is null");
        this.connectorMgr = connectorMgr;
    }

    public void createCatalog(CreateCatalogStmt stmt) throws DdlException {
        createCatalog(stmt.getCatalogType(), stmt.getCatalogName(), stmt.getComment(), stmt.getProperties());
    }

    public void createCatalogForRestore(Catalog catalog) throws DdlException {
        dropCatalogForRestore(catalog, false);
        createCatalog(catalog.getType(), catalog.getName(), catalog.getComment(), catalog.getConfig());
    }

    // please keep connector and catalog create together, they need keep in consistent asap.
    public void createCatalog(String type, String catalogName, String comment, Map<String, String> properties)
            throws DdlException {
        CatalogConnector connector = null;
        Catalog catalog = null;
        writeLock();
        try {
            if (Strings.isNullOrEmpty(type)) {
                throw new DdlException("Missing properties 'type'");
            }

            Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);

            try {
                Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);

                connector = connectorMgr.createConnector(
                        new ConnectorContext(catalogName, type, properties), false);
                if (null == connector) {
                    LOG.error("{} connector [{}] create failed", type, catalogName);
                    throw new DdlException("connector create failed");
                }
                long id = isResourceMappingCatalog(catalogName) ?
                        ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt() :
                        GlobalStateMgr.getCurrentState().getNextId();
                catalog = new ExternalCatalog(id, catalogName, comment, properties);
                catalogs.put(catalogName, catalog);

                if (!isResourceMappingCatalog(catalogName)) {
                    GlobalStateMgr.getCurrentState().getEditLog().logCreateCatalog(catalog);
                }
            } catch (StarRocksConnectorException e) {
                LOG.error("connector create failed. catalog [{}] ", catalogName, e);
                throw new DdlException(String.format("connector create failed {%s}", e.getMessage()));
            }
        } catch (Exception e) {
            if (connector != null && connectorMgr.connectorExists(catalogName)) {
                connectorMgr.removeConnector(catalogName);
            }

            if (catalog != null) {
                catalogs.remove(catalogName);
            }

            throw e;
        } finally {
            writeUnLock();
        }
    }

    private void reCreatCatalog(Catalog catalog, Map<String, String> alterProperties, boolean isReplay) throws DdlException {
        String catalogName = catalog.getName();
        String type = catalog.getType();
        CatalogConnector newConnector = null;

        writeLock();
        try {
            Map<String, String> newProperties = new HashMap<>(catalog.getConfig().size() + alterProperties.size());
            newProperties.putAll(catalog.getConfig());
            newProperties.putAll(alterProperties);

            LOG.info("Recreate catalog [{}] with properties [{}]", catalogName, newProperties);

            newConnector = connectorMgr.createHiddenConnector(
                    new ConnectorContext(catalogName, type, newProperties), isReplay);
            if (null == newConnector) {
                throw new DdlException("Create connector failed");
            }

            // drop old connector
            connectorMgr.removeConnector(catalogName);

            // replace old connector with new connector
            connectorMgr.addConnector(catalogName, newConnector);

            catalog.getConfig().putAll(alterProperties);
        } catch (Exception e) {
            LOG.error("Recreate catalog failed. catalog [{}] ", catalogName, e);

            if (newConnector != null) {
                newConnector.shutdown();
            }

            throw new DdlException(String.format("Alter catalog failed, msg: [%s]", e.getMessage()), e);
        } finally {
            writeUnLock();
        }
    }

    public void dropCatalogForRestore(Catalog catalog, boolean isReplay) {
        if (!isReplay && catalogExists(catalog.getName())) {
            DropCatalogStmt stmt = new DropCatalogStmt(catalog.getName());
            dropCatalog(stmt);
        } else if (isReplay) {
            DropCatalogLog dropCatalogLog = new DropCatalogLog(catalog.getName());
            replayDropCatalog(dropCatalogLog);
        }
    }

    public void dropCatalog(DropCatalogStmt stmt) {
        String catalogName = stmt.getName();
        readLock();
        try {
            Preconditions.checkState(catalogs.containsKey(catalogName), "Catalog '%s' doesn't exist", catalogName);
        } finally {
            readUnlock();
        }

        writeLock();
        try {
            connectorMgr.removeConnector(catalogName);
            Authorizer.getInstance().removeAccessControl(catalogName);
            catalogs.remove(catalogName);
            if (!isResourceMappingCatalog(catalogName)) {
                DropCatalogLog dropCatalogLog = new DropCatalogLog(catalogName);
                GlobalStateMgr.getCurrentState().getEditLog().logDropCatalog(dropCatalogLog);
            }
        } finally {
            writeUnLock();
        }
    }

    public void alterCatalog(AlterCatalogStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        writeLock();
        try {
            Catalog catalog = catalogs.get(catalogName);
            if (catalog == null) {
                return;
            }

            if (stmt.getAlterClause() instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = ((ModifyTablePropertiesClause) stmt.getAlterClause()).getProperties();
                alterCatalog(catalog, properties, false);

                AlterCatalogLog alterCatalogLog = new AlterCatalogLog(catalogName, properties);
                GlobalStateMgr.getCurrentState().getEditLog().logAlterCatalog(alterCatalogLog);
            }
        } finally {
            writeUnLock();
        }
    }

    private void alterCatalog(Catalog catalog, Map<String, String> properties, boolean isReplay) throws DdlException {
        Map<String, String> alterProperties = new HashMap<>(properties.size());
        Map<String, String> oldProperties = catalog.getConfig();

        for (String confName : properties.keySet()) {
            String oldVal = oldProperties.get(confName);
            String newVal = properties.get(confName);
            if (!oldProperties.containsKey(confName) || !Objects.equals(oldVal, newVal)) {
                alterProperties.put(confName, newVal);
            }
        }

        if (alterProperties.isEmpty()) {
            return;
        }

        reCreatCatalog(catalog, alterProperties, isReplay);
    }

    // TODO @caneGuy we should put internal catalog into catalogmgr
    public boolean catalogExists(String catalogName) {
        if (catalogName.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            return true;
        }

        readLock();
        try {
            if (catalogs.containsKey(catalogName)) {
                return true;
            }

            // TODO: Used for replay query dump which only supports `hive` catalog for now.
            if (FeConstants.isReplayFromQueryDump &&
                    catalogs.containsKey(getResourceMappingCatalogName(catalogName, "hive"))) {
                return true;
            }

            return false;
        } finally {
            readUnlock();
        }
    }

    public static boolean isInternalCatalog(String name) {
        return name.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
    }

    public static boolean isInternalCatalog(long catalogId) {
        return catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
    }

    public static boolean isExternalCatalog(String name) {
        return !Strings.isNullOrEmpty(name) && !isInternalCatalog(name) && !isResourceMappingCatalog(name);
    }

    // please keep connector and catalog create together, they need keep in consistent asap.
    public void replayCreateCatalog(Catalog catalog) throws DdlException {
        String type = catalog.getType();
        String catalogName = catalog.getName();
        Map<String, String> config = catalog.getConfig();
        CatalogConnector catalogConnector = null;

        try {
            if (Strings.isNullOrEmpty(type)) {
                throw new DdlException("Missing properties 'type'");
            }

            // skip unsupported connector type
            if (!ConnectorType.isSupport(type)) {
                LOG.error("Replay catalog [{}] encounter unknown catalog type [{}], ignore it", catalogName, type);
                return;
            }

            readLock();
            try {
                Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
            } finally {
                readUnlock();
            }

            try {
                catalogConnector = connectorMgr.createConnector(
                        new ConnectorContext(catalogName, type, config), true);
                if (catalogConnector == null) {
                    LOG.error("{} connector [{}] create failed.", type, catalogName);
                    throw new DdlException("connector create failed");
                }
            } catch (StarRocksConnectorException e) {
                LOG.error("connector create failed [{}], reason {}", catalogName, e.getMessage());
                throw new DdlException(String.format("connector create failed: %s", e.getMessage()));
            }

            writeLock();
            try {
                catalogs.put(catalogName, catalog);
            } finally {
                writeUnLock();
            }
        } catch (Exception e) {
            if (catalogConnector != null && connectorMgr.connectorExists(catalogName)) {
                connectorMgr.removeConnector(catalogName);
            }

            catalogs.remove(catalogName);
            throw e;
        }
    }

    public void replayDropCatalog(DropCatalogLog log) {
        String catalogName = log.getCatalogName();
        readLock();
        try {
            if (!catalogs.containsKey(catalogName)) {
                LOG.error("Catalog [{}] doesn't exist, unsupport this catalog type, ignore it", catalogName);
                return;
            }
        } finally {
            readUnlock();
        }
        connectorMgr.removeConnector(catalogName);

        writeLock();
        try {
            Authorizer.getInstance().removeAccessControl(catalogName);
            catalogs.remove(catalogName);
        } finally {
            writeUnLock();
        }
    }

    public void replayAlterCatalog(AlterCatalogLog log) throws DdlException {
        writeLock();
        try {
            String catalogName = log.getCatalogName();
            Map<String, String> properties = log.getProperties();
            Catalog catalog = catalogs.get(catalogName);

            alterCatalog(catalog, properties, true);
        } finally {
            writeUnLock();
        }
    }

    public void loadResourceMappingCatalog() {
        LOG.info("start to replay resource mapping catalog");

        List<Resource> resources = GlobalStateMgr.getCurrentState().getResourceMgr().getNeedMappingCatalogResources();
        for (Resource resource : resources) {
            Map<String, String> properties = Maps.newHashMap(resource.getProperties());
            String type = resource.getType().name().toLowerCase(Locale.ROOT);
            if (!NEED_MAPPING_CATALOG_RESOURCES.contains(type)) {
                return;
            }

            String catalogName = getResourceMappingCatalogName(resource.getName(), type);
            properties.put("type", type);
            properties.put(HIVE_METASTORE_URIS, resource.getHiveMetastoreURIs());
            try {
                createCatalog(type, catalogName, "mapping " + type + " catalog", properties);
            } catch (Exception e) {
                LOG.error("Failed to load resource mapping inside catalog {}", catalogName, e);
            }
        }
        LOG.info("finished replaying resource mapping catalogs from resources");
    }

    public List<List<String>> getCatalogsInfo() {
        return procNode.fetchResult().getRows();
    }

    public String getCatalogType(String catalogName) {
        if (isInternalCatalog(catalogName)) {
            return "internal";
        } else {
            return catalogs.get(catalogName).getType();
        }
    }

    public Catalog getCatalogByName(String name) {
        return catalogs.get(name);
    }

    public Optional<Catalog> getCatalogById(long id) {
        return catalogs.values().stream().filter(catalog -> catalog.getId() == id).findFirst();
    }

    public Map<String, Catalog> getCatalogs() {
        return new HashMap<>(catalogs);
    }

    public boolean checkCatalogExistsById(long id) {
        return catalogs.entrySet().stream().anyMatch(entry -> entry.getValue().getId() == id);
    }

    public CatalogProcNode getProcNode() {
        return procNode;
    }

    private void readLock() {
        this.catalogLock.readLock().lock();
    }

    private void readUnlock() {
        this.catalogLock.readLock().unlock();
    }

    private void writeLock() {
        this.catalogLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.catalogLock.writeLock().unlock();
    }

    public long getCatalogCount() {
        return catalogs.size();
    }

    public class CatalogProcNode implements ProcDirInterface {
        private static final String DEFAULT_CATALOG_COMMENT =
                "An internal catalog contains this cluster's self-managed tables.";

        @Override
        public boolean register(String name, ProcNodeInterface node) {
            return false;
        }

        @Override
        public ProcNodeInterface lookup(String catalogName) throws AnalysisException {
            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return new DbsProcDir(GlobalStateMgr.getCurrentState());
            }
            return new ExternalDbsProcDir(catalogName);
        }

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(CATALOG_PROC_NODE_TITLE_NAMES);
            readLock();
            try {
                for (Map.Entry<String, Catalog> entry : catalogs.entrySet()) {
                    Catalog catalog = entry.getValue();
                    if (catalog == null || isResourceMappingCatalog(catalog.getName())) {
                        continue;
                    }
                    catalog.getProcNodeData(result);
                }
            } finally {
                readUnlock();
            }
            result.addRow(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "Internal", DEFAULT_CATALOG_COMMENT));
            return result;
        }
    }

    public static class ResourceMappingCatalog {
        public static final String RESOURCE_MAPPING_CATALOG_PREFIX = "resource_mapping_inside_catalog_";

        public static boolean isResourceMappingCatalog(String catalogName) {
            return catalogName.startsWith(RESOURCE_MAPPING_CATALOG_PREFIX);
        }

        public static String getResourceMappingCatalogName(String resourceName, String type) {
            return (RESOURCE_MAPPING_CATALOG_PREFIX + type + "_" + resourceName).toLowerCase(Locale.ROOT);
        }

        public static String toResourceName(String catalogName, String type) {
            return isResourceMappingCatalog(catalogName) ?
                    catalogName.substring(RESOURCE_MAPPING_CATALOG_PREFIX.length() + type.length() + 1) : catalogName;
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        Map<String, Catalog> serializedCatalogs = catalogs.entrySet().stream()
                .filter(entry -> !isResourceMappingCatalog(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        int numJson = 1 + serializedCatalogs.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.CATALOG_MGR, numJson);

        writer.writeInt(serializedCatalogs.size());
        for (Catalog catalog : serializedCatalogs.values()) {
            writer.writeJson(catalog);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(Catalog.class, catalog -> {
            try {
                replayCreateCatalog(catalog);
            } catch (Exception e) {
                LOG.error("Failed to load catalog {}, ignore the error, continue load", catalog.getName(), e);
            }
        });

        loadResourceMappingCatalog();
    }
}
