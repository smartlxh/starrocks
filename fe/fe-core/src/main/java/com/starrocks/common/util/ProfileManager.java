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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/ProfileManager.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.memory.MemoryTrackable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.SizeEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/*
 * if you want to visit the attribute(such as queryID,defaultDb)
 * you can use profile.getInfoStrings("queryId")
 * All attributes can be seen from the above.
 *
 * why the element in the finished profile array is not RuntimeProfile,
 * the purpose is let coordinator can destruct earlier(the fragment profile is in Coordinator)
 *
 */
public class ProfileManager implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static ProfileManager INSTANCE = null;
    public static final String QUERY_ID = "Query ID";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String RETRY_TIMES = "Retry Times";
    public static final String QUERY_TYPE = "Query Type";
    public static final String QUERY_STATE = "Query State";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String VARIABLES = "Variables";
    public static final String PROFILE_COLLECT_TIME = "Collect Profile Time";

    public static final List<String> PROFILE_HEADERS = new ArrayList<>(
            Arrays.asList(QUERY_ID, USER, DEFAULT_DB, SQL_STATEMENT, QUERY_TYPE,
                    START_TIME, END_TIME, TOTAL_TIME, QUERY_STATE));

    @Override
    public long estimateSize() {
        return SizeEstimator.estimate(profileMap) + SizeEstimator.estimate(loadProfileMap);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("Profile", (long) profileMap.size(),
                               "LoadProfile", (long) loadProfileMap.size());
    }

    public static class ProfileElement {
        public final Map<String, String> infoStrings = Maps.newHashMap();
        public final String profile;
        public final byte[] compressedProfile;
        public final ProfilingExecPlan plan;

        public ProfileElement(String profile, ProfilingExecPlan plan) {
            this.profile = profile;
            this.compressedProfile = null;
            this.plan = plan;
        }

        public ProfileElement(byte[] compressedProfile, ProfilingExecPlan plan) {
            this.profile = null;
            this.compressedProfile = compressedProfile;
            this.plan = plan;
        }

        public List<String> toRow() {
            List<String> res = Lists.newArrayList();
            res.add(infoStrings.get(QUERY_ID));
            res.add(infoStrings.get(START_TIME));
            res.add(infoStrings.get(TOTAL_TIME));
            res.add(infoStrings.get(QUERY_STATE));
            String statement = infoStrings.get(SQL_STATEMENT);
            if (statement.length() > 128) {
                statement = statement.substring(0, 124) + " ...";
            }
            res.add(statement);
            return res;
        }

        public String getProfileContent() throws IOException {
            if (profile == null) {
                return CompressionUtils.gzipDecompressString(compressedProfile);
            } else {
                return profile;
            }
        }
    }

    private final Queue<String> profileExpirationQueue;
    private final Map<String, ProfileElement> profileMap; // from QueryId to RuntimeProfile
    private final Queue<String> loadProfileExpirationQueue;
    private final Map<String, ProfileElement> loadProfileMap; // from LoadId to RuntimeProfile

    public static ProfileManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProfileManager();
        }
        return INSTANCE;
    }

    private ProfileManager() {
        profileExpirationQueue = new LinkedBlockingQueue<>();
        profileMap = new ConcurrentHashMap<>();
        loadProfileExpirationQueue = new LinkedBlockingQueue<>();
        loadProfileMap = new ConcurrentHashMap<>();
    }

    public ProfileElement createElement(RuntimeProfile summaryProfile, String profileString, ProfilingExecPlan plan) {
        ProfileElement element = new ProfileElement(profileString, plan);
        if (Config.profile_enable_compression) {
            try {
                byte[] compressedProfile = CompressionUtils.gzipCompressString(profileString);
                element = new ProfileElement(compressedProfile, plan);
            } catch (IOException e) {
                LOG.warn("Compress profile string failed, length: {}, reason: {}",
                        profileString.length(), e.getMessage());
            }
        }
        for (String header : PROFILE_HEADERS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        return element;
    }

    private String generateProfileString(RuntimeProfile profile) {
        if (profile == null) {
            return "";
        }

        String profileString;
        switch (Config.profile_info_format) {
            case "default":
                profileString = profile.toString();
                break;
            case "json":
                RuntimeProfile.ProfileFormatter formatter = new RuntimeProfile.JsonProfileFormatter();
                profileString = formatter.format(profile, "");
                break;
            default:
                profileString = profile.toString();
                LOG.warn("unknown profile format '{}',  use default format instead.", Config.profile_info_format);
        }
        return profileString;
    }

    private void pushProfileWithExpiration(Map<String, ProfileElement> profileMap, Queue<String> expirationQueue,
            String queryId, ProfileElement element, int numProfileReserved) {
        profileMap.compute(queryId, (id, item) -> {
            if (item == null) {
                expirationQueue.offer(id);
            }
            return element;
        });
        while (expirationQueue.size() > numProfileReserved) {
            String expiredQueryId = expirationQueue.poll();
            profileMap.remove(expiredQueryId);
        }
    }

    public String pushProfile(ProfilingExecPlan plan, RuntimeProfile profile) {
        if (profile == null) {
            return null;
        }
        String profileString = generateProfileString(profile);
        ProfileElement element = createElement(profile.getChildList().get(0).first, profileString, plan);
        String queryId = element.infoStrings.get(ProfileManager.QUERY_ID);
        String queryType = element.infoStrings.get(ProfileManager.QUERY_TYPE);
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(queryId)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' column into infoStrings");
        }

        if (queryType != null && queryType.equals("Load")) {
            pushProfileWithExpiration(
                    loadProfileMap, loadProfileExpirationQueue, queryId, element, Config.load_profile_info_reserved_num);
        } else {
            pushProfileWithExpiration(
                    profileMap, profileExpirationQueue, queryId, element, Config.profile_info_reserved_num);
        }
        return profileString;
    }

    public boolean hasProfile(String queryId) {
        return profileMap.containsKey(queryId) || loadProfileMap.containsKey(queryId);
    }

    private List<List<String>> getAllQueries(Map<String, ProfileElement> profileMap) {
        return profileMap.values().stream()
                .sorted((o1, o2) -> {
                    int startTimeResult = o2.infoStrings.get(START_TIME).compareTo(o1.infoStrings.get(START_TIME));
                    if (startTimeResult == 0) {
                        return o2.infoStrings.get(END_TIME).compareTo(o1.infoStrings.get(END_TIME));
                    }
                    return startTimeResult;
                })
                .map(element -> PROFILE_HEADERS.stream().map(element.infoStrings::get).collect(Collectors.toList()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public List<List<String>> getAllQueries() {
        List<List<String>> profiles = getAllQueries(profileMap);
        List<List<String>> loadProfiles = getAllQueries(loadProfileMap);
        profiles.addAll(loadProfiles);
        return profiles;
    }

    public void removeProfile(String queryId) {
        loadProfileMap.remove(queryId);
        profileMap.remove(queryId);
    }

    public void clearProfiles() {
        loadProfileMap.clear();
        profileMap.clear();
    }

    public String getProfile(String queryId) {
        ProfileElement element = null;
        if (profileMap.containsKey(queryId)) {
            element = profileMap.get(queryId);
        } else if (loadProfileMap.containsKey(queryId)) {
            element = loadProfileMap.get(queryId);
        }

        if (element == null) {
            return null;
        }

        if (Config.profile_enable_compression && element.compressedProfile != null) {
            try {
                return CompressionUtils.gzipDecompressString(element.compressedProfile);
            } catch (IOException e) {
                LOG.warn("Decompress profile content failed, length: {}, reason: {}",
                        element.compressedProfile.length, e.getMessage());
            }
        }
        return element.profile;
    }

    public ProfileElement getProfileElement(String queryId) {
        if (profileMap.containsKey(queryId)) {
            return profileMap.get(queryId);
        } else {
            return loadProfileMap.get(queryId);
        }
    }

    public List<ProfileElement> getAllProfileElements() {
        List<ProfileElement> result = Lists.newArrayList();
        result.addAll(profileMap.values());
        result.addAll(loadProfileMap.values());
        return result;
    }

    public long getQueryProfileCount() {
        return profileMap.size();
    }

    public long getLoadProfileCount() {
        return loadProfileMap.size();
    }
}
