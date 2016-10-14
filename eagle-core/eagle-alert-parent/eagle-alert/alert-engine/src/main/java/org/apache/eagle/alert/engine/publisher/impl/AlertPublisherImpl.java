/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.publisher.impl;

import com.typesafe.config.Config;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("rawtypes")
public class AlertPublisherImpl implements AlertPublisher {
    private static final long serialVersionUID = 4809983246198138865L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertPublisherImpl.class);
    private final String name;

    private volatile Map<String, List<String>> policyPublishPluginMapping = new ConcurrentHashMap<>(1);
    private volatile Map<String, AlertPublishPlugin> publishPluginMapping = new ConcurrentHashMap<>(1);
    private Config config;
    private Map conf;

    public AlertPublisherImpl(String name) {
        this.name = name;
    }

    public AlertPublisherImpl(String name, List<Publishment> publishments) {
        this.name = name;
        for (Publishment publishment : publishments) {
            AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(publishment, config, conf);
            if (plugin != null) {
                publishPluginMapping.put(publishment.getName(), plugin);
                onPolicyAdded(publishment.getPolicyIds(), publishment.getName());
            } else {
                LOG.error("Initialized alertPublisher {} failed due to invalid format", publishment);
            }
        }
    }

    @Override
    public void init(Config config, Map conf) {
        this.config = config;
        this.conf = conf;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void nextEvent(AlertStreamEvent event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(event.toString());
        }
        notifyAlert(event);
    }

    private void notifyAlert(AlertStreamEvent event) {
        String policyId = event.getPolicyId();
        if (StringUtils.isEmpty(policyId)) {
            LOG.warn("policyId cannot be null for event to be published");
            return;
        }
        List<String> pubIds = policyPublishPluginMapping.get(policyId);
        if (pubIds == null) {
            LOG.warn("Policy {} does *NOT* subscribe any publishment!", policyId);
            return;
        }

        for (String pubId : pubIds) {
            AlertPublishPlugin plugin = pubId != null ? publishPluginMapping.get(pubId) : null;
            if (plugin == null) {
                LOG.warn("Policy {} does *NOT* subscribe any publishment!", policyId);
                continue;
            }
            try {
                LOG.debug("Execute alert publisher {}", plugin.getClass().getCanonicalName());
                plugin.onAlert(event);
            } catch (Exception ex) {
                LOG.error("Fail invoking publisher's onAlert, continue ", ex);
            }
        }
    }

    @Override
    public void close() {
        publishPluginMapping.values().forEach(plugin -> plugin.close());
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void onPublishChange(List<Publishment> added,
                                List<Publishment> removed,
                                List<Publishment> afterModified,
                                List<Publishment> beforeModified) {
        if (added == null) {
            added = new ArrayList<>();
        }
        if (removed == null) {
            removed = new ArrayList<>();
        }
        if (afterModified == null) {
            afterModified = new ArrayList<>();
        }
        if (beforeModified == null) {
            beforeModified = new ArrayList<>();
        }

        if (afterModified.size() != beforeModified.size()) {
            LOG.warn("beforeModified size != afterModified size");
            return;
        }

        // copy and swap to avoid concurrency issue
        Map<String, List<String>> newPolicyPublishPluginMapping = new HashMap<>(policyPublishPluginMapping);
        Map<String, AlertPublishPlugin> newPublishMap = new HashMap<>(publishPluginMapping);

        // added
        for (Publishment publishment : added) {
            LOG.debug("OnPublishmentChange : add publishment : {} ", publishment);

            AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(publishment, config, conf);
            if (plugin != null) {
                newPublishMap.put(publishment.getName(), plugin);
                addPublishmentPolicies(newPolicyPublishPluginMapping, publishment.getPolicyIds(), publishment.getName());
            } else {
                LOG.error("OnPublishChange alertPublisher {} failed due to invalid format", publishment);
            }
        }
        //removed
        List<AlertPublishPlugin> toBeClosed = new ArrayList<>();
        for (Publishment publishment : removed) {
            String pubName = publishment.getName();
            removePublihsPolicies(newPolicyPublishPluginMapping, publishment.getPolicyIds(), pubName);
            toBeClosed.add(newPublishMap.get(pubName));
            newPublishMap.remove(publishment.getName());
        }
        // updated
        for (int i = 0; i < afterModified.size(); i++) {
            String pubName = afterModified.get(i).getName();
            List<String> newPolicies = afterModified.get(i).getPolicyIds();
            List<String> oldPolicies = beforeModified.get(i).getPolicyIds();

            if (!newPolicies.equals(oldPolicies)) {
                List<String> deletedPolicies = ListUtils.subtract(oldPolicies, newPolicies);
                removePublihsPolicies(newPolicyPublishPluginMapping, deletedPolicies, pubName);
                List<String> addedPolicies = ListUtils.subtract(newPolicies, oldPolicies);
                addPublishmentPolicies(newPolicyPublishPluginMapping, addedPolicies, pubName);
            }
            Publishment newPub = afterModified.get(i);
            newPublishMap.get(pubName).update(newPub.getDedupIntervalMin(), newPub.getProperties());
        }

        // now do the swap
        publishPluginMapping = newPublishMap;
        policyPublishPluginMapping = newPolicyPublishPluginMapping;

        // safely close : it depend on plugin to check if want to wait all data to be flushed.
        closePlugins(toBeClosed);
    }

    private void closePlugins(List<AlertPublishPlugin> toBeClosed) {
        for (AlertPublishPlugin p : toBeClosed) {
            try {
                p.close();
            } catch (Exception e) {
                LOG.error(MessageFormat.format("Error when close publish plugin {}, {}!", p.getClass().getCanonicalName()), e);
            }
        }
    }

    private void addPublishmentPolicies(Map<String, List<String>> newPolicyPublishPluginMapping, List<String> addedPolicyIds, String pubName) {
        if (addedPolicyIds == null || pubName == null) {
            return;
        }

        for (String policyId : addedPolicyIds) {
            newPolicyPublishPluginMapping.putIfAbsent(policyId, new ArrayList<>());
            newPolicyPublishPluginMapping.get(policyId).add(pubName);
        }
    }

    private synchronized void removePublihsPolicies(Map<String, List<String>> newPolicyPublishPluginMapping, List<String> deletedPolicyIds, String pubName) {
        if (deletedPolicyIds == null || pubName == null) {
            return;
        }

        for (String policyId : deletedPolicyIds) {
            List<String> publishIds = newPolicyPublishPluginMapping.get(policyId);
            publishIds.remove(pubName);
        }
    }

}
