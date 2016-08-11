package org.apache.eagle.alert.engine.utils;


import com.typesafe.config.Config;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;

import java.util.Collections;
import java.util.Set;

public class SpecUtils {
    public static Set<String> getTopicsByClient(SpecMetadataServiceClientImpl client) {
        if (client == null) {
            return Collections.emptySet();
        }
        return client.getSpoutSpec().getKafka2TupleMetadataMap().keySet();
    }

    public static Set<String> getTopicsByConfig(Config config) {
        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        return SpecUtils.getTopicsByClient(client);
    }
}
