package org.apache.eagle.alert.engine.spark.function2;

import backtype.storm.metric.api.MultiCountMetric;
import com.typesafe.config.Config;
import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamRouter;
import org.apache.eagle.alert.engine.router.impl.SparkStreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.router.impl.StreamRouterImpl;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class StreamRouteBoltSpark2Function implements MapPartitionsFunction<Tuple2<Integer,PartitionedEvent>, Tuple2<Integer, PartitionedEvent>>, SerializationMetadataProvider {



    private final static Logger LOG = LoggerFactory.getLogger(StreamRouteBoltSpark2Function.class);
    // mapping from StreamPartition to StreamSortSpec
    private Map<StreamPartition, StreamSortSpec> cachedSSS = new HashMap<>();
    // mapping from StreamPartition(streamId, groupbyspec) to StreamRouterSpec
    private Map<StreamPartition, StreamRouterSpec> cachedSRS = new HashMap<>();
    private Map<String, StreamDefinition> sdf = new HashMap<>();
    private RouterSpec routerSpec;
    private String name;
    private Config config;


    public StreamRouteBoltSpark2Function(Config config ,String name) {
        this.name = name;
        this.config = config;
    }
    @Override
    public Iterator<Tuple2<Integer, PartitionedEvent>> call(Iterator<Tuple2<Integer, PartitionedEvent>> tuple2Iterator) throws Exception {
        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        this.sdf = client.getSds();
        this.routerSpec = client.getRouterSpec();
        SparkStreamRouterBoltOutputCollector routeCollector = new SparkStreamRouterBoltOutputCollector(name);
        StreamRouter router = new StreamRouterImpl(name);
        router.prepare(new StreamContextImpl(null, new MultiCountMetric(), null), routeCollector);
        onStreamRouteBoltSpecChange(router, routeCollector, this.routerSpec, sdf);

        while (tuple2Iterator.hasNext()) {
            Tuple2<Integer, PartitionedEvent> tuple2 = tuple2Iterator.next();
            PartitionedEvent partitionedEvent = tuple2._2();
            router.nextEvent(partitionedEvent);
        }
        cleanup(router);
        return routeCollector.emitResult().iterator();
    }



    public void cleanup(StreamRouter router) {
        router.close();
    }

    public void onStreamRouteBoltSpecChange(StreamRouter router, SparkStreamRouterBoltOutputCollector routeCollector, RouterSpec spec, Map<String, StreamDefinition> sds) {

        // figure out added, removed, modified StreamSortSpec
        Map<StreamPartition, StreamSortSpec> newSSS = new HashMap<>();
        spec.getRouterSpecs().forEach(t -> {
            if (t.getPartition().getSortSpec() != null) {
                newSSS.put(t.getPartition(), t.getPartition().getSortSpec());
            }
        });

        Set<StreamPartition> newStreamIds = newSSS.keySet();
        Set<StreamPartition> cachedStreamIds = cachedSSS.keySet();
        Collection<StreamPartition> addedStreamIds = CollectionUtils.subtract(newStreamIds, cachedStreamIds);
        Collection<StreamPartition> removedStreamIds = CollectionUtils.subtract(cachedStreamIds, newStreamIds);
        Collection<StreamPartition> modifiedStreamIds = CollectionUtils.intersection(newStreamIds, cachedStreamIds);

        Map<StreamPartition, StreamSortSpec> added = new HashMap<>();
        Map<StreamPartition, StreamSortSpec> removed = new HashMap<>();
        Map<StreamPartition, StreamSortSpec> modified = new HashMap<>();
        addedStreamIds.forEach(s -> added.put(s, newSSS.get(s)));
        removedStreamIds.forEach(s -> removed.put(s, cachedSSS.get(s)));
        modifiedStreamIds.forEach(s -> {
            if (!newSSS.get(s).equals(cachedSSS.get(s))) { // this means StreamSortSpec is changed for one specific streamId
                modified.put(s, newSSS.get(s));
            }
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("added StreamSortSpec " + added);
            LOG.debug("removed StreamSortSpec " + removed);
            LOG.debug("modified StreamSortSpec " + modified);
        }
        router.onStreamSortSpecChange(added, removed, modified);
        // switch cache
        cachedSSS = newSSS;

        // figure out added, removed, modified StreamRouterSpec
        Map<StreamPartition, StreamRouterSpec> newSRS = new HashMap<>();
        spec.getRouterSpecs().forEach(t -> newSRS.put(t.getPartition(), t));

        Set<StreamPartition> newStreamPartitions = newSRS.keySet();
        Set<StreamPartition> cachedStreamPartitions = cachedSRS.keySet();

        Collection<StreamPartition> addedStreamPartitions = CollectionUtils.subtract(newStreamPartitions, cachedStreamPartitions);
        Collection<StreamPartition> removedStreamPartitions = CollectionUtils.subtract(cachedStreamPartitions, newStreamPartitions);
        Collection<StreamPartition> modifiedStreamPartitions = CollectionUtils.intersection(newStreamPartitions, cachedStreamPartitions);

        Collection<StreamRouterSpec> addedRouterSpecs = new ArrayList<>();
        Collection<StreamRouterSpec> removedRouterSpecs = new ArrayList<>();
        Collection<StreamRouterSpec> modifiedRouterSpecs = new ArrayList<>();
        addedStreamPartitions.forEach(s -> addedRouterSpecs.add(newSRS.get(s)));
        removedStreamPartitions.forEach(s -> removedRouterSpecs.add(cachedSRS.get(s)));
        modifiedStreamPartitions.forEach(s -> {
            if (!newSRS.get(s).equals(cachedSRS.get(s))) { // this means StreamRouterSpec is changed for one specific StreamPartition
                modifiedRouterSpecs.add(newSRS.get(s));
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("added StreamRouterSpec " + addedRouterSpecs);
            LOG.debug("removed StreamRouterSpec " + removedRouterSpecs);
            LOG.debug("modified StreamRouterSpec " + modifiedRouterSpecs);
        }
        routeCollector.onStreamRouterSpecChange(addedRouterSpecs, removedRouterSpecs, modifiedRouterSpecs, sds);
        // switch cache
        cachedSRS = newSRS;
        sdf = sds;
    }


    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return this.sdf.get(streamId);
    }


}