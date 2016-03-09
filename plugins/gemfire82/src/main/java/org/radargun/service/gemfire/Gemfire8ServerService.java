package org.radargun.service.gemfire;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import org.radargun.Service;
import org.radargun.config.Property;
import org.radargun.traits.BasicOperations;
import org.radargun.traits.ProvidesTrait;

import java.io.IOException;

@Service(doc = "Gemfire 8 Server Service")
public class Gemfire8ServerService extends Gemfire8AbstractService {

    @Property(name = "cacheName", doc = "Name of the cache. Default is 'testCache'")
    protected String cacheName = "testCache";

    @Property(name = "dataPolicy", doc = "Gemfire data policy. Default is 'PARTITION'")
    protected DataPolicy dataPolicy = DataPolicy.PARTITION;

    @Property(name = "redundantCopies", doc = "Number of redundant copies. Default is zero")
    protected Integer redundantCopies = 0;

    @ProvidesTrait
    public Gemfire8ServerOperations createBasicOperations() {
        return new Gemfire8ServerOperations();
    }

    // This method is crucial here, it exposes the Lifecycle trait,
    // without it the "start" method won't be invoked
    @ProvidesTrait
    public Gemfire8ServerService getLifecycle() {
        return this;
    }

    @Override
    public void start() {
        cache = new CacheFactory().create();
        CacheServer cacheServer = ((Cache) cache).addCacheServer();

        try {
            cacheServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Could not start cache server", e);
        }

    }

    @Override
    protected <K, V> Region<K, V> createRegion(String name) {
        RegionFactory<K, V> regionFactory = ((Cache) cache).createRegionFactory();
        regionFactory.setDataPolicy(dataPolicy);
        PartitionAttributesImpl partitionAttributes = new PartitionAttributesImpl();
        partitionAttributes.setRedundantCopies(redundantCopies);
        regionFactory.setPartitionAttributes(partitionAttributes);

        return regionFactory.create(name);
    }

    @Override
    public void stop() {
        cache.close();
    }

    @Override
    public boolean isRunning() {
        return cache != null && cache.getDistributedSystem().isConnected();
    }

    public class Gemfire8ServerOperations implements BasicOperations {

        @Override
        public <K, V> BasicOperations.Cache<K, V> getCache(String cacheName) {
            Region<K, V> region = Gemfire8ServerService.this.getOrCreateRegion(cacheName);
            return new Gemfire8Cache<>(region);
        }

    }

}
