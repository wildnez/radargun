package org.radargun.service;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import org.radargun.Service;
import org.radargun.config.Property;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.Lifecycle;
import org.radargun.traits.ProvidesTrait;

import java.util.Set;

@Service(doc = "Hazelcast 3.6 Client")
public class Hazelcast36ClientService implements Lifecycle {

    protected Log log = LogFactory.getLog(getClass());

    @Property(name = "addresses", doc = "Addresses for the client to connect to")
    private Set<String> addresses;

    @Property(name = "map", doc = "Name of the map")
    protected String mapName = "default";

    @Property(name = "nearCache", doc = "Enable near cache")
    protected boolean enableNearCache = false;

    private HazelcastInstance hazelcastInstance;

    @ProvidesTrait
    public Hazelcast36ClientService getLifecycle() {
        return this;
    }

    @ProvidesTrait
    public Hazelcast36ClientOperations getBasicOperations() {
        return new Hazelcast36ClientOperations(this);
    }

    @Override
    public void start() {
        ClientConfig config = new ClientConfig();

        if (enableNearCache) {
            config.addNearCacheConfig(new NearCacheConfig(mapName));
            config.getNetworkConfig().setSmartRouting(true);
        }

        for (String address : addresses) {
            config.getNetworkConfig().addAddress(address);
        }

        hazelcastInstance = HazelcastClient.newHazelcastClient(config);
    }

    @Override
    public void stop() {
        printNearCacheStats();
        hazelcastInstance.shutdown();
    }

    private void printNearCacheStats() {
        if (!enableNearCache) {
            return;
        }

        NearCacheStats nearCacheStats = hazelcastInstance.getMap(mapName).getLocalMapStats().getNearCacheStats();
        long hits = nearCacheStats.getHits();
        long misses = nearCacheStats.getMisses();
        log.info(String.format("Hits: %d, misses: %d", hits, misses));
    }

    @Override
    public boolean isRunning() {
        return hazelcastInstance != null && hazelcastInstance.getLifecycleService().isRunning();
    }

    public <K, V> IMap<K, V> getMap(String name) {
        if (name != null) {
            return hazelcastInstance.getMap(name);
        }

        return hazelcastInstance.getMap(mapName);
    }
}