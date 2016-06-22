package org.radargun.service;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import org.radargun.Service;
import org.radargun.config.Property;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.Lifecycle;
import org.radargun.traits.ProvidesTrait;

import java.io.IOException;
import java.util.Set;

@Service(doc = "Hazelcast 3.7 Client")
public class Hazelcast3ClientService implements Lifecycle {

    protected Log log = LogFactory.getLog(getClass());

    @Property(name = "addresses", doc = "Addresses for the client to connect to")
    private Set<String> addresses;

    @Property(name = "configPath", doc = "Full path for hazelcast-client.xml")
    protected String configPath;

    @Property(name = "map", doc = "Name of the map")
    protected String mapName = "default";

    @Property(name = "nearCache", doc = "Enable near cache")
    protected boolean enableNearCache = false;

    private HazelcastInstance hazelcastInstance;

    private ClientConfig config;

    @ProvidesTrait
    public Hazelcast3ClientService getLifecycle() {
        return this;
    }

    @ProvidesTrait
    public Hazelcast3ClientOperations getBasicOperations() {
        return new Hazelcast3ClientOperations(this);
    }

    @Override
    public void start() {
        try {
            log.info("Read from configPath: "+configPath);
            config = new XmlClientConfigBuilder(configPath).build();

            //ClientConfig config = new ClientConfig();
//            config.setLicenseKey("ENTERPRISE_HD#10Nodes#7NFIAmOKwafEjJk6rHVbTl5y1U0Su5100111011631100011199090100111");

//            if (enableNearCache) {
//                config.addNearCacheConfig(new NearCacheConfig(mapName));
//                config.getNetworkConfig().setSmartRouting(true);
//            }
//
//            for (String address : addresses) {
//                config.getNetworkConfig().addAddress(address);
//            }

            hazelcastInstance = HazelcastClient.newHazelcastClient(config);

        } catch (IOException e) {
            log.error("hazelcast-client.xml not found", e);
        }
    }

    @Override
    public void stop() {
        printNearCacheStats();
        hazelcastInstance.shutdown();
    }

    private void printNearCacheStats() {
        if (config.getNearCacheConfig(mapName) == null) {
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