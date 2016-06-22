package org.radargun.service;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.cluster.ClusterScheme;
import org.radargun.Service;
import org.radargun.config.Property;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.Lifecycle;
import org.radargun.traits.ProvidesTrait;

@Service(doc = "Terracotta BigMemory 4.3 clustered service")
public class BigMemory43Service implements Lifecycle {

    protected final Log log = LogFactory.getLog(getClass());

    @Property(name = "configPath", doc = "Path of ehcache.xml")
    protected String configPath;

    private CacheManager cacheManager;

    private boolean isServiceRunning;

    @ProvidesTrait
    public BigMemory43Operations getBasicOperations() {
        return new BigMemory43Operations(this);
    }

    @ProvidesTrait
    public Lifecycle getLifecycle() {
        return this;
    }

    /**
     * Start the service.
     */
    @Override
    public void start() {
        if(configPath == null || configPath.isEmpty()) {
            log.fatal("Can not proceed without client configurations (ehcache.xml)");
            return;
        }
        cacheManager = CacheManager.create(configPath);
        isServiceRunning = cacheManager.getCluster(ClusterScheme.TERRACOTTA).isClusterOnline();
        log.info("Is Service Running after start(): "+isServiceRunning);
    }

    /**
     * Graciously shutdown the service.
     */
    @Override
    public void stop() {
        cacheManager.shutdown();
        isServiceRunning = false;
    }

    /**
     * @return True if the service was started but not stopped.
     */
    @Override
    public boolean isRunning() {
        return isServiceRunning;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
