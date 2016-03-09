package org.radargun.service.gemfire;


import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import org.radargun.config.Property;
import org.radargun.traits.BasicOperations;
import org.radargun.traits.Lifecycle;
import org.radargun.traits.ProvidesTrait;

public abstract class Gemfire8AbstractService implements Lifecycle {

    protected GemFireCache cache;

    @Property(name = "cacheName", doc = "Name of the cache. Default is 'testCache'")
    protected String cacheName = "testCache";

    @Property(name = "locatorAddress", doc = "Locator address to discover server members")
    protected String locatorAddress;

    // This method is crucial here, it exposes the Lifecycle trait,
    // without it the "start" method won't be invoked
    @ProvidesTrait
    public Lifecycle getLifecycle() {
        return this;
    }

    protected abstract  <K, V> Region<K, V> createRegion(String name);

    @Override
    public void stop() {
        cache.close();
    }

    @Override
    public boolean isRunning() {
        return cache != null && cache.getDistributedSystem().isConnected();
    }

    public <K, V> Region<K, V> getOrCreateRegion(String name) {
        /*

        There are two ways to specify the cache name used for tests.
        First one with the plugin setup:

            <default xmlns="urn:radargun:plugins:gemfire82:2.1" cache-name="theCache"/>

        Second with the stage setup:

            <load-data num-entries="10000">
                <cache-selector>
                    <use-cache cache="testCache"/>
                </cache-selector>
            </load-data>

         In this method name parameter will be null unless you pass a cache selector to a stage
         */
        String actualName = name != null ? name : cacheName;

        Region<K, V> region = cache.getRegion(actualName);

        if (region != null) {
            return region;
        }

        return createRegion(actualName);
    }

    public class Gemfire8Operations implements BasicOperations {

        @Override
        public <K, V> BasicOperations.Cache<K, V> getCache(String cacheName) {
            Region<K, V> region = Gemfire8AbstractService.this.getOrCreateRegion(cacheName);
            return new Gemfire8Cache<>(region);
        }

    }
}
