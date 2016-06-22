package org.radargun.service;

import net.sf.ehcache.Element;
import org.radargun.config.Property;
import org.radargun.traits.BasicOperations;

public class BigMemory43Operations implements BasicOperations {

    private BigMemory43Service service;

    @Property(name = "cacheName", doc = "Name of the cache")
    protected String cacheName = "defaultRadargunCache";

    public BigMemory43Operations(BigMemory43Service service) {
        this.service = service;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        if(cacheName == null || cacheName.isEmpty()) {
            cacheName = this.cacheName;
        }
        return new BigMemoryCache(cacheName);
    }

    private class BigMemoryCache implements BasicOperations.Cache {

        private net.sf.ehcache.Cache cache;

        BigMemoryCache(String cacheName) {
            cache = service.getCacheManager().getCache(cacheName);
        }

        @Override
        public Object get(Object key) {
            return cache.get(key).getObjectValue();
        }

        @Override
        public boolean containsKey(Object key) {
            return cache.isKeyInCache(key);
        }

        @Override
        public void put(Object key, Object value) {
            cache.put(new Element(key, value));
        }

        @Override
        public Object getAndPut(Object key, Object value) {
            return cache.replace(new Element(key, value));
        }

        @Override
        public boolean remove(Object key) {
            return cache.remove(key);
        }

        @Override
        public Object getAndRemove(Object key) {
            return cache.removeAndReturnElement(key);
        }

        @Override
        public void clear() {
            cache.removeAll();
        }
    }
}
