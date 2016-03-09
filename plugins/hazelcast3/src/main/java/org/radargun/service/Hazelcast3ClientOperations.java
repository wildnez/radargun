package org.radargun.service;

import com.hazelcast.core.IMap;
import org.radargun.traits.BasicOperations;

public class Hazelcast3ClientOperations implements BasicOperations {

    private final Hazelcast3ClientService service;

    public Hazelcast3ClientOperations(Hazelcast3ClientService service) {
        this.service = service;
    }

    @Override
    public <K, V> BasicOperations.Cache<K, V> getCache(String cacheName) {
        return new Cache<>(service.<K, V>getMap(cacheName));
    }

    protected static class Cache<K, V> implements BasicOperations.Cache<K, V> {
        protected final IMap<K, V> map;

        public Cache(IMap<K, V> map) {
            this.map = map;
        }

        @Override
        public V get(K key) {
            return map.get(key);
        }

        @Override
        public boolean containsKey(K key) {
            return map.containsKey(key);
        }

        @Override
        public void put(K key, V value) {
            map.put(key, value);
        }

        @Override
        public V getAndPut(K key, V value) {
            return map.put(key, value);
        }

        @Override
        public boolean remove(K key) {
            return map.remove(key) != null;
        }

        @Override
        public V getAndRemove(K key) {
            return map.remove(key);
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

}
