package org.radargun.service;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IMap;
import org.radargun.traits.CacheInformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class HazelcastCacheInfo implements CacheInformation {
   protected final HazelcastService service;

   public HazelcastCacheInfo(HazelcastService service) {
      this.service = service;
   }

   @Override
   public String getDefaultCacheName() {
      return service.mapName;
   }

   @Override
   public Collection<String> getCacheNames() {
      ArrayList<String> names = new ArrayList<String>();
      for (DistributedObject distObj : service.hazelcastInstance.getDistributedObjects()) {
         if(distObj instanceof IMap) {
            names.add(distObj.getName());
         }
      }
      return names;
   }

   @Override
   public Cache getCache(String cacheName) {
      return new Cache(service.getMap(cacheName));
   }

   protected class Cache implements CacheInformation.Cache {
      protected final IMap map;

      public Cache(IMap map) {
         this.map = map;
      }

      @Override
      public long getOwnedSize() {
         return map.getLocalMapStats().getOwnedEntryCount();
      }

      @Override
      public long getLocallyStoredSize() {
         return getMemoryStoredSize();
      }

      @Override
      public long getMemoryStoredSize() {
         return map.getLocalMapStats().getOwnedEntryCount() + map.getLocalMapStats().getBackupEntryCount();
      }

      @Override
      public long getTotalSize() {
         return map.size();
      }

      @Override
      public Map<?, Long> getStructuredSize() {
         return Collections.singletonMap(map.getName(), getOwnedSize());
      }

      @Override
      public int getNumReplicas() {
         return service.hazelcastInstance.getConfig().getMapConfig(map.getName()).getBackupCount() + 1;
      }

      @Override
      public int getEntryOverhead() {
         return -1;
      }
   }
}
