package org.radargun.service.gemfire;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import org.radargun.Service;
import org.radargun.config.Property;
import org.radargun.traits.BasicOperations;
import org.radargun.traits.Lifecycle;
import org.radargun.traits.ProvidesTrait;

@Service(doc = "Gemfire 8 Client Service")
public class Gemfire8ClientService extends Gemfire8AbstractService {

    @Override
    public void start() {
        ClientCacheFactory factory = new ClientCacheFactory();
        addLocator(factory);
        cache = factory.create();
    }

    @Override
    protected <K, V> Region<K, V> createRegion(String name) {
        ClientCache clientCache = (ClientCache) this.cache;
        ClientRegionFactory<K, V> clientRegionFactory =
                clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        Region<K, V> region = clientRegionFactory.<K, V>create(name);
        return region;
    }

    private void addLocator(ClientCacheFactory factory) {
        String[] tokens = locatorAddress.split(":");
        factory.addPoolLocator(tokens[0], Integer.parseInt(tokens[1]));
    }


}
