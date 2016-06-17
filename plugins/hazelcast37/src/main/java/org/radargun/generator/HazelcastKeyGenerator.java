package org.radargun.generator;

import org.radargun.stages.cache.generators.KeyGenerator;

/**
 * Created by rahul on 17/06/16.
 */
public class HazelcastKeyGenerator implements KeyGenerator {

    @Override
    public Object generateKey(long keyIndex) {
        return Long.toHexString(keyIndex).getBytes();
    }
}
