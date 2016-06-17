package org.radargun.generator;

import org.radargun.stages.cache.generators.ValueGenerator;

import java.util.Random;

/**
 * Created by rahul on 10/06/16.
 */
public class BigMemoryValueGenerator implements ValueGenerator {
    @Override
    public Object generateValue(Object key, int size, Random random) {
        return null;
    }

    @Override
    public int sizeOf(Object value) {
        return 0;
    }

    @Override
    public boolean checkValue(Object value, Object key, int expectedSize) {
        return false;
    }
}
