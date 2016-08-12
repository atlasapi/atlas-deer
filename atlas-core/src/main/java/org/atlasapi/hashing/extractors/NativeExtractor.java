package org.atlasapi.hashing.extractors;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

/**
 * Extractor for classes from which a value representing their state can be extracted by just
 * calling <code>#toString()</code>
 */
public class NativeExtractor extends Extractor {

    private final ImmutableList<Class<?>> nativelySupportedTypes = ImmutableList.<Class<?>>builder()
            .add(Boolean.class)
            .add(Byte.class)
            .add(Character.class)
            .add(Short.class)
            .add(Integer.class)
            .add(Long.class)
            .add(Float.class)
            .add(Double.class)
            .add(String.class)
            .add(Interval.class)
            .add(Duration.class)
            .add(LocalDate.class)
            .add(LocalTime.class)
            .add(DateTime.class)
            .build();

    private NativeExtractor() {
    }

    public static NativeExtractor create() {
        return new NativeExtractor();
    }

    @Override
    protected boolean isSupported(Object object) {
        return object.getClass().isPrimitive()
                || nativelySupportedTypes.contains(object.getClass());
    }

    @Override
    protected String extractValueInternal(Object object) {
        return object.toString();
    }
}
