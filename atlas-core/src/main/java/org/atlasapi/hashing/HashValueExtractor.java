package org.atlasapi.hashing;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.hashing.extractors.CountryExtractor;
import org.atlasapi.hashing.extractors.EnumExtractor;
import org.atlasapi.hashing.extractors.Extractor;
import org.atlasapi.hashing.extractors.LocaleExtractor;
import org.atlasapi.hashing.extractors.NativeExtractor;
import org.atlasapi.hashing.extractors.PriceExtractor;
import org.atlasapi.hashing.extractors.SourceStatusExtractor;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for taking an instance of {@link Hashable}, serialising it into a
 * string that can be used to generate a hash value representing the {@link Hashable}. This class
 * will traverse all fields in the {@link Hashable} and all their fields recursively as long as
 * finds data types that it knows how to deal with.
 * <p>
 * This code uses reflection to attempt to serialise different types in a generic fashion. The
 * list of types it supports is not exhaustive however and it is intended that this code should be
 * updated with support for new types as they are needed.
 * <p>
 * This code is fail-fast and will conservatively return absent if it finds a data type it does
 * not explicitly know how to handle. This is to avoid accidentally missing data and returning
 * a partial serialisation that does not fully represent the {@link Hashable}'s state.
 * <p>
 * The generated serialised value is designed to be human readable. This behaviour is only
 * intended to facilitate easier debugging in case of issues and should not be relied upon as
 * guaranteed.
 */
public class HashValueExtractor {

    private static final Logger log = LoggerFactory.getLogger(HashValueExtractor.class);

    private static final String FIELD_DELIMITER = " | ";
    private static final String ITERABLE_DELIMITER = ", ";
    private static final String NESTED_HASHABLE_START = " { ";
    private static final String NESTED_HASHABLE_END = " }";
    private static final String MAP_DELIMITER = "/";
    private static final String CLASS_TYPE_VALUE_FORMAT_STRING = "%s: { %s }";

    private final ImmutableList<Extractor> extractors;

    private HashValueExtractor() {
        extractors = ImmutableList.<Extractor>builder()
                .add(NativeExtractor.create())
                .add(EnumExtractor.create())
                .add(CountryExtractor.create())
                .add(PriceExtractor.create())
                .add(LocaleExtractor.create())
                .add(SourceStatusExtractor.create())
                .build();
    }

    public static HashValueExtractor create() {
        return new HashValueExtractor();
    }

    public Optional<String> getValueToHash(Object object) {
        try {
            String valueToHash = getValueToHashInternal(object);
            return Optional.of(valueToHash);
        } catch (Exception e) {
            log.warn("Failed to extract value to hash for object of type {}",
                    object.getClass().getCanonicalName(), e);
            return Optional.empty();
        }
    }

    private String getValueToHashInternal(@Nullable Object object) {
        if (object == null) {
            return "";
        }
        return extractValue(object);
    }

    private String wrapValueWithTypeInformation(Object object, String valueToHash) {
        return String.format(
                CLASS_TYPE_VALUE_FORMAT_STRING,
                object.getClass().getCanonicalName(),
                valueToHash
        );
    }

    private String extractValueFromFields(Object fieldContainingObject) {
        // We are using Java reflection APIs directly rather than via Apache BeanUtils because
        // we need to access private fields as well which is out of scope for that lib
        return getFields(fieldContainingObject)
                .stream()
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !field.isAnnotationPresent(ExcludeFromHash.class))
                .map(field -> getFieldObject(field, fieldContainingObject))
                .filter(Objects::nonNull)
                .map(this::extractValue)
                .collect(Collectors.joining(FIELD_DELIMITER));
    }

    private ImmutableList<Field> getFields(Object object) {
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();

        for (Class<?> clazz = object.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.equals(Object.class)) {
                break;
            }

            for (Field field : clazz.getDeclaredFields()) {
                fieldsBuilder.add(field);
            }
        }

        return fieldsBuilder.build();
    }

    private String extractValue(Object object) {
        // Try to obtain a value from self-contained (non-recursive) extractors
        Optional<String> extractedValueOptional = extractors.stream()
                .map(extractor -> extractor.extractValue(object))
                .flatMap(value -> value.isPresent() ? Stream.of(value.get()) : Stream.empty())
                .findFirst();

        if (extractedValueOptional.isPresent()) {
            return wrapValueWithTypeInformation(object, extractedValueOptional.get());
        }

        String extractedValue;

        // Try to obtain a value from extractors that recursively call methods in this class
        if (object instanceof Hashable) {
            extractedValue = extractValueFromFields(object);
        } else if (object instanceof Iterable) {
            extractedValue = getIterableValue((Iterable<?>) object);
        } else if (object instanceof Map) {
            extractedValue = getMapValue((Map<?, ?>) object);
        } else if (object.getClass().isArray()) {
            extractedValue = getArrayValue(object);
        } else if (object instanceof com.google.common.base.Optional) {
            extractedValue = getGuavaOptionalValue((com.google.common.base.Optional<?>) object);
        } else if (object instanceof Optional) {
            extractedValue = getOptionalValue((Optional<?>) object);
        } else {
            throw new IllegalArgumentException("Failed to extract value from field of type "
                    + object.getClass().getCanonicalName());
        }

        return wrapValueWithTypeInformation(object, extractedValue);
    }

    @Nullable
    @SuppressWarnings("ThrowFromFinallyBlock")
    private Object getFieldObject(Field field, Object fieldContainingObject) {
        try {
            field.setAccessible(true);
            return field.get(fieldContainingObject);
        } catch (IllegalAccessException e) {
            // This should never happen as we are explicitly making the field accessible
            throw Throwables.propagate(e);
        } finally {
            // We have already called setAccessible before this. If a SecurityException was
            // going to be thrown it would have been thrown before we get here.
            field.setAccessible(false);
        }
    }

    private String getIterableValue(Iterable<?> object) {
        return NESTED_HASHABLE_START
                + StreamSupport.stream(object.spliterator(), false)
                .map(this::getValueToHashInternal)
                .collect(Collectors.joining(ITERABLE_DELIMITER))
                + NESTED_HASHABLE_END;
    }

    private String getMapValue(Map<?, ?> object) {
        return NESTED_HASHABLE_START
                + object.entrySet().stream()
                .map(entry -> extractValue(entry.getKey())
                        + MAP_DELIMITER
                        + getValueToHashInternal(entry.getValue()))
                .collect(Collectors.joining(ITERABLE_DELIMITER))
                + NESTED_HASHABLE_END;
    }

    private String getArrayValue(Object object) {
        List<Object> values = Lists.newArrayList();

        int length = Array.getLength(object);
        for (int i = 0; i < length; i ++) {
            values.add(Array.get(object, i));
        }

        return NESTED_HASHABLE_START
                + values.stream()
                .map(this::getValueToHashInternal)
                .collect(Collectors.joining(ITERABLE_DELIMITER))
                + NESTED_HASHABLE_END;
    }

    @SuppressWarnings("Guava")
    private String getGuavaOptionalValue(com.google.common.base.Optional<?> object) {
        if (object.isPresent()) {
            return getValueToHashInternal(object.get());
        } else {
            return "";
        }
    }

    private String getOptionalValue(Optional<?> object) {
        if (object.isPresent()) {
            return getValueToHashInternal(object.get());
        } else {
            return "";
        }
    }
}
