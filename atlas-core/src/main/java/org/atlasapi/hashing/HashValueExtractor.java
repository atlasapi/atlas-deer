package org.atlasapi.hashing;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.application.v3.SourceStatus;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Country;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashValueExtractor {

    private static final Logger log = LoggerFactory.getLogger(HashValueExtractor.class);

    private static final String FIELD_DELIMITER = " | ";
    private static final String ITERABLE_DELIMITER = ", ";
    private static final String NESTED_HASHABLE_START = " { ";
    private static final String NESTED_HASHABLE_END = " }";
    private static final String MAP_DELIMITER = "/";
    private static final String CLASS_TYPE_VALUE_FORMAT_STRING = "%s: { %s }";

    // These are the classes from which a valid hash value can be extracted by just
    // calling <code>#toString()</code>
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

    private HashValueExtractor() { }

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
        return getValueToHashFromObject(object);
    }

    private String wrapValueWithTypeInformation(Object object, String valueToHash) {
        return String.format(
                CLASS_TYPE_VALUE_FORMAT_STRING,
                object.getClass().getCanonicalName(),
                valueToHash
        );
    }

    private String getValueToHashFromFields(Object fieldContainingObject) {
        // We are using Java reflection APIs directly rather than via Apache BeanUtils because
        // we need to access private fields as well which is out of scope for that lib
        return getFields(fieldContainingObject)
                .stream()
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !field.isAnnotationPresent(ExcludeFromHash.class))
                .map(field -> getFieldObject(field, fieldContainingObject))
                .filter(Objects::nonNull)
                .map(this::getValueToHashFromObject)
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

    private String getValueToHashFromObject(Object object) {
        Optional<String> extractedValue;

        if (object instanceof Hashable) {
            extractedValue = Optional.of(getValueToHashFromFields(object));
        } else if (object.getClass().isPrimitive() ||
                nativelySupportedTypes.contains(object.getClass())) {
            extractedValue = Optional.of(object.toString());
        } else if (object instanceof Enum) {
            extractedValue = Optional.of(((Enum) object).name().toLowerCase());
        } else if (object instanceof Iterable) {
            extractedValue = Optional.of(NESTED_HASHABLE_START
                    + StreamSupport.stream(((Iterable<?>) object).spliterator(), false)
                    .map(this::getValueToHashInternal)
                    .collect(Collectors.joining(ITERABLE_DELIMITER))
                    + NESTED_HASHABLE_END
            );
        } else if (object instanceof Map) {
            extractedValue = Optional.of(NESTED_HASHABLE_START
                    + ((Map<?,?>) object).entrySet().stream()
                    .map(entry -> getValueToHashFromObject(entry.getKey())
                            + MAP_DELIMITER
                            + getValueToHashInternal(entry.getValue()))
                    .collect(Collectors.joining(ITERABLE_DELIMITER))
                    + NESTED_HASHABLE_END
            );
        } else if (object.getClass().isArray()) {
            List<Object> values = Lists.newArrayList();

            int length = Array.getLength(object);
            for (int i = 0; i < length; i ++) {
                values.add(Array.get(object, i));
            }

            extractedValue = Optional.of(NESTED_HASHABLE_START
                    + values.stream()
                    .map(this::getValueToHashInternal)
                    .collect(Collectors.joining(ITERABLE_DELIMITER))
                    + NESTED_HASHABLE_END
            );
        } else {
            extractedValue = getValueUsingCustomExtractor(object);
        }

        if (!extractedValue.isPresent()) {
            throw new IllegalArgumentException("Failed to extract value from field of type "
                    + object.getClass().getCanonicalName());
        }
        return wrapValueWithTypeInformation(object, extractedValue.get());
    }

    private Optional<String> getValueUsingCustomExtractor(Object object) {
        String extractedValue;

        if (object instanceof com.google.common.base.Optional) {
            com.google.common.base.Optional optionalObject =
                    (com.google.common.base.Optional) object;
            if (optionalObject.isPresent()) {
                extractedValue = getValueToHashInternal(optionalObject.get());
            } else {
                extractedValue = "";
            }
        } else if (object instanceof Optional) {
            Optional optionalObject = (Optional) object;
            if (optionalObject.isPresent()) {
                extractedValue = getValueToHashInternal(optionalObject.get());
            } else {
                extractedValue = "";
            }
        } else if (object instanceof Country) {
            Country country = (Country) object;
            extractedValue = country.getName() + country.code();
        } else if (object instanceof Price) {
            Price price = (Price) object;
            extractedValue = price.getAmount() + price.getCurrency().getCurrencyCode();
        } else if (object instanceof Locale) {
            Locale locale = (Locale) object;
            extractedValue = locale.getLanguage();
        } else if (object instanceof SourceStatus) {
            SourceStatus sourceStatus = (SourceStatus) object;
            extractedValue = sourceStatus.getState().name() + sourceStatus.isEnabled();
        } else {
            return Optional.empty();
        }

        return Optional.of(extractedValue);
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
}
