package org.atlasapi.comparison;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.List;

public class ObjectComparer extends Comparer {

    private final List<Comparer> comparers;

    public ObjectComparer() {
        IterableComparer iterableComparer = new IterableComparer(this);

        // Order matters, in particular iterable should happen after
        this.comparers = ImmutableList.of(
                new NativeComparer(),
                new SetComparer(this),
                new ListComparer(iterableComparer),
                iterableComparer
        );
    }

    @Override
    public boolean equals(@Nullable Object o1, @Nullable Object o2) {
        if(o1 == o2) {
            return true;
        }
        if (o1 == null || o2 == null) {
            return false;
        }

        for (Comparer comparer : comparers) {
            if (comparer.isSupported(o1, o2)) {
                boolean isEqual = comparer.equals(o1, o2);
                return isEqual;
            }
        }

        if (o1.getClass() != o2.getClass()) {
            return false;
        }

        ImmutableList<Field> fields = getFields(o1);
        for (Field field : fields) {
            Object fieldValue1 = getFieldObject(field, o1);
            Object fieldValue2 = getFieldObject(field, o2);

            boolean isFieldEqual = equals(fieldValue1, fieldValue2);
            if(!isFieldEqual) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isSupported(Object object1) {
        return true;
    }

    private String[] getIgnoringFields(Object object) {
        List<String> ignoredFields = getIgnoredFields(object);
        String[] ignoredFieldsArray = new String[ignoredFields.size()];
        return ignoredFields.toArray(ignoredFieldsArray);
    }

    private ImmutableList<Field> getFields(Object object) {
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();

        for (Class<?> clazz = object.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.equals(Object.class)) {
                break;
            }

            for (Field field : clazz.getDeclaredFields()) {
                if (!field.isAnnotationPresent(ExcludeFromObjectComparison.class)) {
                    fieldsBuilder.add(field);
                }
            }
        }

        return fieldsBuilder.build();
    }

    private ImmutableList<String> getIgnoredFields(Object object) {
        ImmutableList.Builder<String> fieldsBuilder = ImmutableList.builder();

        for (Class<?> clazz = object.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.equals(Object.class)) {
                break;
            }

            for (Field field : clazz.getDeclaredFields()) {
                if (field.isAnnotationPresent(ExcludeFromObjectComparison.class)) {
                    fieldsBuilder.add(field.getName());
                }
            }
        }

        return fieldsBuilder.build();
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
