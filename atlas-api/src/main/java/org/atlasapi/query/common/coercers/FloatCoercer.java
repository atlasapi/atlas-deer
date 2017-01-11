package org.atlasapi.query.common.coercers;

import java.util.List;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.google.common.collect.ImmutableList;

public class FloatCoercer implements AttributeCoercer<Float> {

    private FloatCoercer() {
    }

    public static FloatCoercer create() {
        return new FloatCoercer();
    }

    @Override
    public List<Float> apply(Iterable<String> values) throws InvalidAttributeValueException {
        ImmutableList.Builder<Float> floats = ImmutableList.builder();

        for (String value : values) {
            try {
                floats.add(Float.valueOf(value));
            } catch (NumberFormatException e) {
                throw new InvalidAttributeValueException(value, e);
            }
        }

        return floats.build();
    }
}
