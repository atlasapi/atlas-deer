package org.atlasapi.query.common.coercers;

import java.util.List;
import java.util.Set;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class BooleanCoercer implements AttributeCoercer<Boolean> {

    private final ImmutableSet<String> validInput =
            ImmutableSet.of(Boolean.TRUE, Boolean.FALSE)
                    .stream()
                    .map(Object::toString)
                    .map(String::toLowerCase)
                    .collect(MoreCollectors.toImmutableSet());

    private BooleanCoercer() {
    }

    public static BooleanCoercer create() {
        return new BooleanCoercer();
    }

    @Override
    public List<Boolean> apply(Iterable<String> input) throws InvalidAttributeValueException {
        Set<Boolean> values = Sets.newHashSet();

        for (String value : input) {
            String lowerCaseValue = value.toLowerCase();

            if (!validInput.contains(lowerCaseValue)) {
                throw new InvalidAttributeValueException(value);
            }

            values.add(Boolean.valueOf(lowerCaseValue));
        }

        return ImmutableSet.copyOf(values).asList();
    }
}
