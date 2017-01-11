package org.atlasapi.query.common.coercers;

import java.util.List;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("Guava")
public class EnumCoercer<E extends Enum<E>> implements AttributeCoercer<E> {

    private final Function<String, Optional<E>> translator;

    private EnumCoercer(Function<String, Optional<E>> translator) {
        this.translator = checkNotNull(translator);
    }

    public static <E extends Enum<E>> EnumCoercer<E> create(
            Function<String, Optional<E>> translator
    ) {
        return new EnumCoercer<>(translator);
    }

    @Override
    public List<E> apply(Iterable<String> values) throws InvalidAttributeValueException {
        ImmutableList.Builder<E> enums = ImmutableList.builder();
        List<String> invalid = Lists.newArrayList();

        for (String value : values) {
            Optional<E> possibleEnum = translator.apply(value);
            if (possibleEnum.isPresent()) {
                enums.add(possibleEnum.get());
            } else {
                invalid.add(value);
            }
        }

        if (!invalid.isEmpty()) {
            throw new InvalidAttributeValueException(Joiner.on(", ").join(invalid));
        }

        return enums.build();
    }
}
