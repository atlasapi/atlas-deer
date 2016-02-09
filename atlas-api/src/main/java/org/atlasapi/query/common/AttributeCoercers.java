package org.atlasapi.query.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.text.MoreStrings;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkNotNull;

public final class AttributeCoercers {

    private AttributeCoercers() {
    }

    public static final AttributeCoercer<String, Id> idCoercer(NumberToShortStringCodec idCodec) {
        return new IdStringCoercer(idCodec);
    }

    public static final AttributeCoercer<String, Publisher> sourceIdCoercer(
            SourceIdCodec sourceIdCodec) {
        return new SourceIdStringCoercer(sourceIdCodec);
    }

    private static class IdStringCoercer extends AbstractAttributeCoercer<String, Id> {

        private final NumberToShortStringCodec idCodec;

        public IdStringCoercer(NumberToShortStringCodec idCodec) {
            this.idCodec = checkNotNull(idCodec);
        }

        @Override
        protected Id coerce(String input) {
            return Id.valueOf(idCodec.decode(input));
        }

    }

    private static class SourceIdStringCoercer extends AbstractAttributeCoercer<String, Publisher> {

        private final SourceIdCodec sourceIdCodec;

        public SourceIdStringCoercer(SourceIdCodec sourceIdCodec) {
            this.sourceIdCodec = sourceIdCodec;
        }

        @Override
        protected Publisher coerce(String input) {
            Optional<Publisher> publisher = sourceIdCodec.decode(input);
            if (publisher.isPresent()) {
                return publisher.get();
            } else {
                return null;
            }
        }

    }

    public static final AttributeCoercer<String, String> stringCoercer() {
        return StringStringCoercer.INSTANCE;
    }

    private enum StringStringCoercer implements AttributeCoercer<String, String> {
        INSTANCE;

        @Override
        public List<String> apply(Iterable<String> input) {
            return ImmutableList.copyOf(input);
        }
    }

    public static final AttributeCoercer<String, Boolean> booleanCoercer() {
        return StringBooleanCoercer.INSTANCE;
    }

    private enum StringBooleanCoercer implements AttributeCoercer<String, Boolean> {
        INSTANCE;

        private final ImmutableSet<String> validInput =
                ImmutableSet.copyOf(Lists.transform(
                        ImmutableList.of(Boolean.TRUE, Boolean.FALSE),
                        Functions.compose(MoreStrings.toLower(), Functions.toStringFunction())
                        )
                );

        @Override
        public List<Boolean> apply(@Nullable Iterable<String> input)
                throws InvalidAttributeValueException {
            HashSet<Boolean> values = Sets.newHashSet();
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

    public static final AttributeCoercer<String, Float> floatCoercer() {
        return FloatCoercer.INSTANCE;
    }

    private enum FloatCoercer implements AttributeCoercer<String, Float> {
        INSTANCE;

        @Override
        public List<Float> apply(Iterable<String> values) throws InvalidAttributeValueException {
            ImmutableList.Builder<Float> floats = ImmutableList.builder();
            for (String value : values) {
                try {
                    floats.add(Float.valueOf(value));
                } catch (NumberFormatException nfe) {
                    throw new InvalidAttributeValueException(value);
                }
            }
            return floats.build();
        }
    }

    public static final <E extends Enum<E>> AttributeCoercer<String, E> enumCoercer(
            final Function<String, Optional<E>> translator) {
        return new AttributeCoercer<String, E>() {

            @Override
            public List<E> apply(Iterable<String> values) throws InvalidAttributeValueException {
                ImmutableList.Builder<E> enums = ImmutableList.builder();
                ArrayList<String> invalid = Lists.newArrayList();
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
        };
    }

}
