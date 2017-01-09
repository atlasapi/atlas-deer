package org.atlasapi.query.common.coercers;

import java.util.List;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

public interface AttributeCoercer<O> {

    /**
     * @param values request parameter values
     * @return input values coerced into type {@link O}
     */
    List<O> apply(Iterable<String> values) throws InvalidAttributeValueException;
}
