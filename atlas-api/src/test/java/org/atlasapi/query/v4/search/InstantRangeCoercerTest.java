package org.atlasapi.query.v4.search;

import com.google.common.collect.ImmutableList;
import joptsimple.internal.Strings;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;
import org.atlasapi.query.v4.search.coercer.InstantRangeCoercer;
import org.atlasapi.query.v4.search.coercer.Range;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class InstantRangeCoercerTest {

    private final InstantRangeCoercer instantRangeCoercer = InstantRangeCoercer.create();

    @Test
    public void testInstantCoercion() throws InvalidAttributeValueException {

        List<String[]> datesList = ImmutableList.of(
                new String[] { "2014-02-01T11:00:00Z", "2014-02-01T11:00:00Z" },
                new String[] { "2014-02-01T11:00:00.0Z", "2014-02-01T11:00:00.0Z" },
                new String[] { "2014-02-01T11:00:00.00Z", "2014-02-01T11:00:00.00Z" },
                new String[] { "2014-02-01T11:00:00.000Z", "2014-02-01T11:00:00.000Z" }
        );

        List<Range<Instant>> ranges = instantRangeCoercer.apply(
                datesList.stream()
                    .map(a -> Strings.join(a, "-"))
                    .collect(Collectors.toList())
        );

        for (int i = 0; i < datesList.size(); i++) {
            String[] dates = datesList.get(i);
            Range<Instant> range = ranges.get(i);
            Assert.assertEquals(Instant.parse(dates[0]), range.getFrom());
            Assert.assertEquals(Instant.parse(dates[1]), range.getTo());
        }
    }

    @Test(expected = InvalidAttributeValueException.class)
    public void testInstantCoercionFails() throws InvalidAttributeValueException {

        List<String[]> datesList = ImmutableList.of(
                new String[] { "2014-01-02", "2014-01-02" }
        );

        List<Range<Instant>> ranges = instantRangeCoercer.apply(
                datesList.stream()
                    .map(a -> Strings.join(a, "-"))
                    .collect(Collectors.toList())
        );
    }
}
