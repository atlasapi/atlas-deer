package org.atlasapi.util;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ElasticsearchUtilsTest {

    @Test
    public void testClampDateToFloorMinute() throws Exception {
        DateTime now = DateTime.now();
        DateTime clamped = ElasticsearchUtils.clampDateToFloorMinute(now);
        assertThat(clamped, is(now.minusSeconds(now.getSecondOfMinute()).minusMillis(now.getMillisOfSecond())));
    }
}