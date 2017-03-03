package org.atlasapi.query;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryOrderingTest {

    @Test
    public void defaultsToAscending() {
        QueryOrdering ordering = QueryOrdering.fromOrderBy("foo,bar.desc,baz");

        ImmutableList<QueryOrdering.Clause> sortOrder = ordering.getSortOrder();

        assertEquals("foo", sortOrder.get(0).getPath());
        assertTrue(sortOrder.get(0).isAscending());

        assertEquals("bar", sortOrder.get(1).getPath());
        assertFalse(sortOrder.get(1).isAscending());

        assertEquals("baz", sortOrder.get(2).getPath());
        assertTrue(sortOrder.get(2).isAscending());
    }
}
