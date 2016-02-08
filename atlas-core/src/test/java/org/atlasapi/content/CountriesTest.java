/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import com.metabroadcast.common.intl.Countries;

import com.google.common.collect.Sets;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CountriesTest {

    @Test
    public void testResolvingCodes() throws Exception {
        assertEquals(Countries.GB, Countries.fromCode("gb"));
        assertEquals(Countries.GB, Countries.fromCode("uk"));
        assertEquals(Countries.GB, Countries.fromCode("GB"));
        assertEquals(Countries.IE, Countries.fromCode("ie"));
        assertNull(Countries.fromCode("5"));
    }

    @Test
    public void testFromList() throws Exception {
        assertEquals(Sets.newHashSet(Countries.GB), Countries.fromDelimtedList("gb"));
        assertEquals(
                Sets.newHashSet(Countries.GB, Countries.IE),
                Countries.fromDelimtedList("uk ie")
        );
        assertEquals(
                Sets.newHashSet(Countries.GB, Countries.IE),
                Countries.fromDelimtedList("uk,  ie")
        );
        assertEquals(
                Sets.newHashSet(Countries.GB, Countries.IE),
                Countries.fromDelimtedList("uk,  ; ie")
        );
    }
}
