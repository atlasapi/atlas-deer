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

import org.atlasapi.entity.Id;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.TimeMachine;

import org.joda.time.Duration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BroadcastTest {

    private final Clock clock = new TimeMachine();

    @Test
    public void testEqualBroadcasts() throws Exception {

        assertEquals(
                new Broadcast(Id.valueOf(1), clock.now(), clock.now().plusHours(1)),
                new Broadcast(Id.valueOf(1), clock.now(), clock.now().plusHours(1))
        );

        assertEquals(
                new Broadcast(Id.valueOf(1), clock.now(), Duration.standardHours(1)),
                new Broadcast(Id.valueOf(1), clock.now(), clock.now().plusHours(1))
        );

        assertEquals(
                new Broadcast(Id.valueOf(1), clock.now(), Duration.standardHours(1)),
                new Broadcast(Id.valueOf(1), clock.now(), Duration.standardHours(1))
        );

    }

    @Test
    public void testUnequalBroadcasts() throws Exception {

        assertFalse(new Broadcast(
                Id.valueOf(1),
                clock.now(),
                Duration.standardHours(1)
        ).equals(new Broadcast(Id.valueOf(2), clock.now(), Duration.standardHours(1))));

        assertFalse(new Broadcast(
                Id.valueOf(1),
                clock.now(),
                Duration.standardHours(2)
        ).equals(new Broadcast(Id.valueOf(1), clock.now(), Duration.standardHours(1))));

        assertFalse(new Broadcast(
                Id.valueOf(1),
                clock.now().plusSeconds(1),
                clock.now().plusHours(1)
        ).equals(new Broadcast(Id.valueOf(1), clock.now(), clock.now().plusHours(1))));

    }
}
