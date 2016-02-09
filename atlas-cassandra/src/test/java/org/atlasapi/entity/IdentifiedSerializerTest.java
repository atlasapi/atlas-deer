package org.atlasapi.entity;

import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IdentifiedSerializerTest {

    private final IdentifiedSerializer<Identified> serializer = new IdentifiedSerializer<>();

    @Test
    public void testDeSerializeIdentified() {
        Identified identified = getIdentified();

        CommonProtos.Identification serialized = serializer.serialize(identified);

        Identified deserialized = serializer.deserialize(serialized, new Identified());

        checkIdentified(identified, deserialized);
    }

    @Test
    public void testDeserializeUsingBuilder() throws Exception {
        Identified identified = getIdentified();

        CommonProtos.Identification serialized = serializer.serialize(identified);

        Identified deserialized = serializer.deserialize(serialized, Identified.builder()).build();

        checkIdentified(identified, deserialized);
    }

    public Identified getIdentified() {
        Identified identified = new Identified();

        identified.setId(Id.valueOf(1234));
        identified.setCanonicalUri("canonicalUri");
        identified.setCurie("curie");
        identified.setAliases(ImmutableSet.of(new Alias("a", "alias1"), new Alias("b", "alias2")));
        identified.setEquivalentTo(ImmutableSet.of(
                new EquivalenceRef(Id.valueOf(1), Publisher.BBC)
        ));
        identified.setLastUpdated(new DateTime(DateTimeZones.UTC));
        identified.setEquivalenceUpdate(new DateTime(DateTimeZones.UTC));

        return identified;
    }

    public void checkIdentified(Identified actual, Identified expected) {
        assertThat(expected.getId(), is(actual.getId()));
        assertThat(expected.getCanonicalUri(), is(actual.getCanonicalUri()));
        assertThat(expected.getCurie(), is(actual.getCurie()));
        assertThat(expected.getAliases(), is(actual.getAliases()));
        assertThat(expected.getEquivalentTo(), is(actual.getEquivalentTo()));
        assertThat(expected.getLastUpdated(), is(actual.getLastUpdated()));
        assertThat(expected.getEquivalenceUpdate(), is(actual.getEquivalenceUpdate()));
    }
}
