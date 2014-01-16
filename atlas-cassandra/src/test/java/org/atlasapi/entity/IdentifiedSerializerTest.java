package org.atlasapi.entity;

import static org.junit.Assert.assertThat;
import org.testng.annotations.Test;
import static org.hamcrest.Matchers.is;
import org.atlasapi.content.Item;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.joda.time.DateTime;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.time.DateTimeZones;


public class IdentifiedSerializerTest {

    private final IdentifiedSerializer serializer = new IdentifiedSerializer();
    
    @Test
    public void testDeSerializeIdentified() {
        Item identified = new Item();
        identified.setId(Id.valueOf(1234));
        identified.setLastUpdated(new DateTime(DateTimeZones.UTC));
        identified.setAliases(ImmutableSet.of(new Alias("a","alias1"),new Alias("b","alias2")));
        identified.setCanonicalUri("canonicalUri");
        identified.setEquivalenceUpdate(new DateTime(DateTimeZones.UTC));
        identified.setEquivalentTo(ImmutableSet.of(new EquivalenceRef(Id.valueOf(1),Publisher.BBC)));
        
        CommonProtos.Identification serialized = serializer.serialize(identified).build();
        
        Item deserialized = serializer.deserialize(serialized, new Item());
        
        assertThat(deserialized.getId(), is(identified.getId()));
        assertThat(deserialized.getAliases(), is(identified.getAliases()));
        assertThat(deserialized.getCanonicalUri(), is(identified.getCanonicalUri()));
        assertThat(deserialized.getEquivalentTo(), is(identified.getEquivalentTo()));
        assertThat(deserialized.getLastUpdated(), is(identified.getLastUpdated()));
    }

}
