package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.serialization.protobuf.ContentProtos;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

public class RestrictionSerializerTest {
    
    private final RestrictionSerializer serializer = new RestrictionSerializer();

    @Test
    public void testDeSerializesRestrictionMessage() throws InvalidProtocolBufferException {

        Restriction restriction = Restriction.from(14, "old");
        
        byte[] bytes = serializer.serialize(restriction).build().toByteArray();
        
        Restriction deserialized = serializer.deserialize(ContentProtos.Restriction.parseFrom(bytes));

        assertThat(deserialized.getMessage(), is(restriction.getMessage()));
        assertThat(deserialized.isRestricted(), is(restriction.isRestricted()));
        assertThat(deserialized.getMinimumAge(), is(restriction.getMinimumAge()));
        assertThat(deserialized.getAuthority(), is(restriction.getAuthority()));
        assertThat(deserialized.getRating(), is(restriction.getRating()));

    }
}
