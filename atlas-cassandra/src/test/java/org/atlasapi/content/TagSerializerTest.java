package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;


public class TagSerializerTest {
    
    private final TagSerializer serializer = new TagSerializer();

    @Test
    public void testDeSerializeTopicRef() {
        Tag tag = new Tag(1234L, null, null, null);
        serializeAndCheck(tag);
        tag.setSupervised(true);
        serializeAndCheck(tag);
        tag.setRelationship(Tag.Relationship.TRANSCRIPTION);
        serializeAndCheck(tag);
        tag.setOffset(1243);
        serializeAndCheck(tag);
        tag.setWeighting(1.0f);
        serializeAndCheck(tag);
    }

    private void serializeAndCheck(Tag tag) {
        Tag deserialized = serializer.deserialize(serializer.serialize(tag));
        assertThat(deserialized.getTopic(), is(tag.getTopic()));
        assertThat(deserialized.isSupervised(), is(tag.isSupervised()));
        assertThat(deserialized.getRelationship(), is(tag.getRelationship()));
        assertThat(deserialized.getWeighting(), is(tag.getWeighting()));
        assertThat(deserialized.getOffset(), is(tag.getOffset()));
    }

}
