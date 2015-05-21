package org.atlasapi.system.bootstrap.workers;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.serialization.json.JsonFactory;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;

public class ContentEquivalenceAssertionLegacyMessageSerializerTest {

    private final MessageSerializer<EquivalenceAssertionMessage> serializer
        = new ContentEquivalenceAssertionLegacyMessageSerializer();
    
    @Test
    public void testDeSerializesContentEquivalenceAssertionMessage() throws Exception {
        ImmutableList<AdjacentRef> adjs = ImmutableList.of(new AdjacentRef("cf2","item",Publisher.PA.key()));
        Set<String> srcs = toKeys(Publisher.all());
        ContentEquivalenceAssertionMessage msg = new ContentEquivalenceAssertionMessage(
            "1", Timestamp.of(1L), "cyp", "item", "bbc.co.uk", adjs, srcs);
        
        byte[] serialized = JsonFactory.makeJsonMapper().writeValueAsBytes(msg);
        
        EquivalenceAssertionMessage deserialized
            = serializer.deserialize(serialized);
        
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getSubject().getId().longValue(), is(SubstitutionTableNumberCodec.lowerCaseOnly().decode(msg.getEntityId()).longValue()));
        assertThat(deserialized.getSubject().getSource().toString(), is(msg.getEntitySource()));
        assertThat(deserialized.getSubject().getResourceType(), is(ResourceType.CONTENT));
        assertThat(toKeys(deserialized.getPublishers()), is(msg.getSources()));
        assertThat(Iterables.getOnlyElement(deserialized.getAssertedAdjacents()).getId().longValue(), 
                is(SubstitutionTableNumberCodec.lowerCaseOnly().decode(Iterables.getOnlyElement(adjs).getId()).longValue()));
        assertThat(Iterables.getOnlyElement(deserialized.getAssertedAdjacents()).getSource().key(),
                is(Iterables.getOnlyElement(adjs).getSource()));
        assertThat(Iterables.getOnlyElement(deserialized.getAssertedAdjacents()), instanceOf(ItemRef.class));
        
    }

    private Set<String> toKeys(Set<Publisher> pubs) {
        return ImmutableSet.copyOf(Iterables.transform(pubs,Publisher.TO_KEY));
    }
}
