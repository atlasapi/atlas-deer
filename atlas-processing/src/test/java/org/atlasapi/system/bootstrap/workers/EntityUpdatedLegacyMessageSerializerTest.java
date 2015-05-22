package org.atlasapi.system.bootstrap.workers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.entity.ResourceType;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.serialization.json.JsonFactory;
import org.junit.Test;

import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;


public class EntityUpdatedLegacyMessageSerializerTest {

    MessageSerializer<ResourceUpdatedMessage> serializer = new EntityUpdatedLegacyMessageSerializer();
    
    @Test
    public void testDeSerializesLegacyMessage() throws Exception {
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), "cyp", "item", "bbc.co.uk");
        
        byte[] serialized = JsonFactory.makeJsonMapper().writeValueAsBytes(msg);
        
        org.atlasapi.messaging.ResourceUpdatedMessage deserialized
            = serializer.deserialize(serialized);
        
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource().getId().longValue(), is(1225L));
        assertThat(deserialized.getUpdatedResource().getSource().toString(), is(msg.getEntitySource()));
        assertThat(deserialized.getUpdatedResource().getResourceType(), is(ResourceType.CONTENT));
        
    }

}
