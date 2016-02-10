package org.atlasapi.equivalence;

import org.atlasapi.content.BrandRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.JacksonMessageSerializer;

import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EquivalenceGraphUpdateMessageTest {

    JacksonMessageSerializer<EquivalenceGraphUpdateMessage> serializer
            = JacksonMessageSerializer.forType(EquivalenceGraphUpdateMessage.class);

    @Test
    public void testDeSerializationWithoutCreatedDeleted() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
                EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
                ImmutableSet.<EquivalenceGraph>of(),
                ImmutableSet.<Id>of()
        ));
    }

    @Test
    public void testDeSerializationWithoutDeleted() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
                EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
                ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(
                        Id.valueOf(2),
                        Publisher.BBC
                ))),
                ImmutableSet.<Id>of()
        ));
    }

    @Test
    public void testDeSerializationWithoutCreated() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
                EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
                ImmutableSet.<EquivalenceGraph>of(),
                ImmutableSet.of(Id.valueOf(1))
        ));
    }

    @Test
    public void testDeSerialization() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
                EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
                ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(
                        Id.valueOf(2),
                        Publisher.BBC
                ))),
                ImmutableSet.of(Id.valueOf(1))
        ));
    }

    private void testSerializingMessageWith(EquivalenceGraphUpdate update)
            throws MessagingException {
        EquivalenceGraphUpdateMessage egum =
                new EquivalenceGraphUpdateMessage("message", Timestamp.of(0), update);

        byte[] serialized = serializer.serialize(egum);

        EquivalenceGraphUpdateMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(egum));
        assertThat(deserialized.getGraphUpdate(), is(egum.getGraphUpdate()));
    }
}
