package org.atlasapi.equivalence;

import java.io.InputStream;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.JacksonMessageSerializer;

import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class EquivalenceGraphUpdateMessageTest {

    private JacksonMessageSerializer<EquivalenceGraphUpdateMessage> serializer
            = JacksonMessageSerializer.forType(EquivalenceGraphUpdateMessage.class);

    @Test
    public void testDeSerializationWithoutCreatedDeleted() throws Exception {
        testSerializingMessageWith(
                EquivalenceGraphUpdate.builder(
                        EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC))
                )
                        .build()
        );
    }

    @Test
    public void testDeSerializationWithoutDeleted() throws Exception {
        testSerializingMessageWith(EquivalenceGraphUpdate
                .builder(EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)))
                .withCreated(
                        ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(
                                Id.valueOf(2),
                                Publisher.BBC
                        )))
                )
                .build()
        );
    }

    @Test
    public void testDeSerializationWithoutCreated() throws Exception {
        testSerializingMessageWith(
                EquivalenceGraphUpdate
                        .builder(
                                EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC))
                        )
                        .withDeleted(ImmutableSet.of(Id.valueOf(1)))
                        .build()
        );
    }

    @Test
    public void testDeSerialization() throws Exception {
        testSerializingMessageWith(
                EquivalenceGraphUpdate
                    .builder(EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)))
                    .withCreated(
                            ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(
                                    Id.valueOf(2),
                                    Publisher.BBC
                            )))
                    )
                    .withDeleted(
                            ImmutableSet.of(Id.valueOf(1))
                    )
                    .build()
        );
    }

    @Test
    public void testDeserialisationWithEquivalenceAssertion() throws Exception {
        EquivalenceGraph updatedGraph = EquivalenceGraph.valueOf(
                new BrandRef(Id.valueOf(1), Publisher.BBC)
        );

        EquivalenceAssertion equivalenceAssertion = EquivalenceAssertion.create(
                new ItemRef(Id.valueOf(100L), Publisher.BBC, "", DateTime.now()),
                ImmutableSet.of(
                        new ItemRef(Id.valueOf(101L), Publisher.BBC, "", DateTime.now())
                ),
                ImmutableSet.of(Publisher.BBC)
        );

        testSerializingMessageWith(
                EquivalenceGraphUpdate
                        .builder(updatedGraph)
                        .withAssertion(equivalenceAssertion)
                        .build()
        );
    }

    @Test
    public void testDeSerializationOfOldGraphUpdateModel() throws Exception {

        BrandRef brandRef1 = new BrandRef(Id.valueOf(1), Publisher.BBC);
        EquivalenceGraph.Adjacents adjacents1 = new EquivalenceGraph.Adjacents(
                brandRef1,
                new DateTime(1470155535169L),
                ImmutableSet.of(brandRef1),
                ImmutableSet.of(brandRef1)
        );
        EquivalenceGraph equivalenceGraph1 = new EquivalenceGraph(
                ImmutableMap.of(brandRef1.getId(), adjacents1),
                new DateTime(1470155535152L)
        );

        BrandRef brandRef2 = new BrandRef(Id.valueOf(2), Publisher.BBC);
        EquivalenceGraph.Adjacents adjacents2 = new EquivalenceGraph.Adjacents(
                brandRef2,
                new DateTime(1470155535188L),
                ImmutableSet.of(brandRef2),
                ImmutableSet.of(brandRef2)
        );
        EquivalenceGraph equivalenceGraph2 = new EquivalenceGraph(
                ImmutableMap.of(brandRef2.getId(), adjacents2),
                new DateTime(1470155535188L)
        );

        EquivalenceGraphUpdate equivalenceGraphUpdate = EquivalenceGraphUpdate.builder(equivalenceGraph1)
                .withCreated(ImmutableSet.of(equivalenceGraph2))
                .withDeleted(ImmutableSet.of(Id.valueOf(1)))
                .build();

        EquivalenceGraphUpdateMessage latestModel = new EquivalenceGraphUpdateMessage(
                "message", Timestamp.of(0), equivalenceGraphUpdate
        );
        byte[] serialised = serializer.serialize(latestModel);

        InputStream modelStream = getClass().getResourceAsStream(
                "/old_equiv_graph_model_byte_array.txt");
        String model = IOUtils.toString(modelStream);
        assertEquals(model, new String(serialised));

        EquivalenceGraphUpdateMessage oldModel = serializer.deserialize(
                model.getBytes()
        );
        EquivalenceGraphUpdateMessage newModel = serializer.deserialize(
                serialised
        );
        assertEquals(oldModel, newModel);
    }

    private void testSerializingMessageWith(EquivalenceGraphUpdate update)
            throws MessagingException {
        EquivalenceGraphUpdateMessage egum =
                new EquivalenceGraphUpdateMessage("message", Timestamp.of(DateTime.now().getMillisOfDay()), update);

        byte[] serialized = serializer.serialize(egum);
        EquivalenceGraphUpdateMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(egum));
        assertThat(deserialized.getGraphUpdate(), is(egum.getGraphUpdate()));
    }
}
