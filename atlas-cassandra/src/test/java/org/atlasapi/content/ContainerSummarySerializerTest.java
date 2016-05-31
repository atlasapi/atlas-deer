package org.atlasapi.content;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ContainerSummarySerializerTest {

    private final ContainerSummarySerializer serializer = new ContainerSummarySerializer();

    @Test
    public void testDeSerializeContainerSummary() {
        serializeAndCheck(ContainerSummary.create(null, null, null, null, null));
        serializeAndCheck(ContainerSummary.create("title", null, null, null, null));
        serializeAndCheck(ContainerSummary.create(null, "desc", null, null, null));
        serializeAndCheck(ContainerSummary.create(null, null, "type", null, null));
        serializeAndCheck(ContainerSummary.create(null, null, null, 1, null));
        serializeAndCheck(ContainerSummary.create(null, null, null, null, 10));
    }

    private void serializeAndCheck(ContainerSummary containerSummary) {
        ContainerSummary deserialized = serializer.deserialize(
                serializer.serialize(containerSummary)
        );
        assertThat(deserialized.getTitle(), is(containerSummary.getTitle()));
        assertThat(deserialized.getDescription(), is(containerSummary.getDescription()));
        assertThat(deserialized.getType(), is(containerSummary.getType()));
        assertThat(deserialized.getSeriesNumber(), is(containerSummary.getSeriesNumber()));
        assertThat(deserialized.getTotalEpisodes(), is(containerSummary.getTotalEpisodes()));
    }
}
