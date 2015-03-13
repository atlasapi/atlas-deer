package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;


public class ContainerSummarySerializerTest {

    private final ContainerSummarySerializer serializer = new ContainerSummarySerializer();
    
    @Test
    public void testDeSerializeContainerSummary() {
        serializeAndCheck(new ContainerSummary(null, null, null, null));
        serializeAndCheck(new ContainerSummary("title", null, null, null));
        serializeAndCheck(new ContainerSummary(null, "desc", null, null));
        serializeAndCheck(new ContainerSummary(null, null, "type", null));
        serializeAndCheck(new ContainerSummary(null, null,  null, 1));
    }

    private void serializeAndCheck(ContainerSummary containerSummary) {
        ContainerSummary deserialized = serializer.deserialize(serializer.serialize(containerSummary));
        assertThat(deserialized.getTitle(), is(containerSummary.getTitle()));
        assertThat(deserialized.getDescription(), is(containerSummary.getDescription()));
        assertThat(deserialized.getType(), is(containerSummary.getType()));
        assertThat(deserialized.getSeriesNumber(), is(containerSummary.getSeriesNumber()));
    }

}
