package org.atlasapi.entity;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class AwardSerializerTest {

    private final AwardSerializer serializer = new AwardSerializer();

    @Test
    public void awardSerializerTest() {
        Award award = new Award();
        award.setOutcome("won");
        award.setDescription("Best Actor");
        award.setTitle("BAFTA");
        award.setYear("2009");

        Award deserialized = serializer.deserialize(serializer.serialize(award));

        assertThat(award.getOutcome(), is(deserialized.getOutcome()));
        assertThat(award.getTitle(), is(deserialized.getTitle()));
        assertThat(award.getDescription(), is(deserialized.getDescription()));
        assertThat(award.getYear(), is(deserialized.getYear()));
    }

}
