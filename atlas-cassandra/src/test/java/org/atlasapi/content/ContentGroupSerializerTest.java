package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ContentGroupSerializerTest {

    private ContentGroupSerializer<ContentGroup> serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new ContentGroupSerializer<>();
    }

    @Test
    public void testSerialization() throws Exception {
        ContentGroup expected = new ContentGroup();

        expected.setType(ContentGroup.Type.FRANCHISE);
        expected.addContent(new ItemRef(Id.valueOf(0L), Publisher.BBC, "sort",
                DateTime.now().withZone(DateTimeZone.UTC)
        ));
        expected.setTitle("title");

        CommonProtos.ContentGroup serialized = serializer.serialize(expected);
        ContentGroup actual = serializer.deserialize(serialized, new ContentGroup());

        checkContentGroup(expected, actual);
        checkDescribed(expected, actual);
    }

    private void checkContentGroup(ContentGroup expected, ContentGroup actual) {
        assertThat(actual.getType(), is(expected.getType()));
        assertThat(actual.getContents(), is(expected.getContents()));
    }

    private void checkDescribed(Described expected, Described actual) {
        assertThat(actual.getTitle(), is(expected.getTitle()));
    }
}