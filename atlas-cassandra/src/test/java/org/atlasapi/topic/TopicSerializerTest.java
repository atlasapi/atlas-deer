package org.atlasapi.topic;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

public class TopicSerializerTest {

    private final TopicSerializer serializer = new TopicSerializer();
    
    @Test
    public void testDeSerializesTopic() {
        Topic topic = new Topic(1234);
        topic.setPublisher(Publisher.DBPEDIA);
        topic.setType(Topic.Type.PERSON);
        serializeAndCheck(topic);
        topic.addAlias(new Alias("a","alias1"));
        serializeAndCheck(topic);
        topic.setTitle("Jim");
        serializeAndCheck(topic);
        topic.setDescription("Top Bloke");
        serializeAndCheck(topic);
        topic.setImage("suave");
        serializeAndCheck(topic);
        topic.setThumbnail("present");
        serializeAndCheck(topic);
    }

    private void serializeAndCheck(Topic topic) {
        Topic serialized = serializer.deserialize(serializer.serialize(topic));
        assertThat(serialized.getId(), is(topic.getId()));
        assertThat(serialized.getSource(), is(topic.getSource()));
        assertThat(serialized.getAliases(), is(topic.getAliases()));
        assertThat(serialized.getType(), is(topic.getType()));
        assertThat(serialized.getTitle(), is(topic.getTitle()));
        assertThat(serialized.getDescription(), is(topic.getDescription()));
        assertThat(serialized.getImage(), is(topic.getImage()));
        assertThat(serialized.getThumbnail(), is(topic.getThumbnail()));
    }

}
