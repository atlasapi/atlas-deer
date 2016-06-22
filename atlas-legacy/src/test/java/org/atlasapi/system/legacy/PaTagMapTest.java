package org.atlasapi.system.legacy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.CoreMatchers.hasItems;

import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaTagMapTest {

    @Mock
    TopicStore topicStore;

    @Mock
    MongoSequentialIdGenerator idGenerator;

    @InjectMocks
    PaTagMap paTagMapUnderTest;

    final private String UNCLASSIFIED_TAG = "http://metabroadcast.com/tags/unclassified";
    final private Long UNCLASSIFIED_TAG_ID = 1000L;

    final private String FILM_TAG = "http://metabroadcast.com/tags/films";
    final private Long FILM_TAG_ID = 1001L;

    final private String ACTION_TAG = "http://metabroadcast.com/tags/action";
    final private Long ACTION_TAG_ID = 1002L;

    final private String COMEDY_TAG = "http://metabroadcast.com/tags/comedy";
    final private Long COMEDY_TAG_ID = 1003L;

    final private String SCIFI_TAG = "http://metabroadcast.com/tags/scifi";
    final private Long SCIFI_TAG_ID = 1004L;

    final private String CURRENT_AFFAIRS_TAG = "http://metabroadcast.com/tags/current-affairs-politics";
    final private Long CURRENT_AFFAIRS_ID = 1005L;

    @Before
    public void setUp() {
        Map<String, Long> fakeTopicStore = new ImmutableMap.Builder<String, Long>()
                .put(FILM_TAG, FILM_TAG_ID)
                .put(UNCLASSIFIED_TAG, UNCLASSIFIED_TAG_ID)
                .put(ACTION_TAG, ACTION_TAG_ID)
                .put(COMEDY_TAG, COMEDY_TAG_ID)
                .put(SCIFI_TAG, SCIFI_TAG_ID)
                .put(CURRENT_AFFAIRS_TAG, CURRENT_AFFAIRS_ID)
                .build();

        when(topicStore.topicFor((Publisher) notNull(), (String) notNull(), (String) notNull()))
                .thenAnswer(new Answer<Maybe<Topic>>() {

                    @Override
                    public Maybe<Topic> answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();

                        Publisher publisher = (Publisher) args[0];
                        String namespace = (String) args[1];
                        String value = (String) args[2];

                        if (fakeTopicStore.containsKey(value)) {
                            Long id = fakeTopicStore.get(value);
                            Topic topic = new Topic(id, namespace, value);
                            topic.setPublisher(publisher);
                            return Maybe.just(topic);
                        } else {
                            return Maybe.nothing();
                        }
                    }
                });
    }

    @Test
    public void testWithNoGenres() {
        assertThatGenresMapToTagRefs(
                ImmutableSet.of(),
                ImmutableList.of(UNCLASSIFIED_TAG_ID)
        );
    }

    @Test
    public void testWithActionFilmSpecialCase() {
        // PA articulate Films-Action by omitting the subgenre specialisation
        // this comes from Voila 1 logic
        assertThatGenresMapToTagRefs(
                ImmutableSet.of(
                        "http://pressassociation.com/genres/BF01"
                ),
                ImmutableList.of(
                        FILM_TAG_ID,
                        ACTION_TAG_ID
                )
        );
    }

    @Test
    public void testRomcomScifiFilm() {
        assertThatGenresMapToTagRefs(
                ImmutableSet.of(
                        "http://pressassociation.com/genres/BF01",
                        "http://pressassociation.com/genres/1400",
                        "http://pressassociation.com/genres/1300"
                ),
                ImmutableList.of(
                        FILM_TAG_ID,
                        COMEDY_TAG_ID,
                        SCIFI_TAG_ID
                )
        );
    }

    @Test
    public void testCurrentAffairs() {
        assertThatGenresMapToTagRefs(
                ImmutableSet.of("http://pressassociation.com/genres/8200"),
                ImmutableList.of(CURRENT_AFFAIRS_ID)
        );
    }

    @Test
    public void testUnmappedGenres() {
        assertThatGenresMapToTagRefs(
                ImmutableSet.of(
                        "http://pressassociation.com/genres/FFFF",
                        "http://pressassociation.com/genres/8200"),
                ImmutableList.of(CURRENT_AFFAIRS_ID)
        );
    }

    @Test
    public void testEffectivelyNoGenres() {
        assertThatGenresMapToTagRefs(
                ImmutableSet.of("http://pressassociation.com/genres/FFFF"),
                ImmutableList.of(UNCLASSIFIED_TAG_ID)
        );
    }


    private void assertThatGenresMapToTagRefs(Set<String> inputGenres, List<Long> tagRefs) {
        Set<TopicRef> actualRefs = paTagMapUnderTest.mapGenresToTopicRefs(inputGenres);

        // Java type erasure is causing a compile interference error with hasItems(tagRefs.toArray())
        List<Object> actualTopicRefs = actualRefs.stream()
                .map(topicRef -> topicRef.getTopic())
                .collect(Collectors.toList());

        assertThat(actualRefs.size(), is(tagRefs.size()));
        assertThat(actualTopicRefs, hasItems(tagRefs.toArray()));
    }

}
