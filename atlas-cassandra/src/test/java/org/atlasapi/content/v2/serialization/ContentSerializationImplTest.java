package org.atlasapi.content.v2.serialization;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.beanutils.PropertyUtils;
import org.atlasapi.content.Content;
import org.atlasapi.content.v2.CqlContentGenerator;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionComparatorMode;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static org.unitils.reflectionassert.ReflectionAssert.assertReflectionEquals;

public class ContentSerializationImplTest {

    private final ContentSerialization serialization = new ContentSerializationImpl();

    private List<Content> contents;

    @Before
    public void setUp() {
        this.contents = CqlContentGenerator.makeContent();
    }

    @Test
    public void serializerSerializesStuff() {
        for (Content original : contents) {
            org.atlasapi.content.v2.model.Content serialized = serialization.serialize(original);
            Content deserialized = serialization.deserialize(serialized);
            assertReflectionEquals(
                    original.getClass().getName(),
                    original,
                    deserialized,
                    ReflectionComparatorMode.LENIENT_ORDER
            );
        }
    }

    @Test
    public void serializerCopesWithNulls() {
        Set<String> nonNullProps = ImmutableSet.of(
                "id", // part of the primary key, can't possibly be null
                "canonicalUri", // can't be null because it's essentially an Owl PK
                "manifestedAs", // explicitly initialised to an empty set
                "equivalentTo", // explicitly initialised to an empty set
                "restrictions", // explicitly initialised to an empty set
                "awards", // explicitly initialised to an empty set
                "reviews",  // explicitly initialised to an empty set
                "ratings",  // explicitly initialised to an empty set
                "broadcasts", // explicitly initialised to an empty set
                "people", // explicitly initialised to an empty set
                "mediaType", // explicitly initialised to VIDEO,
                "countriesOfOrigin", // takes an iterable, can't be set to null
                "itemSummaries", // takes an iterable, can't be set to null
                "upcomingContent", // takes an iterable, can't be set to null
                "availableContent", // takes an iterable, can't be set to null
                "Song#specialization", // explicitly initialised to MUSIC
                "Film#specialization", // explicitly initialised to FILM,
                "customFields", //explicitly initialised to an empty map
                "titles"   // explicitly initialised to an empty set
        );

        for (Content original : contents) {
            for (PropertyDescriptor pd : PropertyUtils.getPropertyDescriptors(original)) {
                Method setter = pd.getWriteMethod();
                Method getter = pd.getReadMethod();
                if (setter == null
                        || getter == null
                        || setter.getParameterTypes()[0].isPrimitive()
                        || nonNullProps.contains(pd.getName())
                        || nonNullProps.contains(String.format(
                            "%s#%s",
                            original.getClass().getSimpleName(),
                            pd.getName()
                        ))
                ) {
                    /* skip computed values, primitive types and props that aren't allowed
                    to be null */
                    continue;
                }
                Content mashed;
                try {
                    setter.invoke(original, (Object) null);
                    mashed = serialization.deserialize(serialization.serialize(original));
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format(
                                    "Exception in prop %s : %s",
                                    original.getClass().getName(),
                                    pd
                            ),
                            e
                    );
                }
                assertReflectionEquals(original, mashed, ReflectionComparatorMode.LENIENT_ORDER);
            }
        }
    }
}