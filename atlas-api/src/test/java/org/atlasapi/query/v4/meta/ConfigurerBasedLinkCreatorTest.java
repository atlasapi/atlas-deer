package org.atlasapi.query.v4.meta;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigurerBasedLinkCreatorTest {

    private String baseUri = "base";
    private final LinkCreator linkCreator = new MetaApiLinkCreator(baseUri);

    @Test
    public void testLinkCreation() {
        String type = "type";

        assertEquals(
                baseUri + "/4/meta/types/" + type + ".json",
                linkCreator.createModelLink(type)
        );
    }

}
