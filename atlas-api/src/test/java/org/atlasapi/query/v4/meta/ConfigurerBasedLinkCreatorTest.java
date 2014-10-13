package org.atlasapi.query.v4.meta;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class ConfigurerBasedLinkCreatorTest {

    private String baseUri = "base";
    private final LinkCreator linkCreator = new MetaApiLinkCreator(baseUri);
    
    @Test
    public void testLinkCreation() {
        String type = "type";
        
        assertEquals(baseUri + "/4/model_classes/" + type + ".json", linkCreator.createModelLink(type));
    }

}
