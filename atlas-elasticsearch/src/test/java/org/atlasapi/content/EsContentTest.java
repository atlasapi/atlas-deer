package org.atlasapi.content;

import org.junit.Test;

public class EsContentTest {

    @Test
    public void getMapping() throws Exception {
        System.out.println(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_CONTAINER).string());
        System.out.println(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_ITEM).string());
        System.out.println(EsContent.getChildMapping().string());
    }
}