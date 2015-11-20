package org.atlasapi.system.legacy;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.atlasapi.content.Content;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class ContentListerResourceListerAdapterTest {

    @Mock
    private LegacyContentLister contentLister;

    @Mock
    private LegacyContentTransformer transformer;

    @InjectMocks
    private ContentListerResourceListerAdapter objectUnderTest;


    @Test
    public void testListReturnNullOnException() throws Exception {

        org.atlasapi.media.entity.Content legacyContent1 = mock(org.atlasapi.media.entity.Content.class);
        org.atlasapi.media.entity.Content legacyContent2 = mock(org.atlasapi.media.entity.Content.class);
        org.atlasapi.media.entity.Content legacyContent3 = mock(org.atlasapi.media.entity.Content.class);

        Content content1 = mock(Content.class);
        Content content3 = mock(Content.class);

        when(transformer.createDescribed(legacyContent1)).thenReturn(content1);
        when(transformer.createDescribed(legacyContent2)).thenThrow(new RuntimeException("Exception while transforming content"));
        when(transformer.createDescribed(legacyContent3)).thenReturn(content3);

        when(
                contentLister.listContent(any(ContentListingCriteria.class)
                )
        ).thenReturn(ImmutableSet.of(legacyContent1, legacyContent2, legacyContent3).iterator());

        FluentIterable<Content> transformedContent = objectUnderTest.list();


        assertThat(transformedContent, containsInAnyOrder(content1, null, content3));

    }


}