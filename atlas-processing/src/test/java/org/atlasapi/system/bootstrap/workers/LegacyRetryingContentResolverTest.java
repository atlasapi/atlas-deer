package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.system.legacy.LegacyContentResolver;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LegacyRetryingContentResolverTest {

    private ContentResolver currentResolver;

    private LegacyContentResolver legacyContentResolver;

    private ContentWriter contentWriter;

    private LegacyRetryingContentResolver objectUnderTest;

    @Before
    public void setUp() {

        currentResolver = mock(ContentResolver.class);
        legacyContentResolver = mock(LegacyContentResolver.class);
        contentWriter = mock(ContentWriter.class);
        objectUnderTest = new LegacyRetryingContentResolver(
                currentResolver,
                legacyContentResolver,
                contentWriter
        );
    }

    @Test
    public void testDontCheckLegacyStoreIfContentPresentInCurrent() throws Exception {
        Id id1 = Id.valueOf(1);
        Id id2 = Id.valueOf(2);
        Id id3 = Id.valueOf(3);

        Content content1 = mock(Content.class);
        when(content1.getId()).thenReturn(id1);

        Content content2 = mock(Content.class);
        when(content2.getId()).thenReturn(id2);

        Content content3 = mock(Content.class);
        when(content3.getId()).thenReturn(id3);

        Resolved<Content> currentResolved = Resolved.valueOf(ImmutableList.of(
                content1,
                content2,
                content3
        ));
        when(currentResolver.resolveIds(ImmutableList.of(id1, id2, id3)))
                .thenReturn(
                        Futures.immediateFuture(currentResolved)
                );

        Resolved<Content> resolved = Futures.getChecked(
                objectUnderTest.resolveIds(ImmutableList.of(id1, id2, id3)),
                Exception.class

        );

        assertThat(resolved, is(currentResolved));
        verify(legacyContentResolver, never()).resolveIds(anyCollection());
        verify(contentWriter, never()).writeContent(Matchers.any(Content.class));

    }

    @Test
    public void testCheckLegacyStoreIfContentNotPresentInCurrent() throws Exception {
        Id id1 = Id.valueOf(1);
        Id id2 = Id.valueOf(2);
        Id id3 = Id.valueOf(3);

        Content content1 = mock(Content.class);
        when(content1.getId()).thenReturn(id1);

        Content content2 = mock(Content.class);
        when(content2.getId()).thenReturn(id2);

        Content content3 = mock(Content.class);
        when(content3.getId()).thenReturn(id3);

        Resolved<Content> currentResolved = Resolved.valueOf(ImmutableList.of(content2));

        when(currentResolver.resolveIds(ImmutableList.of(id1, id2, id3)))
                .thenReturn(
                        Futures.immediateFuture(currentResolved)
                );

        Resolved<Content> legacyResolved = Resolved.valueOf(ImmutableList.of(content1, content3));
        when(legacyContentResolver.resolveIds(ImmutableList.of(id1, id3)))
                .thenReturn(
                        Futures.immediateFuture(legacyResolved)
                );

        Resolved<Content> resolved = Futures.getChecked(
                objectUnderTest.resolveIds(ImmutableList.of(id1, id2, id3)),
                Exception.class

        );

        assertThat(resolved.getResources(), is(containsInAnyOrder(content1, content2, content3)));
        verify(contentWriter).writeContent(content1);
        verify(contentWriter).writeContent(content3);
    }
}