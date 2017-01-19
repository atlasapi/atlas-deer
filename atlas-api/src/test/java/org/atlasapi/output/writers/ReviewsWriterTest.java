package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.entity.Review;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.context.QueryContext;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class ReviewsWriterTest {

    private @Mock FieldWriter fieldWriter;
    private @Mock EntityWriter<Publisher> sourceWriter;
    private @Mock HttpServletRequest request = mock(HttpServletRequest.class);
    private @Mock Application application = mock(Application.class);

    private OutputContext ctxt = OutputContext.valueOf(
            new QueryContext(application, ActiveAnnotations.standard(), request)
    );


    private ReviewsWriter writerUnderTest;

    @Before
    public void setUp() {
        writerUnderTest = new ReviewsWriter(sourceWriter);
    }

    @Test
    public void testReview() throws IOException {
        Review review = new Review(Locale.CHINESE, "hen hao", Optional.of(Publisher.BBC));

        writerUnderTest.write(
                review,
                fieldWriter,
                ctxt);

        verify(fieldWriter).writeField("language", review.getLocale().getLanguage());
        verify(fieldWriter).writeField("review", review.getReview());
        verify(fieldWriter).writeObject(sourceWriter, review.getSource().get(), ctxt);
        verifyNoMoreInteractions(fieldWriter);
    }


    @Test
    public void testReviewWithoutLanguage() throws IOException {
        Review review = new Review(null, "hen hao", Optional.of(Publisher.BBC));

        writerUnderTest.write(
                review,
                fieldWriter,
                ctxt);

        verify(fieldWriter).writeField("language", null);
        verify(fieldWriter).writeField("review", review.getReview());
        verify(fieldWriter).writeObject(sourceWriter, review.getSource().get(), ctxt);
        verifyNoMoreInteractions(fieldWriter);
    }


    @Test
    public void testReviewWithoutSource() throws IOException {
        Review review = new Review(null, "hen hao", Optional.empty());

        writerUnderTest.write(
                review,
                fieldWriter,
                ctxt);

        verify(fieldWriter).writeField("language", null);
        verify(fieldWriter).writeField("review", review.getReview());
        verifyNoMoreInteractions(fieldWriter);
    }
}
