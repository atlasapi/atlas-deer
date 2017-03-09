package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.ReviewType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.context.QueryContext;

import org.joda.time.DateTime;
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
            QueryContext.create(application, ActiveAnnotations.standard(), request)
    );


    private ReviewsWriter writerUnderTest;

    @Before
    public void setUp() {
        writerUnderTest = new ReviewsWriter(sourceWriter);
    }

    @Test
    public void testReview() throws IOException {
        Review review = Review.builder("hen hao")
                .withLocale(Locale.CHINESE)
                .withSource(Optional.of(Publisher.BBC))
                .withAuthor("Top Cat")
                .withAuthorInitials("TC")
                .withRating("5 stars")
                .withReviewType(ReviewType.NORMAL_REVIEW)
                .withDate(DateTime.now())
                .build();

        writerUnderTest.write(review, fieldWriter, ctxt);

        verify(fieldWriter).writeField("language", review.getLocale().getLanguage());
        verify(fieldWriter).writeField("review", review.getReview());
        verify(fieldWriter).writeObject(sourceWriter, review.getSource().get(), ctxt);
        verify(fieldWriter).writeField("author", review.getAuthor());
        verify(fieldWriter).writeField("author_initials", review.getAuthorInitials());
        verify(fieldWriter).writeField("rating", review.getRating());
        verify(fieldWriter).writeField("date", review.getDate());
        verify(fieldWriter).writeField("review_type", review.getReviewType().toKey());

        verifyNoMoreInteractions(fieldWriter);
    }


    @Test
    public void testReviewWithoutLanguage() throws IOException {
        Review review = Review.builder("hen hao")
                .withSource(Optional.of(Publisher.BBC))
                .build();

        writerUnderTest.write(review, fieldWriter, ctxt);

        verify(fieldWriter).writeField("language", null);
        verify(fieldWriter).writeField("review", review.getReview());
        verify(fieldWriter).writeObject(sourceWriter, review.getSource().get(), ctxt);
        verify(fieldWriter).writeField("author", null);
        verify(fieldWriter).writeField("author_initials", null);
        verify(fieldWriter).writeField("rating", null);
        verify(fieldWriter).writeField("date", null);
        verify(fieldWriter).writeField("review_type", null);

        verifyNoMoreInteractions(fieldWriter);
    }


    @Test
    public void testReviewWithoutLanguageAndSource() throws IOException {
        Review review = Review.builder("hen hao").build();

        writerUnderTest.write(review, fieldWriter, ctxt);

        verify(fieldWriter).writeField("language", null);
        verify(fieldWriter).writeField("review", review.getReview());
        verify(fieldWriter).writeField("author", null);
        verify(fieldWriter).writeField("author_initials", null);
        verify(fieldWriter).writeField("rating", null);
        verify(fieldWriter).writeField("date", null);
        verify(fieldWriter).writeField("review_type", null);

        verifyNoMoreInteractions(fieldWriter);
    }
}
