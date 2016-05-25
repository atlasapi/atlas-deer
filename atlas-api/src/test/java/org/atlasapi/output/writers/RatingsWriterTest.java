package org.atlasapi.output.writers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.entity.Rating;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.QueryContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class RatingsWriterTest {

    private @Mock FieldWriter fieldWriter;
    private @Mock EntityWriter<Publisher> sourceWriter;

    @Test
    public void testRating() throws IOException {
        RatingsWriter writerUnderTest = new RatingsWriter(sourceWriter);
        Rating rating = new Rating("MOOSE", 1.0f, Publisher.BBC);

        OutputContext ctxt = OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)));

        writerUnderTest.write(
                rating,
                fieldWriter,
                ctxt);

        verify(fieldWriter).writeField("type", rating.getType());
        verify(fieldWriter).writeField("value", rating.getValue());
        verify(fieldWriter).writeObject(sourceWriter, rating.getPublisher(), ctxt);
        verifyNoMoreInteractions(fieldWriter);
    }

}
