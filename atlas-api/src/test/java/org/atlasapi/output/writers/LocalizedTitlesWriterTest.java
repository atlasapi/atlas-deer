package org.atlasapi.output.writers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.LocalizedTitle;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.applications.client.model.internal.Application;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class LocalizedTitlesWriterTest {

    private @Mock FieldWriter fieldWriter;
    private @Mock EntityWriter<Publisher> sourceWriter;
    private @Mock Application application;
    private @Mock HttpServletRequest request;

    @Test
    public void testLocalizedTitle() throws IOException {
        LocalizedTitlesWriter writerUnderTest = new LocalizedTitlesWriter();
        LocalizedTitle localizedTitle = new LocalizedTitle();
        localizedTitle.setTitle("Titlu");
        localizedTitle.setLocale("","RO");

        OutputContext ctxt = OutputContext.valueOf(
                QueryContext.create(application, ActiveAnnotations.standard(), request)
        );

        writerUnderTest.write(
                localizedTitle,
                fieldWriter,
                ctxt);

        verify(fieldWriter).writeField("title", localizedTitle.getTitle());
        verify(fieldWriter).writeField("locale", localizedTitle.getLanguageTag());
        verifyNoMoreInteractions(fieldWriter);
    }

}