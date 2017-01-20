package org.atlasapi.system;

import com.google.common.base.Optional;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.writers.SourceWithIdWriter;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Controller
public class SystemSourcesController {

    private final ResponseWriterFactory writerResolver;
    private final EntityListWriter<Publisher> sourcesWriter;
    private final SourceIdCodec sourceIdCodec;

    private SystemSourcesController(SourceIdCodec sourceIdCodec) {
        this.sourceIdCodec = sourceIdCodec;
        sourcesWriter = SourceWithIdWriter.create(sourceIdCodec, "source", "sources");
        writerResolver = new ResponseWriterFactory();
    }

    public static SystemSourcesController create(SourceIdCodec sourceIdCodec) {
        return new SystemSourcesController(sourceIdCodec);
    }

    @RequestMapping({ "/system/sources/{sourceId}.*" })
    public void listSources(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId
    ) throws QueryParseException, QueryExecutionException, IOException {

        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            writer.startResponse();
            listSingleSource(writer, request, sourceId);
            writer.finishResponse();
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping({"/system/sources.*"})
    public void listAllSources(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws QueryParseException, QueryExecutionException, IOException {

        ResponseWriter writer = writerResolver.writerFor(request, response);

        try {
            writer.startResponse();
            listAllSources(writer, request);
            writer.finishResponse();
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }

    }

    private void listAllSources(
            ResponseWriter writer,
            HttpServletRequest request
    ) throws IOException {
        writer.writeList(
                sourcesWriter,
                Publisher.all(),
                OutputContext.valueOf(QueryContext.standard(request))
        );
    }

    private void listSingleSource(
            ResponseWriter writer,
            HttpServletRequest request,
            String sourceId
    ) throws IOException {
        Optional<Publisher> publisher = sourceIdCodec.decode(sourceId).toGuavaOptional();
        if(!publisher.isPresent()) {
            throw new IOException("Publisher ID not found: " + sourceId);
        }

        writer.writeObject(
                sourcesWriter,
                publisher.get(),
                OutputContext.valueOf(QueryContext.standard(request))
        );
    }

}
