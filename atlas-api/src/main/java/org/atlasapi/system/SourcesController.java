package org.atlasapi.system;

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
import java.util.Optional;

@Controller
public class SourcesController {

    private final ResponseWriterFactory writerResolver;
    private final EntityListWriter<Publisher> sourcesWriter;
    private final SourceIdCodec sourceIdCodec;

    private SourcesController(SourceIdCodec sourceIdCodec) {
        this.sourceIdCodec = sourceIdCodec;
        sourcesWriter = SourceWithIdWriter.create(sourceIdCodec, "source", "sources");
        writerResolver = new ResponseWriterFactory();
    }

    public static SourcesController create(SourceIdCodec sourceIdCodec) {
        return new SourcesController(sourceIdCodec);
    }

    @RequestMapping({ "/system/sources/{sourceId}.*" })
    public void listSources(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId
    ) throws QueryParseException, QueryExecutionException, IOException {

        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            Optional<Publisher> publisher = sourceIdCodec.decode(sourceId);
            if(!publisher.isPresent()) {
                throw new IOException("Publisher ID not found: " + sourceId);
            }

            writer.startResponse();
            writer.writeObject(
                    sourcesWriter,
                    publisher.get(),
                    OutputContext.valueOf(QueryContext.standard(request))
            );
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
            writer.writeList(
                    sourcesWriter,
                    Publisher.all(),
                    OutputContext.valueOf(QueryContext.standard(request))
            );
            writer.finishResponse();
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }

    }

}
