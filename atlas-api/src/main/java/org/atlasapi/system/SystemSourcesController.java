package org.atlasapi.system;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.writers.SourceWithIdWriter;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.QueryParam;
import java.io.IOException;

@Controller
public class SystemSourcesController {

    private final ResponseWriterFactory writerResolver;
    private final EntityListWriter<Publisher> sourcesWriter;
    private final SourceIdCodec sourceIdCodec;

    private SystemSourcesController(SourceIdCodec sourceIdCodec) {
        this.sourceIdCodec = sourceIdCodec;
        sourcesWriter = new SourceWithIdWriter(sourceIdCodec, "source", "sources");
        writerResolver = new ResponseWriterFactory();
    }

    public static SystemSourcesController create(SourceIdCodec sourceIdCodec) {
        return new SystemSourcesController(sourceIdCodec);
    }

    @RequestMapping({ "/system/sources/{sid}.*", "/system/sources.*" })
    public void listSources(
            HttpServletRequest request,
            HttpServletResponse response,
            @QueryParam("sid") String sourceId
    ) throws QueryParseException, QueryExecutionException, IOException {

        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            writer.startResponse();
            if(Strings.isNullOrEmpty(sourceId)) {
                listAllSources(writer, request);
            } else {
                listSingleSource(writer, request, sourceId);
            }
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
        Optional<Publisher> publisher = sourceIdCodec.decode(sourceId);
        if(!publisher.isPresent()) {
            throw new IOException("Publisher ID not found: " + sourceId);
        }

        writer.writeObject(
                sourcesWriter,
                sourceIdCodec.decode(sourceId).get(),
                OutputContext.valueOf(QueryContext.standard(request))
        );
    }

}
