package org.atlasapi.system;

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
import java.io.IOException;

@Controller
public class SourcesController {

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final EntityListWriter<Publisher> sourcesWriter = SourceWriter.sourceListWriter("sources");

    private SourcesController() {

    }

    public static SourcesController create() {
        return new SourcesController();
    }

    @RequestMapping({ "/system/sources/{sid}.*", "/system/sources.*" })
    public void listSources(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws QueryParseException, QueryExecutionException, IOException {

        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            writer.writeList(
                    sourcesWriter,
                    Publisher.all(),
                    OutputContext.valueOf(QueryContext.standard(request))
            );
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

}
