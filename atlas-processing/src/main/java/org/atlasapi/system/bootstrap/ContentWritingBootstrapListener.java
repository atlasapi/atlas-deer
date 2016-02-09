package org.atlasapi.system.bootstrap;

import java.util.concurrent.ThreadPoolExecutor;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentWriter;

public class ContentWritingBootstrapListener
        extends AbstractMultiThreadedBootstrapListener<Content> {

    private final ContentWriter writer;

    public ContentWritingBootstrapListener(int concurrencyLevel, ContentWriter writer) {
        super(concurrencyLevel);
        this.writer = writer;
    }

    public ContentWritingBootstrapListener(ThreadPoolExecutor executor,
            ContentWriter contentWriter) {
        super(executor);
        this.writer = contentWriter;
    }

    @Override
    protected void onChange(Content content) throws Exception {
        content.setReadHash(null);
        writer.writeContent(content);
    }
}
