package org.atlasapi.system.processing;

import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.ContentMessage;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class ContentPersistenceWorker implements Worker<ContentMessage> {

    private final ContentWriter contentWriter;

    public ContentPersistenceWorker(ContentWriter contentWriter) {
        this.contentWriter = contentWriter;
    }

    @Override
    public void process(ContentMessage message) throws RecoverableException {
        try {
            contentWriter.writeContent(message.getContent());
        } catch (WriteException e) {
            throw new RecoverableException(e);
        }
    }
}
