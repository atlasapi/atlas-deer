package org.atlasapi.application;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.input.ModelReader;
import org.atlasapi.messaging.ContentMessage;
import org.atlasapi.util.ClientModelConverter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;

@Controller
public class ContentWriterController {

    private static final Logger log = LoggerFactory.getLogger(ContentWriterController.class);

    private final ModelReader modelReader;
    private final ClientModelConverter converter;
    private final MessageSender<ContentMessage> messageSender;

    public ContentWriterController(ModelReader modelReader, ClientModelConverter converter,
            MessageSender<ContentMessage> messageSender) {
        this.modelReader = checkNotNull(modelReader);
        this.converter = checkNotNull(converter);
        this.messageSender = checkNotNull(messageSender);
    }

    @RequestMapping(value = "/4/content", method = RequestMethod.POST)
    public void addContent(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        try {
            org.atlasapi.deer.client.model.types.Content requestContent =
                    modelReader.read(request.getReader(), org.atlasapi.deer.client.model.types.Content.class);
            Content content = converter.convert(requestContent);

            messageSender.sendMessage(
                    new ContentMessage(
                            UUID.randomUUID().toString(), Timestamp.of(DateTime.now()), content));

            response.setStatus(HttpStatus.ACCEPTED.value());
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }
}
