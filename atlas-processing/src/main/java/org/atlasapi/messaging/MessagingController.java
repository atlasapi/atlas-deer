package org.atlasapi.messaging;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.queue.FailedMessagesStore;
import com.metabroadcast.common.queue.MessageResender;
import com.metabroadcast.common.queue.MessagingException;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class MessagingController {


    private final FailedMessagesStore failedMessagesStore;
    private final Map<String, MessageResender> messageResenders;

    private static final DateTimeFormatter dateParser = ISODateTimeFormat.date();


    public MessagingController(FailedMessagesStore failedMessagesStore, Map<String, MessageResender> messageResenders) {
        this.failedMessagesStore = checkNotNull(failedMessagesStore);
        this.messageResenders = ImmutableMap.copyOf(messageResenders);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/system/messaging/{topic}/failedMessagesCount")
    public void getFailedMessagesCount(HttpServletResponse resp, @PathVariable("topic") String topic, @RequestParam("day") String day) throws IOException {
        LocalDate date =  dateParser.parseLocalDate(day);

        Integer failedMessages = failedMessagesStore.getFailedMessagesCount(topic, date);
        resp.setStatus(HttpStatusCode.OK.code());
        resp.getWriter().write(failedMessages.toString());
    }
    @RequestMapping(method = RequestMethod.POST, value = "/system/messaging/{topic}/resend")
    public void resendFailedMessages(HttpServletResponse resp, @PathVariable("topic") String topic, @RequestParam("day") String day) throws MessagingException, IOException {
        LocalDate date =  dateParser.parseLocalDate(day);
        Integer resentMessages = messageResenders.get(topic).resendMessages(date);
        resp.setStatus(HttpStatusCode.OK.code());
        resp.getWriter().write(
                String.format(
                        "Resent %s messages",
                        resentMessages
                )
        );

    }
}
