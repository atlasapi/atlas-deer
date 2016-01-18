package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.system.legacy.LegacyTopicResolver;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicWriter;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

@Controller
public class IndividualTopicBootstrapController {

    private final LegacyTopicResolver resolver;
    private final TopicWriter writer;
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public IndividualTopicBootstrapController(LegacyTopicResolver read, TopicWriter write) {
        this.resolver = checkNotNull(read);
        this.writer = checkNotNull(write);
    }
    
    @RequestMapping(value="/system/bootstrap/topic/{id}", method=RequestMethod.POST)
    public void bootstrapTopic(@PathVariable("id") String encodedId,
            HttpServletResponse resp) throws IOException {
        Id id = Id.valueOf(idCodec.decode(encodedId).longValue());
        ListenableFuture<Resolved<Topic>> possibleTopic = resolver.resolveIds(ImmutableList.of(id));
        
        Resolved<Topic> resolved = Futures.get(possibleTopic, IOException.class);
        if (resolved.getResources().isEmpty()) {
            resp.sendError(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        for (Topic topic : resolved.getResources()) {
            writer.writeTopic(topic);
        }
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }

    @RequestMapping(value="/system/bootstrap/topic/all", method=RequestMethod.POST)
    public void bootstrapAllTopic(HttpServletResponse resp) throws IOException {
        Iterable<Topic> topics = resolver.resolveAll();

        for (Topic topic : topics) {
            writer.writeTopic(topic);
        }
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }
}
