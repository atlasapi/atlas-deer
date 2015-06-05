package org.atlasapi.system.bootstrap;


import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentGroupResolver;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ContentGroupBootstrapController {

    private final ContentIndex index;
    private final ContentGroupResolver resolver;

    private final NumberToShortStringCodec codec = new SubstitutionTableNumberCodec();

    public  ContentGroupBootstrapController(ContentIndex index, ContentGroupResolver resolver) {
        this.index = checkNotNull(index);
        this.resolver = checkNotNull(resolver);
    }

    @RequestMapping(value="/system/bootstrap/contentgroup/{id}", method= RequestMethod.POST)
    public void bootstrapContentGroup(@RequestParam("id") String id, HttpServletResponse resp) throws IOException, IndexException {
        Id contentGroupId = Id.valueOf(codec.decode(id));
        Resolved<ContentGroup> resolved = Futures.get(
                resolver.resolveIds(ImmutableList.of(contentGroupId)),
                IOException.class
        );
        index.index(resolved.getResources().first().get());
        resp.getWriter().write("Finished indexing content group " + contentGroupId);
    }

}
