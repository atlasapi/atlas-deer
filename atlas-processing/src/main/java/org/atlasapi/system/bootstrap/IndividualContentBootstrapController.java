package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;

@Controller
public class IndividualContentBootstrapController {
    
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentResolver read;
    private final ContentWriter write;
    private final ResourceLister<Content> contentLister;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ContentIndex contentIndex;
    private final AtlasPersistenceModule persistence;
    private final DirectAndExplicitEquivalenceMigrator equivalenceMigrator;

    public IndividualContentBootstrapController(ContentResolver read, 
            ResourceLister<Content> contentLister, ContentWriter write, 
            ContentIndex contentIndex, AtlasPersistenceModule persistence,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator) {
        this.persistence = checkNotNull(persistence);
        this.equivalenceMigrator = checkNotNull(equivalenceMigrator);
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
        this.contentLister = checkNotNull(contentLister);
        this.contentIndex = checkNotNull(contentIndex);
    }
    
    @RequestMapping(value="/system/bootstrap/source", method=RequestMethod.POST) 
    public void bootstrapSource(@RequestParam("source") final String sourceString, HttpServletResponse resp) {
        ContentVisitorAdapter<String> visitor = visitor();
        log.info("Bootstrapping source: {}", sourceString);
        Maybe<Publisher> fromKey = Publisher.fromKey(sourceString);
        executorService.execute(() -> 
            {
                for (Content c : contentLister.list(ImmutableList.of(fromKey.requireValue()))) {
                    c.accept(visitor);
                }
            });
        resp.setStatus(HttpStatus.ACCEPTED.value());
    }
 
    @RequestMapping(value="/system/bootstrap/content", method=RequestMethod.POST)
    public void bootstrapContent(@RequestParam("id") final String id, final HttpServletResponse resp) throws IOException {
        log.info("Bootstrapping: {}", id);
        Identified identified = Iterables.getOnlyElement(resolve(ImmutableList.of(Id.valueOf(id))),null);
        log.info("Bootstrapping: {} {}", id, identified);
        if (!(identified instanceof Content)) {
            resp.sendError(500, "Resolved not content");
            return;
        }
        Content content = (Content) identified;

        resp.setStatus(HttpStatus.OK.value());
        
        String result = content.accept(visitor());
        resp.getWriter().println(result);
        resp.getWriter().flush();
    }

    private FluentIterable<Content> resolve(Iterable<Id> ids) {
        try {
            ListenableFuture<Resolved<Content>> resolved = read.resolveIds(ids);
            return Futures.get(resolved, IOException.class).getResources();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
    
    private ContentVisitorAdapter<String> visitor() {
        return new ContentVisitorAdapter<String>() {
            
            @Override
            public String visit(Brand brand) {
                WriteResult<?, Content> brandWrite = write(brand);
                int series = resolveAndWrite(Iterables.transform(brand.getSeriesRefs(), Identifiables.toId()));
                int childs = resolveAndWrite(Iterables.transform(brand.getItemRefs(), Identifiables.toId()));
                return String.format("%s s:%s c:%s", brandWrite, series, childs);
            }
            
            @Override
            public String visit(Series series) {
                WriteResult<?, Content> seriesWrite = write(series);
                int childs = resolveAndWrite(Iterables.transform(series.getItemRefs(), Identifiables.toId()));
                return String.format("%s c:%s", seriesWrite, childs);
            }

            private int resolveAndWrite(Iterable<Id> ids) {
                FluentIterable<Content> resolved = resolve(ids);
                int i = 0;
                for (Content content : Iterables.filter(resolved, Content.class)) {
                    if (write(content) != null) {
                        i++;
                    }
                }
                return i;
            }
            
            @Override
            protected String visitItem(Item item) {
                return write(item).toString();
            }

            private WriteResult<? extends Content, Content> write(Content content) {
                try {
                    log.info(
                            "Content type: {}, id: {}, activelyPublished: {}",
                            ContentType.fromContent(content).get(),
                            content.getId(),
                            content.isActivelyPublished()
                    );
                    content.setReadHash(null);
                    WriteResult<Content, Content> writeResult = write.writeContent(content);
                    contentIndex.index(content);
                    Optional<EquivalenceGraphUpdate> graphUpdate =
                            equivalenceMigrator.migrateEquivalence(content);
                    persistence.getEquivalentContentStore().updateContent(content.toRef());
                    if (graphUpdate.isPresent()) {
                        persistence.getEquivalentContentStore().updateEquivalences(graphUpdate.get());
                    }
                    return writeResult;
                } catch (Exception e) {
                    log.error(String.format("Bootstrapping: %s %s", content.getId(), content), e);
                    return null;
                }
            }
            
        };
    }
}
