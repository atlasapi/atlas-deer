package org.atlasapi.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.time.SystemClock;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.LicenseModule;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Specialization;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.event.Event;
import org.atlasapi.generation.EndpointClassInfoSingletonStore;
import org.atlasapi.generation.ModelClassInfoSingletonStore;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ScrubbablesSegmentRelatedLinkMerger;
import org.atlasapi.output.SegmentRelatedLinkMergingFetcher;
import org.atlasapi.output.annotation.*;
import org.atlasapi.output.writers.SeriesWriter;
import org.atlasapi.output.writers.SubItemSummaryListWriter;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.output.writers.ContainerSummaryWriter;
import org.atlasapi.output.writers.ItemDetailWriter;
import org.atlasapi.output.writers.ItemRefWriter;
import org.atlasapi.output.writers.RequestWriter;
import org.atlasapi.output.writers.SeriesSummaryWriter;
import org.atlasapi.output.writers.UpcomingContentDetailWriter;
import org.atlasapi.query.annotation.AnnotationIndex;
import org.atlasapi.query.annotation.ImagesAnnotation;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.AttributeCoercers;
import org.atlasapi.query.common.ContextualQueryContextParser;
import org.atlasapi.query.common.ContextualQueryParser;
import org.atlasapi.query.common.IndexAnnotationsExtractor;
import org.atlasapi.query.common.IndexContextualAnnotationsExtractor;
import org.atlasapi.query.common.QueryAtomParser;
import org.atlasapi.query.common.QueryAttributeParser;
import org.atlasapi.query.common.QueryContextParser;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryParser;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.StandardQueryParser;
import org.atlasapi.query.v4.channel.ChannelController;
import org.atlasapi.query.v4.channel.ChannelListWriter;
import org.atlasapi.query.v4.channel.ChannelQueryResultWriter;
import org.atlasapi.query.v4.channel.ChannelWriter;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;
import org.atlasapi.query.v4.channelgroup.ChannelGroupController;
import org.atlasapi.query.v4.channelgroup.ChannelGroupListWriter;
import org.atlasapi.query.v4.channelgroup.ChannelGroupQueryResultWriter;
import org.atlasapi.query.v4.content.ContentController;
import org.atlasapi.query.v4.event.EventController;
import org.atlasapi.query.v4.event.EventListWriter;
import org.atlasapi.query.v4.event.EventQueryResultWriter;
import org.atlasapi.query.v4.meta.LinkCreator;
import org.atlasapi.query.v4.meta.MetaApiLinkCreator;
import org.atlasapi.query.v4.meta.endpoint.EndpointController;
import org.atlasapi.query.v4.meta.endpoint.EndpointInfoListWriter;
import org.atlasapi.query.v4.meta.endpoint.EndpointInfoQueryResultWriter;
import org.atlasapi.query.v4.meta.model.ModelController;
import org.atlasapi.query.v4.meta.model.ModelInfoListWriter;
import org.atlasapi.query.v4.meta.model.ModelInfoQueryResultWriter;
import org.atlasapi.query.v4.schedule.ContentListWriter;
import org.atlasapi.query.v4.schedule.LegacyChannelListWriter;
import org.atlasapi.query.v4.schedule.ScheduleController;
import org.atlasapi.query.v4.schedule.ScheduleEntryListWriter;
import org.atlasapi.query.v4.schedule.ScheduleListWriter;
import org.atlasapi.query.v4.schedule.ScheduleQueryResultWriter;
import org.atlasapi.query.v4.search.ContentQueryResultWriter;
import org.atlasapi.query.v4.search.SearchController;
import org.atlasapi.query.v4.topic.PopularTopicController;
import org.atlasapi.query.v4.topic.TopicContentController;
import org.atlasapi.query.v4.topic.TopicContentResultWriter;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.query.v4.topic.TopicListWriter;
import org.atlasapi.query.v4.topic.TopicQueryResultWriter;
import org.atlasapi.search.SearchResolver;
import org.atlasapi.source.Sources;
import org.atlasapi.topic.PopularTopicIndex;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.servlet.http.HttpServletRequest;

import static org.atlasapi.annotation.Annotation.AVAILABLE_CONTENT;
import static org.atlasapi.annotation.Annotation.AVAILABLE_CONTENT_DETAIL;
import static org.atlasapi.annotation.Annotation.AVAILABLE_LOCATIONS;
import static org.atlasapi.annotation.Annotation.BRAND_REFERENCE;
import static org.atlasapi.annotation.Annotation.BRAND_SUMMARY;
import static org.atlasapi.annotation.Annotation.BROADCASTS;
import static org.atlasapi.annotation.Annotation.CHANNEL;
import static org.atlasapi.annotation.Annotation.CHANNELS;
import static org.atlasapi.annotation.Annotation.CHANNEL_GROUP;
import static org.atlasapi.annotation.Annotation.CHANNEL_GROUPS;
import static org.atlasapi.annotation.Annotation.CHANNEL_GROUPS_ADVERTISED;
import static org.atlasapi.annotation.Annotation.CHANNEL_GROUPS_SUMMARY;
import static org.atlasapi.annotation.Annotation.CHANNEL_SUMMARY;
import static org.atlasapi.annotation.Annotation.CLIPS;
import static org.atlasapi.annotation.Annotation.CONTENT_DETAIL;
import static org.atlasapi.annotation.Annotation.CONTENT_SUMMARY;
import static org.atlasapi.annotation.Annotation.CURRENT_AND_FUTURE_BROADCASTS;
import static org.atlasapi.annotation.Annotation.DESCRIPTION;
import static org.atlasapi.annotation.Annotation.EXTENDED_DESCRIPTION;
import static org.atlasapi.annotation.Annotation.EXTENDED_ID;
import static org.atlasapi.annotation.Annotation.FIRST_BROADCASTS;
import static org.atlasapi.annotation.Annotation.ID;
import static org.atlasapi.annotation.Annotation.ID_SUMMARY;
import static org.atlasapi.annotation.Annotation.IMAGES;
import static org.atlasapi.annotation.Annotation.KEY_PHRASES;
import static org.atlasapi.annotation.Annotation.LOCATIONS;
import static org.atlasapi.annotation.Annotation.META_ENDPOINT;
import static org.atlasapi.annotation.Annotation.META_MODEL;
import static org.atlasapi.annotation.Annotation.NEXT_BROADCASTS;
import static org.atlasapi.annotation.Annotation.PARENT;
import static org.atlasapi.annotation.Annotation.PEOPLE;
import static org.atlasapi.annotation.Annotation.PLATFORM;
import static org.atlasapi.annotation.Annotation.REGIONS;
import static org.atlasapi.annotation.Annotation.RELATED_LINKS;
import static org.atlasapi.annotation.Annotation.SEGMENT_EVENTS;
import static org.atlasapi.annotation.Annotation.SERIES;
import static org.atlasapi.annotation.Annotation.SERIES_REFERENCE;
import static org.atlasapi.annotation.Annotation.SERIES_SUMMARY;
import static org.atlasapi.annotation.Annotation.SUB_ITEMS;
import static org.atlasapi.annotation.Annotation.SUB_ITEM_SUMMARIES;
import static org.atlasapi.annotation.Annotation.TAGS;
import static org.atlasapi.annotation.Annotation.UPCOMING_BROADCASTS;
import static org.atlasapi.annotation.Annotation.UPCOMING_CONTENT_DETAIL;
import static org.atlasapi.annotation.Annotation.VARIATIONS;
import static org.atlasapi.annotation.Annotation.EVENT;
import static org.atlasapi.annotation.Annotation.EVENT_DETAILS;

@Configuration
@Import({ QueryModule.class, LicenseModule.class })
public class QueryWebModule {

    private static final String CONTAINER_FIELD = "container";
    private @Value("${local.host.name}") String localHostName;
    private @Value("${atlas.uri}") String baseAtlasUri;

    private @Autowired DatabasedMongo mongo;
    private
    @Autowired
    QueryModule queryModule;
    private
    @Autowired
    org.atlasapi.media.channel.ChannelResolver legacyChannelResolver;
    private
    @Autowired
    SearchResolver v4SearchResolver;
    private
    @Autowired
    TopicResolver topicResolver;
    private
    @Autowired
    PopularTopicIndex popularTopicIndex;
    private
    @Autowired
    UserFetcher userFetcher;

    private
    @Autowired
    ApplicationSourcesFetcher configFetcher;

    private
    @Autowired
    AtlasPersistenceModule persistenceModule;

    private
    @Autowired
    QueryExecutor<Channel> channelQueryExecutor;

    private
    @Autowired
    ChannelResolver channelResolver;

    private
    @Autowired
    QueryExecutor<ChannelGroup<?>> channelGroupQueryExecutor;

    private
    @Autowired
    ChannelGroupResolver channelGroupResolver;

    private
    @Autowired
    ContainerSummaryResolver containerSummaryResolver;

    @Autowired
    @Qualifier("licenseWriter")
    EntityWriter<Object> licenseWriter;

    @Bean NumberToShortStringCodec idCodec() {
        return SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    @Bean SelectionBuilder selectionBuilder() {
        return Selection.builder().withDefaultLimit(50).withMaxLimit(100);
    }

    @Bean EntityWriter<HttpServletRequest> requestWriter() {
        return new RequestWriter();
    }

    @Bean ScheduleController v4ScheduleController() {
        EntityListWriter<ItemAndBroadcast> entryListWriter =
                new ScheduleEntryListWriter(contentListWriter(),
                        new BroadcastWriter("broadcasts", idCodec(), channelResolver, channelGroupResolver));
        ScheduleListWriter scheduleWriter = new ScheduleListWriter(channelListWriter(),
                entryListWriter);
        return new ScheduleController(queryModule.equivalentScheduleStoreScheduleQueryExecutor(),
                configFetcher,
                new ScheduleQueryResultWriter(scheduleWriter, licenseWriter, requestWriter()),
                new IndexContextualAnnotationsExtractor(ResourceAnnotationIndex.combination()
                        .addExplicitSingleContext(channelAnnotationIndex())
                        .addExplicitListContext(contentAnnotationIndex())
                        .combine()));
    }

    @Bean TopicController v4TopicController() {
        return new TopicController(topicQueryParser(),
                queryModule.topicQueryExecutor(),
                new TopicQueryResultWriter(topicListWriter(), licenseWriter, requestWriter()));
    }

    @Bean ContentController contentController() {
        return new ContentController(contentQueryParser(),
                queryModule.contentQueryExecutor(),
                new ContentQueryResultWriter(contentListWriter(), licenseWriter, requestWriter(), channelGroupResolver, idCodec()));
    }

    @Bean TopicContentController topicContentController() {
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(configFetcher,
                userFetcher,
                new IndexContextualAnnotationsExtractor(ResourceAnnotationIndex.combination()
                        .addImplicitListContext(contentAnnotationIndex())
                        .addExplicitSingleContext(topicAnnotationIndex())
                        .combine()
                ), selectionBuilder());

        ContextualQueryParser<Topic, Content> parser = new ContextualQueryParser<Topic, Content>(
                Resource.TOPIC, Attributes.TOPIC_ID, Resource.CONTENT, idCodec(),
                contentQueryAttributeParser(),
                contextParser
        );

        return new TopicContentController(
                parser,
                queryModule.topicContentQueryExecutor(),
                new TopicContentResultWriter(
                        topicListWriter(), contentListWriter()
                )
        );
    }

    @Bean EventController eventController(){
        return new EventController(eventQueryParser(),
                queryModule.eventQueryExecutor(),
                new EventQueryResultWriter(eventListWriter(),licenseWriter,requestWriter()));
    }

    @Bean LinkCreator linkCreator() {
        return new MetaApiLinkCreator(baseAtlasUri);
    }

    @Bean ModelController modelController() {
        QueryResultWriter<ModelClassInfo> resultWriter = new ModelInfoQueryResultWriter(
                modelListWriter(),
                licenseWriter,
                requestWriter());
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(
                configFetcher,
                userFetcher,
                new IndexContextualAnnotationsExtractor(ResourceAnnotationIndex.combination()
                        .addImplicitListContext(modelInfoAnnotationIndex())
                        .combine()
                ),
                selectionBuilder()
        );

        return new ModelController(
                ModelClassInfoSingletonStore.INSTANCE,
                resultWriter,
                contextParser
        );
    }

    @Bean EndpointController endpointController() {
        QueryResultWriter<EndpointClassInfo> resultWriter = new EndpointInfoQueryResultWriter(
                endpointListWriter(),
                licenseWriter,
                requestWriter());
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(
                configFetcher,
                userFetcher,
                new IndexContextualAnnotationsExtractor(ResourceAnnotationIndex.combination()
                        .addImplicitListContext(endpointInfoAnnotationIndex())
                        .combine()
                ),
                selectionBuilder()
        );

        return new EndpointController(
                EndpointClassInfoSingletonStore.INSTANCE,
                resultWriter,
                contextParser
        );
    }

    private QueryAttributeParser contentQueryAttributeParser() {
        return new QueryAttributeParser(
                ImmutableList.<QueryAtomParser<String, ? extends Comparable<?>>>of(
                        QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec())),
                        QueryAtomParser.valueOf(Attributes.CONTENT_TYPE,
                                AttributeCoercers.enumCoercer(ContentType.fromKey())),
                        QueryAtomParser.valueOf(Attributes.SOURCE,
                                AttributeCoercers.enumCoercer(Sources.fromKey())),
                        QueryAtomParser.valueOf(Attributes.ALIASES_NAMESPACE,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.ALIASES_VALUE,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.TAG_RELATIONSHIP,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.TAG_SUPERVISED,
                                AttributeCoercers.booleanCoercer()),
                        QueryAtomParser.valueOf(Attributes.TAG_WEIGHTING,
                                AttributeCoercers.floatCoercer()),
                        QueryAtomParser.valueOf(Attributes.CONTENT_TITLE_PREFIX,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.GENRE,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.CONTENT_GROUP,
                                AttributeCoercers.idCoercer(idCodec())),
                        QueryAtomParser.valueOf(Attributes.SPECIALIZATION,
                                AttributeCoercers.enumCoercer(Specialization.FROM_KEY()))
                )
        );
    }

    private StandardQueryParser<Content> contentQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(
                configFetcher,
                userFetcher,
                new IndexAnnotationsExtractor(contentAnnotationIndex()), selectionBuilder()
        );

        return new StandardQueryParser<>(
                Resource.CONTENT,
                contentQueryAttributeParser(),
                idCodec(), contextParser
        );
    }

    @Bean ChannelController channelController() {
        return new ChannelController(
                channelQueryParser(),
                channelQueryExecutor,
                new ChannelQueryResultWriter(channelListWriter(), licenseWriter, requestWriter())
        );
    }

    @Bean ChannelGroupController channelGroupController() {
        return new ChannelGroupController(
                channelGroupQueryParser(),
                channelGroupQueryExecutor,
                new ChannelGroupQueryResultWriter(
                        channelGroupListWriter(),
                        licenseWriter,
                        requestWriter()
                )
        );
    }

    private ChannelWriter channelWriter() {
        return new ChannelWriter(
                channelGroupResolver,
                "channels",
                "channel",
                new ChannelGroupSummaryWriter(idCodec())
        );
    }
    private ChannelGroupListWriter channelGroupListWriter() {
        return new ChannelGroupListWriter(AnnotationRegistry.<ChannelGroup<?>>builder()
                .registerDefault(CHANNEL_GROUP, new ChannelGroupAnnotation())
                .register(
                        CHANNELS,
                        new ChannelGroupChannelsAnnotation(
                                new ChannelGroupChannelWriter(
                                        channelWriter()
                                ),
                                channelResolver),
                        CHANNEL_GROUP
                )
                .register(REGIONS, new PlatformAnnontation(channelGroupResolver), CHANNEL_GROUP)
                .register(PLATFORM, new RegionsAnnotation(channelGroupResolver), CHANNEL_GROUP)
                .register(CHANNEL_GROUPS_SUMMARY,  NullWriter.create(ChannelGroup.class),ImmutableList.of(CHANNELS, CHANNEL_GROUP))
                .register(CHANNEL_GROUPS_ADVERTISED, new ChannelGroupAdvertisedChannelsAnnotation(new ChannelGroupChannelWriter(channelWriter()), channelResolver))
                .build());
    }

    private QueryParser<ChannelGroup<?>> channelGroupQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(
                configFetcher,
                userFetcher,
                new IndexAnnotationsExtractor(
                        channelGroupAnnotationIndex()
                ),
                selectionBuilder()
        );
        return new StandardQueryParser<>(
                Resource.CHANNEL_GROUP,
                channelGroupQueryAttributeParser(),
                idCodec(),
                contextParser
        );
    }

    private AnnotationIndex channelGroupAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CHANNEL_GROUP, Annotation.all()).build();
    }

    private QueryAttributeParser channelGroupQueryAttributeParser() {
        return new QueryAttributeParser(
                ImmutableList.of(
                        QueryAtomParser.valueOf(Attributes.ID,
                                AttributeCoercers.idCoercer(idCodec())),
                        QueryAtomParser.valueOf(Attributes.CHANNEL_GROUP_TYPE,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.CHANNEL_GROUP_CHANNEL_GENRES,
                                AttributeCoercers.stringCoercer())
                )
        );
    }

    private StandardQueryParser<Channel> channelQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(
                configFetcher,
                userFetcher,
                new IndexAnnotationsExtractor(
                        channelAnnotationIndex()
                ),
                selectionBuilder()
        );
        return new StandardQueryParser<>(
                Resource.CHANNEL,
                channelQueryAttributeParser(),
                idCodec(),
                contextParser
        );
    }

    private QueryAttributeParser channelQueryAttributeParser() {
        return new QueryAttributeParser(
                ImmutableList.<QueryAtomParser<String, ? extends Comparable<?>>>of(
                        QueryAtomParser.valueOf(Attributes.ID,
                                AttributeCoercers.idCoercer(idCodec())),
                        QueryAtomParser.valueOf(Attributes.AVAILABLE_FROM,
                                AttributeCoercers.enumCoercer(Sources.fromKey())),
                        QueryAtomParser.valueOf(Attributes.BROADCASTER,
                                AttributeCoercers.enumCoercer(Sources.fromKey())),
                        QueryAtomParser.valueOf(Attributes.ORDER_BY_CHANNEL,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.ADVERTISED_ON,
                                AttributeCoercers.booleanCoercer()),
                        QueryAtomParser.valueOf(
                                Attributes.MEDIA_TYPE,
                                AttributeCoercers.enumCoercer(
                                        new Function<String, Optional<MediaType>>() {
                                            @Override
                                            public Optional<MediaType> apply(
                                                    String input) {
                                                return MediaType.fromKey(input);
                                            }
                                        }
                                )
                        )
                )
        );
    }

    private StandardQueryParser<Topic> topicQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(configFetcher, userFetcher,
                new IndexAnnotationsExtractor(topicAnnotationIndex()), selectionBuilder());

        return new StandardQueryParser<Topic>(Resource.TOPIC,
                new QueryAttributeParser(ImmutableList.<QueryAtomParser<String, ? extends Comparable<?>>>of(
                        QueryAtomParser.valueOf(Attributes.ID,
                                AttributeCoercers.idCoercer(idCodec())),
                        QueryAtomParser.valueOf(Attributes.TOPIC_TYPE,
                                AttributeCoercers.enumCoercer(Topic.Type.fromKey())),
                        QueryAtomParser.valueOf(Attributes.SOURCE,
                                AttributeCoercers.enumCoercer(Sources.fromKey())),
                        QueryAtomParser.valueOf(Attributes.ALIASES_NAMESPACE,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.ALIASES_VALUE,
                                AttributeCoercers.stringCoercer())
                )),
                idCodec(), contextParser
        );
    }

    private StandardQueryParser<Event> eventQueryParser() {
        QueryContextParser contextParser = new QueryContextParser(configFetcher, userFetcher,
                new IndexAnnotationsExtractor(eventAnnotationIndex()), selectionBuilder());

        return new StandardQueryParser<Event>(Resource.EVENT,
                new QueryAttributeParser(ImmutableList.<QueryAtomParser<String, ? extends Comparable<?>>>of(
                        QueryAtomParser.valueOf(Attributes.ID,
                                AttributeCoercers.idCoercer(idCodec())),
                        QueryAtomParser.valueOf(Attributes.SOURCE,
                                AttributeCoercers.enumCoercer(Sources.fromKey())),
                        QueryAtomParser.valueOf(Attributes.ALIASES_NAMESPACE,
                                AttributeCoercers.stringCoercer()),
                        QueryAtomParser.valueOf(Attributes.ALIASES_VALUE,
                                AttributeCoercers.stringCoercer())
                )),
                idCodec(), contextParser
        );
    }

    @Bean PopularTopicController popularTopicController() {
        return new PopularTopicController(topicResolver,
                popularTopicIndex,
                new TopicQueryResultWriter(topicListWriter(), licenseWriter, requestWriter()),
                configFetcher);
    }

    @Bean SearchController searchController() {
        return new SearchController(v4SearchResolver,
                configFetcher,
                new ContentQueryResultWriter(contentListWriter(), licenseWriter, requestWriter(), channelGroupResolver, idCodec()));
    }

    @Bean ResourceAnnotationIndex contentAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CONTENT, Annotation.all())
                .attach(Annotation.TAGS, topicAnnotationIndex(), Annotation.ID)
                .build();
    }

    @Bean ResourceAnnotationIndex topicAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.TOPIC, Annotation.all()).build();
    }
    @Bean ResourceAnnotationIndex eventAnnotationIndex(){
        return ResourceAnnotationIndex.builder(Resource.EVENT, Annotation.all())
                .attach(Annotation.EVENT_DETAILS, topicAnnotationIndex(), Annotation.ID)
                .build();
    }

    @Bean ResourceAnnotationIndex channelAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CHANNEL, Annotation.all()).build();
    }

    @Bean ResourceAnnotationIndex modelInfoAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.MODEL_INFO, Annotation.all()).build();
    }

    @Bean ResourceAnnotationIndex endpointInfoAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.ENDPOINT_INFO, Annotation.all()).build();
    }

    @Bean EntityListWriter<Content> contentListWriter() {
        return new ContentListWriter(contentAnnotations());
    }

    private AnnotationRegistry<Content> contentAnnotations() {
        ImmutableSet<Annotation> commonImplied = ImmutableSet.of(ID_SUMMARY);
        return AnnotationRegistry.<Content>builder()
                .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
                .register(ID, new IdentificationAnnotation(), commonImplied)
                .register(EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID))
                .register(SERIES_REFERENCE, new SeriesReferenceAnnotation(idCodec()), commonImplied)
                .register(
                        SERIES_SUMMARY,
                        new SeriesSummaryAnnotation(
                                new SeriesSummaryWriter(idCodec(),containerSummaryResolver)
                        ),
                        commonImplied,
                        ImmutableSet.of(SERIES_REFERENCE)
                )
                .register(BRAND_REFERENCE, new BrandReferenceAnnotation(idCodec()), commonImplied)
                .register(
                        BRAND_SUMMARY,
                        new ContainerSummaryAnnotation(
                                CONTAINER_FIELD,
                                new ContainerSummaryWriter(idCodec(),
                                        CONTAINER_FIELD,
                                        containerSummaryResolver)
                        ),
                        commonImplied,
                        ImmutableSet.of(BRAND_REFERENCE)
                )
                .register(SERIES, new SeriesAnnotation(new SeriesWriter(persistenceModule.contentStore())), commonImplied)
                .register(DESCRIPTION,
                        new ContentDescriptionAnnotation(),
                        ImmutableSet.of(ID, SERIES_REFERENCE, BRAND_REFERENCE))
                .register(EXTENDED_DESCRIPTION,
                        new ExtendedDescriptionAnnotation(),
                        ImmutableSet.of(DESCRIPTION, EXTENDED_ID))
                .register(SUB_ITEMS, new SubItemAnnotation(idCodec()), commonImplied)
                .register(CLIPS, new ClipsAnnotation(), commonImplied)
                .register(PEOPLE, new PeopleAnnotation(), commonImplied)
                .register(TAGS,
                        new TopicsAnnotation(topicResolver, topicListWriter()),
                        commonImplied)
                .register(SEGMENT_EVENTS,
                        new SegmentEventsAnnotation(segmentRelatedLinkMergingFetcher()),
                        commonImplied)
                .register(RELATED_LINKS, new RelatedLinksAnnotation(), commonImplied)
                .register(KEY_PHRASES, new KeyPhrasesAnnotation(), commonImplied)
                .register(LOCATIONS,
                        new LocationsAnnotation(
                                persistenceModule.playerResolver(),
                                persistenceModule.serviceResolver()
                        ),
                        commonImplied
                )
                .register(BROADCASTS, new BroadcastsAnnotation(idCodec(), channelResolver, channelGroupResolver), commonImplied)
                .register(UPCOMING_BROADCASTS, new UpcomingBroadcastsAnnotation(idCodec(), channelResolver, channelGroupResolver), commonImplied)
                .register(CURRENT_AND_FUTURE_BROADCASTS, new CurrentAndFutureBroadcastsAnnotation(idCodec(), channelResolver, channelGroupResolver), commonImplied)
                .register(FIRST_BROADCASTS, new FirstBroadcastAnnotation(idCodec(), channelResolver, channelGroupResolver), commonImplied)
                .register(NEXT_BROADCASTS,
                        new NextBroadcastAnnotation(new SystemClock(), idCodec(), channelResolver, channelGroupResolver),
                        commonImplied)
                .register(AVAILABLE_LOCATIONS, new AvailableLocationsAnnotation(
                                persistenceModule.playerResolver(),
                                persistenceModule.serviceResolver()
                        ),
                        commonImplied
                )
                .register(IMAGES, new ImagesAnnotation(), commonImplied)
                .register(CHANNELS, new ChannelsAnnotation(), commonImplied)
                .register(CONTENT_SUMMARY,
                        NullWriter.create(Content.class),
                        ImmutableSet.of(DESCRIPTION, BRAND_SUMMARY,
                                SERIES_SUMMARY, BROADCASTS, LOCATIONS))
                .register(CONTENT_DETAIL,
                        NullWriter.create(Content.class),
                        ImmutableSet.of(EXTENDED_DESCRIPTION,
                                SUB_ITEMS,
                                CLIPS,
                                PEOPLE,
                                BRAND_SUMMARY,
                                SERIES_SUMMARY,
                                BROADCASTS,
                                LOCATIONS,
                                KEY_PHRASES,
                                RELATED_LINKS))
                .register(
                        UPCOMING_CONTENT_DETAIL,
                        new UpcomingContentDetailAnnotation(
                                queryModule.mergingContentResolver(),
                                new UpcomingContentDetailWriter(
                                        new BroadcastWriter("broadcasts", idCodec(), channelResolver, channelGroupResolver),
                                        new ItemDetailWriter(new IdentificationSummaryAnnotation(idCodec()))
                                )
                        ), commonImplied)
                .register(
                        AVAILABLE_CONTENT_DETAIL,
                        new AvailableContentDetailAnnotation(
                                queryModule.mergingContentResolver(),
                                new ItemDetailWriter(
                                        new IdentificationSummaryAnnotation(idCodec()),
                                        AvailableContentDetailAnnotation.AVAILABLE_CONTENT_DETAIL,
                                        new LocationsAnnotation(
                                                persistenceModule.playerResolver(),
                                                persistenceModule.serviceResolver()
                                        )
                                )
                        ), commonImplied)
                .register(
                        AVAILABLE_CONTENT,
                        new AvailableContentAnnotation(
                                new ItemRefWriter(idCodec(), AVAILABLE_CONTENT.toKey())
                        ), commonImplied)
                .register(
                        SUB_ITEM_SUMMARIES,
                        new SubItemSummariesAnnotations(
                                new SubItemSummaryListWriter(
                                        new ItemRefWriter(idCodec(), "items", "item")
                                )
                        )
                        ,
                        commonImplied
                )
                .build();
    }

    @Bean
    protected EntityListWriter<Topic> topicListWriter() {
        return new TopicListWriter(AnnotationRegistry.<Topic>builder()
                .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
                .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                .register(EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID))
                .register(DESCRIPTION, new DescriptionAnnotation<>(), ImmutableSet.of(EXTENDED_ID))
                .build());
    }

    @Bean
    protected EntityListWriter<Event> eventListWriter() {
        return new EventListWriter(AnnotationRegistry.<Event>builder()
                .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
                .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                .register(EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID))
                .register(EVENT, new EventAnnotation(new ItemRefWriter(idCodec(), "content")))
                .register(EVENT_DETAILS, new EventDetailsAnnotation(topicAnnotationRegistry()))
                .build());
    }

    private AnnotationRegistry<Topic> topicAnnotationRegistry(){
        return AnnotationRegistry.<Topic>builder()
                .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
                .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                .register(EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID))
                .register(DESCRIPTION, new DescriptionAnnotation<>(), ImmutableSet.of(EXTENDED_ID))
                .build();
    }

    @Bean SegmentRelatedLinkMergingFetcher segmentRelatedLinkMergingFetcher() {
        return new SegmentRelatedLinkMergingFetcher(persistenceModule.segmentStore(),
                new ScrubbablesSegmentRelatedLinkMerger());
    }

    protected EntityListWriter<org.atlasapi.media.channel.Channel> legacyChannelListWriter() {
        return new LegacyChannelListWriter(AnnotationRegistry.<org.atlasapi.media.channel.Channel>builder()
                //            .registerDefault(ID_SUMMARY, new IdentificationSummaryAnnotation(idCodec()))
                //            .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                //            .register(EXTENDED_ID, new ExtendedIdentificationAnnotation(idCodec()), ImmutableSet.of(ID))
                .registerDefault(CHANNEL_SUMMARY, new ChannelSummaryWriter(idCodec()))
                .register(CHANNEL, new LegacyChannelAnnotation(), ImmutableSet.of(CHANNEL_SUMMARY))
                .build());
    }

    protected EntityListWriter<Channel> channelListWriter() {
        return new ChannelListWriter(
                AnnotationRegistry.<Channel>builder()
                        .registerDefault(CHANNEL, new ChannelAnnotation(channelWriter()))
                        .register(
                                CHANNEL_GROUPS,
                                new ChannelGroupMembershipAnnotation(
                                        new ChannelGroupMembershipListWriter(
                                                "channel_groups",
                                                "channel_group",
                                                channelGroupResolver
                                        )
                                ),
                                CHANNEL
                        )
                        .register(PARENT, new ParentChannelAnnotation(channelWriter(), channelResolver), CHANNEL)
                        .register(VARIATIONS,
                                new ChannelVariationAnnotation(channelResolver, channelWriter()),
                                CHANNEL)
                        .build());
    }

    @Bean
    protected EntityListWriter<ModelClassInfo> modelListWriter() {
        return new ModelInfoListWriter(AnnotationRegistry.<ModelClassInfo>builder()
                .registerDefault(META_MODEL, new ModelInfoAnnotation<>(linkCreator()))
                .build());
    }

    @Bean
    protected EntityListWriter<EndpointClassInfo> endpointListWriter() {
        return new EndpointInfoListWriter(AnnotationRegistry.<EndpointClassInfo>builder()
                .registerDefault(META_ENDPOINT, new EndpointInfoAnnotation<>(linkCreator()))
                .build());
    }
}
