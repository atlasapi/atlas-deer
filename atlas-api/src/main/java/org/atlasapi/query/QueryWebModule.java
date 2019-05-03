package org.atlasapi.query;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.LicenseModule;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.channel.ResolvedChannelGroup;
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
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.ChannelMerger;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ScrubbablesSegmentRelatedLinkMerger;
import org.atlasapi.output.SegmentRelatedLinkMergingFetcher;
import org.atlasapi.output.annotation.AggregatedBroadcastsAnnotation;
import org.atlasapi.output.annotation.AvailableContentAnnotation;
import org.atlasapi.output.annotation.AvailableContentDetailAnnotation;
import org.atlasapi.output.annotation.AvailableLocationsAnnotation;
import org.atlasapi.output.annotation.BrandReferenceAnnotation;
import org.atlasapi.output.annotation.BroadcastsAnnotation;
import org.atlasapi.output.annotation.ChannelAnnotation;
import org.atlasapi.output.annotation.ChannelGroupAdvertisedChannelsAnnotation;
import org.atlasapi.output.annotation.ChannelGroupAnnotation;
import org.atlasapi.output.annotation.ChannelGroupChannelIdsAnnotation;
import org.atlasapi.output.annotation.ChannelGroupChannelsAnnotation;
import org.atlasapi.output.annotation.ChannelGroupIdSummaryAnnotation;
import org.atlasapi.output.annotation.ChannelGroupMembershipAnnotation;
import org.atlasapi.output.annotation.ChannelGroupMembershipListWriter;
import org.atlasapi.output.annotation.ChannelIdSummaryAnnotation;
import org.atlasapi.output.annotation.ChannelVariationAnnotation;
import org.atlasapi.output.annotation.ChannelsAnnotation;
import org.atlasapi.output.annotation.ClipsAnnotation;
import org.atlasapi.output.annotation.ContainerSummaryAnnotation;
import org.atlasapi.output.annotation.ContentDescriptionAnnotation;
import org.atlasapi.output.annotation.CurrentAndFutureBroadcastsAnnotation;
import org.atlasapi.output.annotation.CustomFieldsAnnotation;
import org.atlasapi.output.annotation.DescriptionAnnotation;
import org.atlasapi.output.annotation.EndpointInfoAnnotation;
import org.atlasapi.output.annotation.EventAnnotation;
import org.atlasapi.output.annotation.EventDetailsAnnotation;
import org.atlasapi.output.annotation.ExtendedDescriptionAnnotation;
import org.atlasapi.output.annotation.ExtendedIdentificationAnnotation;
import org.atlasapi.output.annotation.FirstBroadcastAnnotation;
import org.atlasapi.output.annotation.IdentificationAnnotation;
import org.atlasapi.output.annotation.IdentificationSummaryAnnotation;
import org.atlasapi.output.annotation.KeyPhrasesAnnotation;
import org.atlasapi.output.annotation.LocationsAnnotation;
import org.atlasapi.output.annotation.ModelInfoAnnotation;
import org.atlasapi.output.annotation.ModifiedDatesAnnotation;
import org.atlasapi.output.annotation.NextBroadcastAnnotation;
import org.atlasapi.output.annotation.NullWriter;
import org.atlasapi.output.annotation.ParentChannelAnnotation;
import org.atlasapi.output.annotation.PeopleAnnotation;
import org.atlasapi.output.annotation.PlatformAnnotation;
import org.atlasapi.output.annotation.RatingsAnnotation;
import org.atlasapi.output.annotation.RegionsAnnotation;
import org.atlasapi.output.annotation.RelatedLinksAnnotation;
import org.atlasapi.output.annotation.ReviewsAnnotation;
import org.atlasapi.output.annotation.SegmentEventsAnnotation;
import org.atlasapi.output.annotation.SeriesAnnotation;
import org.atlasapi.output.annotation.SeriesReferenceAnnotation;
import org.atlasapi.output.annotation.SeriesSummaryAnnotation;
import org.atlasapi.output.annotation.SubItemAnnotation;
import org.atlasapi.output.annotation.SubItemSummariesAnnotations;
import org.atlasapi.output.annotation.TopicsAnnotation;
import org.atlasapi.output.annotation.UpcomingBroadcastsAnnotation;
import org.atlasapi.output.annotation.UpcomingContentDetailAnnotation;
import org.atlasapi.output.writers.BroadcastWriter;
import org.atlasapi.output.writers.ChannelGroupChannelIdsWriter;
import org.atlasapi.output.writers.ContainerSummaryWriter;
import org.atlasapi.output.writers.IdSummaryWriter;
import org.atlasapi.output.writers.ItemDetailWriter;
import org.atlasapi.output.writers.ItemRefWriter;
import org.atlasapi.output.writers.RatingsWriter;
import org.atlasapi.output.writers.RequestWriter;
import org.atlasapi.output.writers.ReviewsWriter;
import org.atlasapi.output.writers.SeriesSummaryWriter;
import org.atlasapi.output.writers.SeriesWriter;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.output.writers.SubItemSummaryListWriter;
import org.atlasapi.output.writers.UpcomingContentDetailWriter;
import org.atlasapi.output.writers.common.CommonContainerSummaryWriter;
import org.atlasapi.query.annotation.AnnotationIndex;
import org.atlasapi.query.annotation.ImagesAnnotation;
import org.atlasapi.query.annotation.IndexContextualAnnotationsExtractor;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.ContextualQueryContextParser;
import org.atlasapi.query.common.ContextualQueryParser;
import org.atlasapi.query.common.IndexAnnotationsExtractor;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryParser;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.StandardQueryParser;
import org.atlasapi.query.common.attributes.QueryAtomParser;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.coercers.BooleanCoercer;
import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.common.coercers.FloatCoercer;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;
import org.atlasapi.query.common.context.QueryContextParser;
import org.atlasapi.query.v4.channel.ChannelController;
import org.atlasapi.query.v4.channel.ChannelIdWriter;
import org.atlasapi.query.v4.channel.ChannelListWriter;
import org.atlasapi.query.v4.channel.ChannelQueryResultWriter;
import org.atlasapi.query.v4.channel.ChannelWriter;
import org.atlasapi.query.v4.channel.MergingChannelWriter;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;
import org.atlasapi.query.v4.channelgroup.ChannelGroupController;
import org.atlasapi.query.v4.channelgroup.ChannelGroupListWriter;
import org.atlasapi.query.v4.channelgroup.ChannelGroupQueryResultWriter;
import org.atlasapi.query.v4.content.ContentController;
import org.atlasapi.query.v4.event.EventController;
import org.atlasapi.query.v4.event.EventListWriter;
import org.atlasapi.query.v4.event.EventQueryResultWriter;
import org.atlasapi.query.v4.event.PersonListWriter;
import org.atlasapi.query.v4.meta.LinkCreator;
import org.atlasapi.query.v4.meta.MetaApiLinkCreator;
import org.atlasapi.query.v4.meta.endpoint.EndpointController;
import org.atlasapi.query.v4.meta.endpoint.EndpointInfoListWriter;
import org.atlasapi.query.v4.meta.endpoint.EndpointInfoQueryResultWriter;
import org.atlasapi.query.v4.meta.model.ModelController;
import org.atlasapi.query.v4.meta.model.ModelInfoListWriter;
import org.atlasapi.query.v4.meta.model.ModelInfoQueryResultWriter;
import org.atlasapi.query.v4.organisation.OrganisationController;
import org.atlasapi.query.v4.organisation.OrganisationListWriter;
import org.atlasapi.query.v4.organisation.OrganisationQueryResultWriter;
import org.atlasapi.query.v4.schedule.ContentListWriter;
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

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.atlasapi.annotation.Annotation.ADVERTISED_CHANNELS;
import static org.atlasapi.annotation.Annotation.AGGREGATED_BROADCASTS;
import static org.atlasapi.annotation.Annotation.ALL_AGGREGATED_BROADCASTS;
import static org.atlasapi.annotation.Annotation.ALL_BROADCASTS;
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
import static org.atlasapi.annotation.Annotation.CHANNEL_GROUPS_SUMMARY;
import static org.atlasapi.annotation.Annotation.CHANNEL_GROUP_INFO;
import static org.atlasapi.annotation.Annotation.CHANNEL_IDS;
import static org.atlasapi.annotation.Annotation.CLIPS;
import static org.atlasapi.annotation.Annotation.CONTENT_DETAIL;
import static org.atlasapi.annotation.Annotation.CONTENT_SUMMARY;
import static org.atlasapi.annotation.Annotation.CURRENT_AND_FUTURE_BROADCASTS;
import static org.atlasapi.annotation.Annotation.CUSTOM_FIELDS;
import static org.atlasapi.annotation.Annotation.DESCRIPTION;
import static org.atlasapi.annotation.Annotation.EVENT;
import static org.atlasapi.annotation.Annotation.EVENT_DETAILS;
import static org.atlasapi.annotation.Annotation.EXTENDED_DESCRIPTION;
import static org.atlasapi.annotation.Annotation.EXTENDED_ID;
import static org.atlasapi.annotation.Annotation.FIRST_BROADCASTS;
import static org.atlasapi.annotation.Annotation.FUTURE_CHANNELS;
import static org.atlasapi.annotation.Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY;
import static org.atlasapi.annotation.Annotation.ID;
import static org.atlasapi.annotation.Annotation.ID_SUMMARY;
import static org.atlasapi.annotation.Annotation.IMAGES;
import static org.atlasapi.annotation.Annotation.KEY_PHRASES;
import static org.atlasapi.annotation.Annotation.LOCATIONS;
import static org.atlasapi.annotation.Annotation.META_ENDPOINT;
import static org.atlasapi.annotation.Annotation.META_MODEL;
import static org.atlasapi.annotation.Annotation.MODIFIED_DATES;
import static org.atlasapi.annotation.Annotation.NEXT_BROADCASTS;
import static org.atlasapi.annotation.Annotation.NON_MERGED;
import static org.atlasapi.annotation.Annotation.PARENT;
import static org.atlasapi.annotation.Annotation.PEOPLE;
import static org.atlasapi.annotation.Annotation.PLATFORM;
import static org.atlasapi.annotation.Annotation.RATINGS;
import static org.atlasapi.annotation.Annotation.REGIONS;
import static org.atlasapi.annotation.Annotation.RELATED_LINKS;
import static org.atlasapi.annotation.Annotation.REVIEWS;
import static org.atlasapi.annotation.Annotation.SEGMENT_EVENTS;
import static org.atlasapi.annotation.Annotation.SERIES;
import static org.atlasapi.annotation.Annotation.SERIES_REFERENCE;
import static org.atlasapi.annotation.Annotation.SERIES_SUMMARY;
import static org.atlasapi.annotation.Annotation.SUB_ITEMS;
import static org.atlasapi.annotation.Annotation.SUB_ITEM_SUMMARIES;
import static org.atlasapi.annotation.Annotation.SUPPRESS_EPISODE_NUMBERS;
import static org.atlasapi.annotation.Annotation.TAGS;
import static org.atlasapi.annotation.Annotation.UPCOMING_BROADCASTS;
import static org.atlasapi.annotation.Annotation.UPCOMING_CONTENT_DETAIL;
import static org.atlasapi.annotation.Annotation.VARIATIONS;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import({ QueryModule.class, LicenseModule.class, KafkaMessagingModule.class })
public class QueryWebModule {

    private static final String CONTAINER_FIELD = "container";
    private @Value("${local.host.name}") String localHostName;
    private @Value("${atlas.uri}") String baseAtlasUri;

    private IdSummaryWriter idSummaryWriter = IdSummaryWriter.create();

    private
    @Autowired
    KafkaMessagingModule messaging;

    private
    @Autowired
    DatabasedMongo mongo;

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
    ApplicationFetcher configFetcher;

    private
    @Autowired
    AtlasPersistenceModule persistenceModule;

    private
    @Autowired
    QueryExecutor<ResolvedChannel> channelQueryExecutor;

    private
    @Autowired
    ChannelResolver channelResolver;

    private
    @Autowired
    QueryExecutor<ResolvedChannelGroup> channelGroupQueryExecutor;

    private
    @Autowired
    ChannelGroupResolver channelGroupResolver;

    private
    @Autowired
    ContainerSummaryResolver containerSummaryResolver;

    @Autowired
    @Qualifier("licenseWriter")
    EntityWriter<Object> licenseWriter;

    @Bean
    NumberToShortStringCodec idCodec() {
        return SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    @Bean
    SelectionBuilder selectionBuilder() {
        return Selection.builder().withDefaultLimit(50).withMaxLimit(250);
    }

    @Bean
    EntityWriter<HttpServletRequest> requestWriter() {
        return new RequestWriter();
    }

    @Bean
    ScheduleController v4ScheduleController() {
        EntityListWriter<ItemAndBroadcast> entryListWriter =
                new ScheduleEntryListWriter(
                        contentListWriter(),
                        BroadcastWriter.create(
                                "broadcasts",
                                "broadcast",
                                idCodec()
                        ),
                        channelResolver
                );
        ScheduleListWriter scheduleWriter = new ScheduleListWriter(
                channelListWriter(),
                entryListWriter,
                channelResolver
        );
        return new ScheduleController(
                queryModule.equivalentScheduleStoreScheduleQueryExecutor(),
                configFetcher,
                new ScheduleQueryResultWriter(scheduleWriter, licenseWriter, requestWriter()),
                IndexContextualAnnotationsExtractor.create(ResourceAnnotationIndex.combination()
                        .addExplicitSingleContext(channelAnnotationIndex())
                        .addExplicitListContext(contentAnnotationIndex())
                        .combine())
        );
    }

    @Bean
    TopicController v4TopicController() {
        return new TopicController(
                topicQueryParser(),
                queryModule.topicQueryExecutor(),
                new TopicQueryResultWriter(topicListWriter(), licenseWriter, requestWriter())
        );
    }

    @Bean
    ContentController contentController() {
        return new ContentController(
                contentQueryParser(),
                queryModule.contentQueryExecutor(),
                new ContentQueryResultWriter(
                        contentListWriter(),
                        licenseWriter,
                        requestWriter(),
                        channelGroupResolver,
                        idCodec()
                )
        );
    }

    @Bean
    TopicContentController topicContentController() {
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(
                configFetcher,
                IndexContextualAnnotationsExtractor.create(ResourceAnnotationIndex.combination()
                        .addImplicitListContext(contentAnnotationIndex())
                        .addExplicitSingleContext(topicAnnotationIndex())
                        .combine()
                ),
                selectionBuilder()
        );

        ContextualQueryParser<Topic, Content> parser = new ContextualQueryParser<>(
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

    @Bean
    EventController eventController() {
        return new EventController(
                eventQueryParser(),
                queryModule.eventQueryExecutor(),
                new EventQueryResultWriter(eventListWriter(), licenseWriter, requestWriter())
        );
    }

    @Bean
    OrganisationController organisationController() {
        return new OrganisationController(
                organisationQueryParser(),
                queryModule.organisationQueryExecutor(),
                new OrganisationQueryResultWriter(
                        organisationListWriter(),
                        licenseWriter,
                        requestWriter()
                )
        );
    }

    @Bean
    LinkCreator linkCreator() {
        return new MetaApiLinkCreator(baseAtlasUri);
    }

    @Bean
    ModelController modelController() {
        QueryResultWriter<ModelClassInfo> resultWriter = new ModelInfoQueryResultWriter(
                modelListWriter(),
                licenseWriter,
                requestWriter()
        );
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(
                configFetcher,
                IndexContextualAnnotationsExtractor.create(ResourceAnnotationIndex.combination()
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

    @Bean
    EndpointController endpointController() {
        QueryResultWriter<EndpointClassInfo> resultWriter = new EndpointInfoQueryResultWriter(
                endpointListWriter(),
                licenseWriter,
                requestWriter()
        );
        ContextualQueryContextParser contextParser = new ContextualQueryContextParser(
                configFetcher,
                IndexContextualAnnotationsExtractor.create(ResourceAnnotationIndex.combination()
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
        return QueryAttributeParser.create(
                ImmutableList.<QueryAtomParser<? extends Comparable<?>>>of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.CONTENT_TYPE,
                                EnumCoercer.create(ContentType.fromKey())
                        ),
                        QueryAtomParser.create(
                                Attributes.SOURCE,
                                EnumCoercer.create(Sources.fromKey())
                        ),
                        QueryAtomParser.create(
                                Attributes.ALIASES_NAMESPACE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.ALIASES_VALUE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.LOCATIONS_ALIASES_NAMESPACE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.LOCATIONS_ALIASES_VALUE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.TAG_RELATIONSHIP,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.TAG_SUPERVISED,
                                BooleanCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.TAG_WEIGHTING,
                                FloatCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.CONTENT_TITLE_PREFIX,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.GENRE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.CONTENT_GROUP,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.SPECIALIZATION,
                                EnumCoercer.create(Specialization.FROM_KEY())
                        ),
                        QueryAtomParser.create(
                                Attributes.Q,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.TITLE_BOOST,
                                FloatCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.ORDER_BY,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.REGION,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.PLATFORM,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_DTT_CHANNELS,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_IP_CHANNELS,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.BROADCAST_WEIGHT,
                                FloatCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.SEARCH_TOPIC_ID,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.BRAND_ID,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.EPISODE_BRAND_ID,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.ACTIONABLE_FILTER_PARAMETERS,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.SERIES_ID,
                                IdCoercer.create(idCodec())
                        )
                )
        );
    }

    private StandardQueryParser<Content> contentQueryParser() {
        QueryContextParser contextParser = QueryContextParser.create(
                configFetcher,
                new IndexAnnotationsExtractor(contentAnnotationIndex()), selectionBuilder()
        );

        return StandardQueryParser.create(
                Resource.CONTENT,
                contentQueryAttributeParser(),
                idCodec(),
                contextParser
        );
    }

    @Bean
    ChannelController channelController() {
        return new ChannelController(
                channelQueryParser(),
                channelQueryExecutor,
                new ChannelQueryResultWriter(
                        channelListWriter(),
                        licenseWriter,
                        requestWriter()
                )
        );
    }

    @Bean
    ChannelGroupController channelGroupController() {
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
        return MergingChannelWriter.create(
                "channels",
                "channel",
                ChannelGroupSummaryWriter.create(idCodec()),
                ChannelMerger.create()
        );
    }

    private ChannelIdWriter channelIdWriter() {
        return ChannelIdWriter.create("channels", "channel");
    }

    private ChannelGroupListWriter channelGroupListWriter() {
        return new ChannelGroupListWriter(AnnotationRegistry.<ResolvedChannelGroup>builder()
                .registerDefault(CHANNEL_GROUP, new ChannelGroupAnnotation())
                .register(
                        CHANNELS,
                        new ChannelGroupChannelsAnnotation(
                                new ChannelGroupChannelWriter(channelWriter())
                        ),
                        CHANNEL_GROUP
                )
                .register(
                        FUTURE_CHANNELS,
                        new ChannelGroupChannelsAnnotation(
                                new ChannelGroupChannelWriter(channelWriter())
                        ),
                        CHANNEL_GROUP
                )
                .register(ID_SUMMARY, ChannelGroupIdSummaryAnnotation.create(idSummaryWriter))
                .register(REGIONS, new PlatformAnnotation(), CHANNEL_GROUP)
                .register(PLATFORM, new RegionsAnnotation(), CHANNEL_GROUP)
                .register(
                        CHANNEL_GROUPS_SUMMARY,
                        NullWriter.create(ResolvedChannelGroup.class),
                        ImmutableList.of(CHANNELS, CHANNEL_GROUP)
                )
                .register(
                        GENERIC_CHANNEL_GROUPS_SUMMARY,
                        NullWriter.create(ResolvedChannelGroup.class),
                        ImmutableList.of(CHANNEL_GROUPS_SUMMARY)
                )
                .register(
                        ADVERTISED_CHANNELS,
                        new ChannelGroupAdvertisedChannelsAnnotation(
                                new ChannelGroupChannelWriter(channelWriter()
                                )
                        )
                )
                .register(CHANNEL_GROUP_INFO, new ChannelGroupAnnotation())
                .register(
                        CHANNEL_IDS,
                        new ChannelGroupChannelIdsAnnotation(
                                new ChannelGroupChannelIdsWriter(
                                        channelIdWriter()
                                )
                        )
                )
                .build());
    }

    private QueryParser<ResolvedChannelGroup> channelGroupQueryParser() {
        QueryContextParser contextParser = QueryContextParser.create(
                configFetcher,
                new IndexAnnotationsExtractor(
                        channelGroupAnnotationIndex()
                ),
                selectionBuilder()
        );
        return StandardQueryParser.create(
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
        return QueryAttributeParser.create(
                ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_TYPE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_CHANNEL_GENRES,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_DTT_CHANNELS,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_IP_CHANNELS,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.SOURCE,
                                EnumCoercer.create(Sources.fromKey())
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_REFRESH_CACHE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_IDS,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.CHANNEL_GROUP_REQUEST_TIMESTAMP,
                                StringCoercer.create()
                        )
                )
        );
    }

    private StandardQueryParser<ResolvedChannel> channelQueryParser() {
        QueryContextParser contextParser = QueryContextParser.create(
                configFetcher,
                new IndexAnnotationsExtractor(
                        channelAnnotationIndex()
                ),
                selectionBuilder()
        );
        return StandardQueryParser.create(
                Resource.CHANNEL,
                channelQueryAttributeParser(),
                idCodec(),
                contextParser
        );
    }

    private QueryAttributeParser channelQueryAttributeParser() {
        return QueryAttributeParser.create(
                ImmutableList.<QueryAtomParser<? extends Comparable<?>>>of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec())
                        ),
                        QueryAtomParser.create(
                                Attributes.AVAILABLE_FROM,
                                EnumCoercer.create(Sources.fromKey())
                        ),
                        QueryAtomParser.create(
                                Attributes.BROADCASTER,
                                EnumCoercer.create(Sources.fromKey())
                        ),
                        QueryAtomParser.create(
                                Attributes.ORDER_BY_CHANNEL,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.ADVERTISED_ON,
                                BooleanCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.MEDIA_TYPE,
                                EnumCoercer.create(MediaType::fromKey)
                        ),
                        QueryAtomParser.create(
                                Attributes.ALIASES_NAMESPACE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.ALIASES_VALUE,
                                StringCoercer.create()
                        ),
                        QueryAtomParser.create(
                                Attributes.REFRESH_CACHE,
                                StringCoercer.create()
                        )
                )
        );
    }

    private StandardQueryParser<Topic> topicQueryParser() {
        QueryContextParser contextParser = QueryContextParser.create(
                configFetcher,
                new IndexAnnotationsExtractor(topicAnnotationIndex()),
                selectionBuilder()
        );

        return StandardQueryParser.create(
                Resource.TOPIC,
                QueryAttributeParser.create(
                        ImmutableList.<QueryAtomParser<? extends Comparable<?>>>of(
                                QueryAtomParser.create(
                                        Attributes.ID,
                                        IdCoercer.create(idCodec())
                                ),
                                QueryAtomParser.create(
                                        Attributes.TOPIC_TYPE,
                                        EnumCoercer.create(Topic.Type.fromKey())
                                ),
                                QueryAtomParser.create(
                                        Attributes.SOURCE,
                                        EnumCoercer.create(Sources.fromKey())
                                ),
                                QueryAtomParser.create(
                                        Attributes.ALIASES_NAMESPACE,
                                        StringCoercer.create()
                                ),
                                QueryAtomParser.create(
                                        Attributes.ALIASES_VALUE,
                                        StringCoercer.create()
                                )
                        )
                ),
                idCodec(),
                contextParser
        );
    }

    private StandardQueryParser<Event> eventQueryParser() {
        QueryContextParser contextParser = QueryContextParser.create(
                configFetcher,
                new IndexAnnotationsExtractor(eventAnnotationIndex()),
                selectionBuilder()
        );

        return StandardQueryParser.create(Resource.EVENT,
                QueryAttributeParser.create(
                        ImmutableList.<QueryAtomParser<? extends Comparable<?>>>of(
                                QueryAtomParser.create(
                                        Attributes.ID,
                                        IdCoercer.create(idCodec())
                                ),
                                QueryAtomParser.create(
                                        Attributes.SOURCE,
                                        EnumCoercer.create(Sources.fromKey())
                                ),
                                QueryAtomParser.create(
                                        Attributes.ALIASES_NAMESPACE,
                                        StringCoercer.create()
                                ),
                                QueryAtomParser.create(
                                        Attributes.ALIASES_VALUE,
                                        StringCoercer.create()
                                )
                        )
                ),
                idCodec(), contextParser
        );
    }

    private StandardQueryParser<Organisation> organisationQueryParser() {
        QueryContextParser contextParser = QueryContextParser.create(
                configFetcher,
                new IndexAnnotationsExtractor(organisationAnnotationIndex()),
                selectionBuilder()
        );

        return StandardQueryParser.create(Resource.ORGANISATION,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec())
                        )
                )),
                idCodec(), contextParser
        );
    }

    @Bean
    PopularTopicController popularTopicController() {
        return new PopularTopicController(
                topicResolver,
                popularTopicIndex,
                new TopicQueryResultWriter(topicListWriter(), licenseWriter, requestWriter()),
                configFetcher
        );
    }

    @Bean
    SearchController searchController() {
        return new SearchController(
                v4SearchResolver,
                configFetcher,
                new ContentQueryResultWriter(
                        contentListWriter(),
                        licenseWriter,
                        requestWriter(),
                        channelGroupResolver,
                        idCodec()
                )
        );
    }

    @Bean
    ResourceAnnotationIndex contentAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CONTENT, Annotation.all())
                .attach(Annotation.TAGS, topicAnnotationIndex(), Annotation.ID)
                .build();
    }

    @Bean
    ResourceAnnotationIndex topicAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.TOPIC, Annotation.all()).build();
    }

    @Bean
    ResourceAnnotationIndex eventAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.EVENT, Annotation.all())
                .attach(Annotation.EVENT_DETAILS, topicAnnotationIndex(), Annotation.ID)
                .build();
    }

    @Bean
    ResourceAnnotationIndex channelAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.CHANNEL, Annotation.all()).build();
    }

    @Bean
    ResourceAnnotationIndex modelInfoAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.MODEL_INFO, Annotation.all()).build();
    }

    @Bean
    ResourceAnnotationIndex endpointInfoAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.ENDPOINT_INFO, Annotation.all()).build();
    }

    @Bean
    ResourceAnnotationIndex organisationAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.ORGANISATION, Annotation.all()).build();
    }

    @Bean
    EntityListWriter<Content> contentListWriter() {
        return new ContentListWriter(contentAnnotations());
    }

    private AnnotationRegistry<Content> contentAnnotations() {
        ImmutableSet<Annotation> commonImplied = ImmutableSet.of(ID_SUMMARY);
        return AnnotationRegistry.<Content>builder()
                .registerDefault(
                        ID_SUMMARY,
                        IdentificationSummaryAnnotation.create(idSummaryWriter)
                )
                .register(ID, new IdentificationAnnotation(), commonImplied)
                .register(
                        EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID)
                )
                .register(SERIES_REFERENCE, new SeriesReferenceAnnotation(idCodec()), commonImplied)
                .register(
                        SERIES_SUMMARY,
                        new SeriesSummaryAnnotation(SeriesSummaryWriter.create(
                                idCodec(),
                                containerSummaryResolver,
                                CommonContainerSummaryWriter.create()
                        )),
                        commonImplied,
                        ImmutableSet.of(SERIES_REFERENCE)
                )
                .register(BRAND_REFERENCE, new BrandReferenceAnnotation(idCodec()), commonImplied)
                .register(
                        BRAND_SUMMARY,
                        new ContainerSummaryAnnotation(
                                CONTAINER_FIELD,
                                ContainerSummaryWriter.create(
                                        idCodec(),
                                        CONTAINER_FIELD,
                                        containerSummaryResolver,
                                        CommonContainerSummaryWriter.create()
                                )
                        ),
                        commonImplied,
                        ImmutableSet.of(BRAND_REFERENCE)
                )
                .register(
                        SERIES,
                        new SeriesAnnotation(new SeriesWriter(persistenceModule.contentStore())),
                        commonImplied
                )
                .register(
                        DESCRIPTION,
                        new ContentDescriptionAnnotation(),
                        ImmutableSet.of(ID, SERIES_REFERENCE, BRAND_REFERENCE)
                )
                .register(
                        EXTENDED_DESCRIPTION,
                        new ExtendedDescriptionAnnotation(),
                        ImmutableSet.of(DESCRIPTION, EXTENDED_ID)
                )
                .register(SUB_ITEMS, new SubItemAnnotation(idCodec()), commonImplied)
                .register(CLIPS, new ClipsAnnotation(new LocationsAnnotation(
                        persistenceModule.playerResolver(),
                        persistenceModule.serviceResolver()
                )), commonImplied)
                .register(PEOPLE, new PeopleAnnotation(), commonImplied)
                .register(
                        TAGS,
                        new TopicsAnnotation(topicResolver, topicListWriter()),
                        commonImplied
                )
                .register(
                        SEGMENT_EVENTS,
                        new SegmentEventsAnnotation(segmentRelatedLinkMergingFetcher()),
                        commonImplied
                )
                .register(RELATED_LINKS, new RelatedLinksAnnotation(), commonImplied)
                .register(KEY_PHRASES, new KeyPhrasesAnnotation(), commonImplied)
                .register(
                        LOCATIONS,
                        new LocationsAnnotation(
                                persistenceModule.playerResolver(),
                                persistenceModule.serviceResolver()
                        ),
                        commonImplied
                )
                .register(ALL_AGGREGATED_BROADCASTS,
                        AggregatedBroadcastsAnnotation.create(idCodec(), channelResolver),
                        commonImplied
                )
                .register(
                        AGGREGATED_BROADCASTS,
                        AggregatedBroadcastsAnnotation.create(idCodec(), channelResolver),
                        commonImplied
                )
                .register(
                        BROADCASTS,
                        BroadcastsAnnotation.create(idCodec(), channelResolver),
                        commonImplied
                )
                .register(
                        UPCOMING_BROADCASTS,
                        UpcomingBroadcastsAnnotation.create(
                                idCodec(),
                                channelResolver
                        ),
                        commonImplied
                )
                .register(
                        CURRENT_AND_FUTURE_BROADCASTS,
                        CurrentAndFutureBroadcastsAnnotation.create(
                                idCodec(),
                                channelResolver
                        ),
                        commonImplied
                )
                .register(
                        FIRST_BROADCASTS,
                        FirstBroadcastAnnotation.create(
                                idCodec(),
                                channelResolver
                        ),
                        commonImplied
                )
                .register(
                        NEXT_BROADCASTS,
                        NextBroadcastAnnotation.create(
                                new SystemClock(),
                                idCodec(),
                                channelResolver
                        ),
                        commonImplied
                )
                .register(
                        ALL_BROADCASTS,
                        NullWriter.create(Content.class),
                        ImmutableSet.of(BROADCASTS)
                )
                .register(AVAILABLE_LOCATIONS, new AvailableLocationsAnnotation(
                                persistenceModule.playerResolver(),
                                persistenceModule.serviceResolver()
                        ),
                        commonImplied
                )
                .register(IMAGES, new ImagesAnnotation(), commonImplied)
                .register(CHANNELS, new ChannelsAnnotation(), commonImplied)
                .register(
                        CONTENT_SUMMARY,
                        NullWriter.create(Content.class),
                        ImmutableSet.of(DESCRIPTION, BRAND_SUMMARY,
                                SERIES_SUMMARY, BROADCASTS, LOCATIONS
                        )
                )
                .register(
                        CONTENT_DETAIL,
                        NullWriter.create(Content.class),
                        ImmutableSet.of(
                                EXTENDED_DESCRIPTION,
                                SUB_ITEMS,
                                CLIPS,
                                PEOPLE,
                                BRAND_SUMMARY,
                                SERIES_SUMMARY,
                                BROADCASTS,
                                LOCATIONS,
                                KEY_PHRASES,
                                RELATED_LINKS
                        )
                )
                .register(
                        UPCOMING_CONTENT_DETAIL,
                        new UpcomingContentDetailAnnotation(
                                queryModule.mergingContentResolver(),
                                new UpcomingContentDetailWriter(
                                        BroadcastWriter.create(
                                                "broadcasts",
                                                "broadcast",
                                                idCodec()
                                        ),
                                        new ItemDetailWriter(
                                                IdentificationSummaryAnnotation.create(
                                                        idSummaryWriter
                                                )
                                        ),
                                        channelResolver
                                )
                        ), commonImplied
                )
                .register(
                        AVAILABLE_CONTENT_DETAIL,
                        new AvailableContentDetailAnnotation(
                                queryModule.mergingContentResolver(),
                                new ItemDetailWriter(
                                        IdentificationSummaryAnnotation.create(idSummaryWriter),
                                        AvailableContentDetailAnnotation.AVAILABLE_CONTENT_DETAIL,
                                        new LocationsAnnotation(
                                                persistenceModule.playerResolver(),
                                                persistenceModule.serviceResolver()
                                        )
                                )
                        ), commonImplied
                )
                .register(
                        AVAILABLE_CONTENT,
                        new AvailableContentAnnotation(
                                new ItemRefWriter(idCodec(), AVAILABLE_CONTENT.toKey())
                        ), commonImplied
                )
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
                .register(
                        SUPPRESS_EPISODE_NUMBERS,
                        NullWriter.create(Content.class)
                )
                .register(NON_MERGED, NullWriter.create(Content.class))
                .register(REVIEWS, new ReviewsAnnotation(
                        new ReviewsWriter(SourceWriter.sourceWriter("source")))
                )
                .register(RATINGS, new RatingsAnnotation(
                        new RatingsWriter(SourceWriter.sourceWriter("source")))
                )
                .register(MODIFIED_DATES, new ModifiedDatesAnnotation())
                .register(CUSTOM_FIELDS, new CustomFieldsAnnotation())
                .build();
    }

    @Bean
    protected EntityListWriter<Topic> topicListWriter() {
        return new TopicListWriter(AnnotationRegistry.<Topic>builder()
                .registerDefault(
                        ID_SUMMARY,
                        IdentificationSummaryAnnotation.create(idSummaryWriter)
                )
                .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                .register(
                        EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID)
                )
                .register(DESCRIPTION, new DescriptionAnnotation<>(), ImmutableSet.of(EXTENDED_ID))
                .build());
    }

    @Bean
    protected EntityListWriter<Event> eventListWriter() {
        return new EventListWriter(AnnotationRegistry.<Event>builder()
                .registerDefault(
                        ID_SUMMARY,
                        IdentificationSummaryAnnotation.create(idSummaryWriter)
                )
                .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                .register(
                        EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID)
                )
                .register(
                        EVENT,
                        new EventAnnotation(
                                new ItemRefWriter(idCodec(), "content"),
                                persistenceModule.organisationStore()
                        )
                )
                .register(EVENT_DETAILS, new EventDetailsAnnotation(topicAnnotationRegistry()))
                .build());
    }

    @Bean
    protected EntityListWriter<Organisation> organisationListWriter() {
        return new OrganisationListWriter(new PersonListWriter());
    }

    private AnnotationRegistry<Topic> topicAnnotationRegistry() {
        return AnnotationRegistry.<Topic>builder()
                .registerDefault(
                        ID_SUMMARY,
                        IdentificationSummaryAnnotation.create(idSummaryWriter)
                )
                .register(ID, new IdentificationAnnotation(), ID_SUMMARY)
                .register(
                        EXTENDED_ID,
                        new ExtendedIdentificationAnnotation(idCodec()),
                        ImmutableSet.of(ID)
                )
                .register(DESCRIPTION, new DescriptionAnnotation<>(), ImmutableSet.of(EXTENDED_ID))
                .build();
    }

    @Bean
    SegmentRelatedLinkMergingFetcher segmentRelatedLinkMergingFetcher() {
        return new SegmentRelatedLinkMergingFetcher(
                persistenceModule.segmentStore(),
                new ScrubbablesSegmentRelatedLinkMerger()
        );
    }

    @SuppressWarnings("unchecked")
    private EntityListWriter<ResolvedChannel> channelListWriter() {
        return new ChannelListWriter(
                AnnotationRegistry.<ResolvedChannel>builder()
                        .registerDefault(CHANNEL, new ChannelAnnotation(channelWriter()))
                        .register(ID_SUMMARY, ChannelIdSummaryAnnotation.create(idSummaryWriter))
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
                        .register(
                                PARENT,
                                new ParentChannelAnnotation(channelWriter()),
                                CHANNEL
                        )
                        .register(
                                VARIATIONS,
                                new ChannelVariationAnnotation(channelWriter()),
                                CHANNEL
                        )
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
