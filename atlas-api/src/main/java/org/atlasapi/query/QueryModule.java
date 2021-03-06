/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.query;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Content;
import org.atlasapi.content.MergingEquivalentsResolverBackedContainerSummaryResolver;
import org.atlasapi.equivalence.AnnotationBasedMergingEquivalentsResolver;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.event.Event;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.output.EquivalentSetContentHierarchyChooser;
import org.atlasapi.output.MostPrecidentWithChildrenContentHierarchyChooser;
import org.atlasapi.output.OutputContentMerger;
import org.atlasapi.output.StrategyBackedEquivalentsMerger;
import org.atlasapi.query.common.ContextualQueryExecutor;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.v4.channel.ChannelQueryExecutor;
import org.atlasapi.query.v4.channelgroup.ChannelGroupQueryExecutor;
import org.atlasapi.query.v4.content.IndexBackedEquivalentContentQueryExecutor;
import org.atlasapi.query.v4.event.EventQueryExecutor;
import org.atlasapi.query.v4.organisation.OrganisationQueryExecutor;
import org.atlasapi.query.v4.schedule.EquivalentScheduleQueryExecutor;
import org.atlasapi.query.v4.schedule.ScheduleQueryExecutor;
import org.atlasapi.query.v4.search.ContentResolvingSearcher;
import org.atlasapi.query.v4.topic.IndexBackedTopicQueryExecutor;
import org.atlasapi.query.v4.topic.TopicContentQueryExecutor;
import org.atlasapi.schedule.FlexibleBroadcastMatcher;
import org.atlasapi.topic.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
//@Import(EquivModule.class)
public class QueryModule {

    private @Autowired AtlasPersistenceModule persistenceModule;

    @Bean
    QueryExecutor<Topic> topicQueryExecutor() {
        return new IndexBackedTopicQueryExecutor(
                persistenceModule.topicSearcher(),
                persistenceModule.topicStore()
        );
    }

    @Bean
    public ContextualQueryExecutor<Topic, Content> topicContentQueryExecutor() {
        return TopicContentQueryExecutor.create(
                persistenceModule.topicStore(),
                persistenceModule.equivContentSearcher(),
                mergingContentResolver()
        );
    }

    @Bean
    public QueryExecutor<Event> eventQueryExecutor() {
        return new EventQueryExecutor(persistenceModule.eventResolver());
    }

    @Bean
    public QueryExecutor<Organisation> organisationQueryExecutor() {
        return new OrganisationQueryExecutor(persistenceModule.organisationStore());
    }

    @Bean
    public QueryExecutor<Content> contentQueryExecutor() {
        return IndexBackedEquivalentContentQueryExecutor.create(
                persistenceModule.equivContentSearcher(),
                mergingContentResolver()
        );
    }

    @Bean
    public QueryExecutor<ResolvedChannel> channelQueryExecutor() {
        return ChannelQueryExecutor.create(
                persistenceModule.channelResolver(),
                persistenceModule.channelGroupResolver()
        );
    }

    @Bean
    public QueryExecutor<ResolvedChannelGroup> channelGroupQueryExecutor() {
        return new ChannelGroupQueryExecutor(
                persistenceModule.outputChannelGroupResolver(),
                persistenceModule.channelResolver());
    }

    public MergingEquivalentsResolver<Content> mergingContentResolver() {
        return new AnnotationBasedMergingEquivalentsResolver<>(
                persistenceModule.getEquivalentContentStore(),
                equivalentsMerger()
        );
    }

    private StrategyBackedEquivalentsMerger<Content> equivalentsMerger() {
        return new StrategyBackedEquivalentsMerger<>(new OutputContentMerger(
                contentHierarchyChooser()));
    }

    private EquivalentSetContentHierarchyChooser contentHierarchyChooser() {
        return new MostPrecidentWithChildrenContentHierarchyChooser();
    }

    @Qualifier("store")
    @Bean
    ScheduleQueryExecutor equivalentScheduleStoreScheduleQueryExecutor() {
        return new EquivalentScheduleQueryExecutor(persistenceModule.channelResolver(),
                persistenceModule.getEquivalentScheduleStore(),
                equivalentsMerger(),
                FlexibleBroadcastMatcher.exactStartEnd()
        );
    }

    @Bean
    public ContentResolvingSearcher searchResolver() {
        return new ContentResolvingSearcher(
                persistenceModule.sherlockSearcher(),
                mergingContentResolver(),
                60000
        );
    }

    @Bean
    public ContainerSummaryResolver containerSummaryResolver() {
        return new MergingEquivalentsResolverBackedContainerSummaryResolver(
                mergingContentResolver()
        );
    }

}
