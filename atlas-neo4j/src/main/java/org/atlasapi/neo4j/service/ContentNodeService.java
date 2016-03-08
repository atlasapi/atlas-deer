package org.atlasapi.neo4j.service;

import java.text.MessageFormat;
import java.util.stream.StreamSupport;

import org.atlasapi.neo4j.model.nodes.ContentNode;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentNodeService {

    public static final int DEPTH_LIST = 0;

    private final Session session;

    private ContentNodeService(Session session) {
        this.session = checkNotNull(session);
    }

    public static ContentNodeService create(Session session) {
        return new ContentNodeService(session);
    }

    public Iterable<ContentNode> findAll() {
        return session.loadAll(ContentNode.class, DEPTH_LIST);
    }

    public ContentNode find(Long id, int depth) {
        return session.load(ContentNode.class, id, depth);
    }

    public void delete(Long id) {
        session.delete(session.load(ContentNode.class, id));
    }

    public ContentNode createOrUpdate(ContentNode entity, int depth) {
        session.save(entity, depth);
        return find(entity.getId(), depth);
    }

    public ContentNode findByContentId(Long contentId) {
        String query = "MATCH (c:" + ContentNode.LABEL + " { contentId: {contentId} }) \n"
                + "RETURN c";
        return session.queryForObject(
                ContentNode.class, query, ImmutableMap.of("contentId", contentId)
        );
    }

    public Iterable<ContentNode> getEquivalentSet(Long id) {
        String query = MessageFormat.format(
                "MATCH (s:{0})-[:{1}*]-(p:{0}) WHERE s.contentId = {2} return s, p",
                ContentNode.LABEL,
                ContentNode.EQUIVALENT_TO,
                id
        );
        return session.query(ContentNode.class, query, ImmutableMap.of());
    }

    public ImmutableList<Long> getEquivalentSetIds(Long id) {
        String query = MessageFormat.format(
                "MATCH (s:{0})-[:{1}*]-(p:{0}) WHERE s.contentId = {2} RETURN p.contentId AS contentId",
                ContentNode.LABEL,
                ContentNode.EQUIVALENT_TO,
                id
        );
        Result result = session.query(query, ImmutableMap.of());

        return StreamSupport.stream(result.spliterator(), false)
                .map(resultEntry -> resultEntry.get("contentId"))
                .map(contentId -> Long.valueOf(contentId.toString()))
                .collect(ImmutableCollectors.toList());
    }

    public void write(ContentNode node) {
        String query = "CREATE (node:" + ContentNode.LABEL + " {contentId:{contentId}, data:{data}})";

        session.query(query, ImmutableMap.of(
                "contentId", node.getContentId(),
                "data", node.getData()
        ));
    }
}
