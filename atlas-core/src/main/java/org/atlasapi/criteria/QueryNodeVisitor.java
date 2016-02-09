package org.atlasapi.criteria;

public interface QueryNodeVisitor<V> {

    V visit(QueryNode.IntermediateNode node);

    V visit(QueryNode.TerminalNode node);

}