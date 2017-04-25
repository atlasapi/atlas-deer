package org.atlasapi.query.common.attributes;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class PrefixInTree<T> extends ConcurrentRadixTree<Optional<T>> {

    protected PrefixInTree() {
        super(new DefaultCharArrayNodeFactory());
    }

    public static <T> PrefixInTree<T> create() {
        return new PrefixInTree<>();
    }

    public Optional<T> valueForKeyPrefixOf(String superString) {
        checkNotNull(superString, "can't lookup null key");
        if (superString.isEmpty()) {
            return Optional.absent();
        }

        int matchLen = 0;
        int nodeMatchLen = 0;
        Node prev = null;
        Node node = root;

        while (matchLen < superString.length() && node != null) {

            prev = node;
            nodeMatchLen = 0;
            CharSequence edgeChars = node.getIncomingEdge();
            while (nodeMatchLen < edgeChars.length() && matchLen < superString.length()) {
                if (edgeChars.charAt(nodeMatchLen) != superString.charAt(matchLen)) {
                    return processResult(superString, node, matchLen, nodeMatchLen);
                }
                matchLen++;
                nodeMatchLen++;
            }

            node = matchLen < superString.length()
                   ? node.getOutgoingEdge(superString.charAt(matchLen))
                   : null;
        }

        return processResult(superString, prev, matchLen, nodeMatchLen);
    }

    @SuppressWarnings("unchecked")
    private Optional<T> processResult(
            String key,
            Node curNode,
            int matchLen,
            int curNodeMatch
    ) {
        int incomingEdgeLength = curNode.getIncomingEdge().length();
        Optional<T> value = Optional.absent();
        if (matchLen > 0 && matchLen <= key.length()
                && curNodeMatch == incomingEdgeLength && leafNode(curNode)) {
            value = (Optional<T>) curNode.getValue();
        }
        return value;
    }

    private boolean leafNode(Node curNode) {
        return curNode.getValue() != null;
    }

    public Set<String> allKeys() {
        ImmutableSet.Builder<String> keys = ImmutableSet.builder();

        traverseDescendants(
                "",
                root,
                nodeKeyPair -> {
                    if (leafNode(nodeKeyPair.node)) {
                        keys.add(nodeKeyPair.key.toString());
                    }
                    return true;
                }
        );

        return keys.build();
    }

    public PrefixInTree<T> put(String key, T value) {
        put(key, Optional.fromNullable(value));
        return this;
    }
}
