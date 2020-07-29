package org.atlasapi.query.common;

import java.util.Set;

import org.atlasapi.criteria.attribute.ContentAttributes;
import org.atlasapi.query.common.attributes.AttributeLookupTree;

import org.junit.Test;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AttributeLookupTreeTest {

    @Test
    public void testAttributeLookup() {
        AttributeLookupTree tree = AttributeLookupTree.create();

        tree.put(ContentAttributes.ALIASES_NAMESPACE);

        assertFalse(tree.attributeFor("").isPresent());
        assertFalse(tree.attributeFor("w").isPresent());
        assertFalse(tree.attributeFor("aliases").isPresent());
        assertFalse(tree.attributeFor("aliases.value").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace.beginning").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace.equals").isPresent());

        tree.put(ContentAttributes.ALIASES_VALUE);

        assertFalse(tree.attributeFor("").isPresent());
        assertFalse(tree.attributeFor("w").isPresent());
        assertFalse(tree.attributeFor("aliases").isPresent());
        assertFalse(tree.attributeFor("aliases.valu").isPresent());
        assertFalse(tree.attributeFor("waliases.value").isPresent());
        assertTrue(tree.attributeFor("aliases.value").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace.beginning").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace.equals").isPresent());

    }

    @Test
    public void testDoesntProduceNullWhenBestMatchIsNonLeafNode() {
        AttributeLookupTree tree = AttributeLookupTree.create();

        tree.put(ContentAttributes.TAG_RELATIONSHIP);
        tree.put(ContentAttributes.TAG_SUPERVISED);

        assertNotNull(tree.attributeFor(ContentAttributes.TOPIC_ID.externalName()));

    }

    @Test
    public void testGetAllKeys() {
        AttributeLookupTree tree = AttributeLookupTree.create();

        tree.put(ContentAttributes.ID);
        tree.put(ContentAttributes.ALIASES_NAMESPACE);
        tree.put(ContentAttributes.ALIASES_VALUE);

        Set<String> keys = tree.allKeys();
        assertThat(keys.size(), is(3));
        assertThat(keys, hasItems("id", "aliases.namespace", "aliases.value"));
    }

    @Test
    public void getAllKeysWhenOneKeyIsPrefixOfAnother() throws Exception {
        AttributeLookupTree tree = AttributeLookupTree.create();

        tree.put(ContentAttributes.CONTENT_TITLE_PREFIX);
        tree.put(ContentAttributes.TITLE_BOOST);

        Set<String> keys = tree.allKeys();
        assertThat(keys.size(), is(2));
        assertThat(
                keys.contains(ContentAttributes.CONTENT_TITLE_PREFIX.externalName()),
                is(true)
        );
        assertThat(
                keys.contains(ContentAttributes.TITLE_BOOST.externalName()),
                is(true)
        );
    }
}
