package org.atlasapi.query.common;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.atlasapi.criteria.attribute.Attributes;
import org.junit.Test;


public class AttributeLookupTreeTest {

    @Test
    public void testAttributeLookup() {
        
        AttributeLookupTree tree = new AttributeLookupTree();
        
        tree.put(Attributes.ALIASES_NAMESPACE);
        
        assertFalse(tree.attributeFor("").isPresent());
        assertFalse(tree.attributeFor("w").isPresent());
        assertFalse(tree.attributeFor("aliases").isPresent());
        assertFalse(tree.attributeFor("aliases.value").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace.beginning").isPresent());
        assertTrue(tree.attributeFor("aliases.namespace.equals").isPresent());
        
        tree.put(Attributes.ALIASES_VALUE);
        
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
        AttributeLookupTree tree = new AttributeLookupTree();
        
        tree.put(Attributes.TAG_RELATIONSHIP);
        tree.put(Attributes.TAG_SUPERVISED);
        
        assertNotNull(tree.attributeFor(Attributes.TOPIC_ID.externalName()));
        
    }

    @Test
    public void testGetAllKeys() {

        AttributeLookupTree tree = new AttributeLookupTree();
        
        tree.put(Attributes.ID);
        tree.put(Attributes.ALIASES_NAMESPACE);
        tree.put(Attributes.ALIASES_VALUE);
        
        Set<String> keys = tree.allKeys();
        assertThat(keys.size(), is(3));
        assertThat(keys, hasItems("id", "aliases.namespace", "aliases.value"));
    }
    
}
