package org.atlasapi.content;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.AbstractTester;
import com.google.common.collect.testing.FeatureSpecificTestSuiteBuilder;

public class EquivalentContentStoreTestSuiteBuilder
        extends
        FeatureSpecificTestSuiteBuilder<EquivalentContentStoreTestSuiteBuilder, EquivalentContentStoreSubjectGenerator> {

    @Override
    @SuppressWarnings("rawtypes")
    protected List<Class<? extends AbstractTester>> getTesters() {
        return ImmutableList.<Class<? extends AbstractTester>>of(
                EquivalentContentStoreTester.class
        );
    }

    public static EquivalentContentStoreTestSuiteBuilder using(
            EquivalentContentStoreSubjectGenerator generator) {
        return new EquivalentContentStoreTestSuiteBuilder().usingGenerator(generator);
    }

}
