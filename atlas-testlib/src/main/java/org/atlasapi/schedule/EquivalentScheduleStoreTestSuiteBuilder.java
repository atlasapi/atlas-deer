package org.atlasapi.schedule;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.AbstractTester;
import com.google.common.collect.testing.FeatureSpecificTestSuiteBuilder;

public class EquivalentScheduleStoreTestSuiteBuilder
        extends
        FeatureSpecificTestSuiteBuilder<EquivalentScheduleStoreTestSuiteBuilder, EquivalentScheduleStoreSubjectGenerator> {

    @Override
    @SuppressWarnings("rawtypes")
    protected List<Class<? extends AbstractTester>> getTesters() {
        return ImmutableList.<Class<? extends AbstractTester>>of(
                EquivalentScheduleStoreTester.class
        );
    }

    public static EquivalentScheduleStoreTestSuiteBuilder using(
            EquivalentScheduleStoreSubjectGenerator generator) {
        return new EquivalentScheduleStoreTestSuiteBuilder().usingGenerator(generator);
    }

}
