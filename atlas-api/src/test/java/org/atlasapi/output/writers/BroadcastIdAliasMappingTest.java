package org.atlasapi.output.writers;

import java.util.Arrays;

import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;

import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BroadcastIdAliasMappingTest {

    @Parameters(name = "{index}: mapping {0} = {1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "pa:63847018", new Alias("pa:slot", "63847018") },
                { "bbc:p00tlngb", new Alias("bbc:pid", "p00tlngb") },
                {
                        "bbc:b00fh2nl.imi:bds.tv/123507757",
                        new Alias("bbc:bds", "b00fh2nl.imi:bds.tv/123507757") },
                {
                        "bbc:p003fmx1.imi:infax.bbc.co.uk/tx_seq_num:2906045:4287",
                        new Alias(
                                "bbc:infax",
                                "p003fmx1.imi:infax.bbc.co.uk/tx_seq_num:2906045:4287"
                        ) },
                { "youview:9877042", new Alias("youview:slot", "9877042") },
                { "BroadcastITV2201302121505", new Alias("itv:slot", "BroadcastITV2201302121505") },
                { "BroadcastCITV201108140615", new Alias("itv:slot", "BroadcastCITV201108140615") },
                {
                        "Broadcastb1abce42-0299-4227-b0a9-38d9d37dcb3f",
                        new Alias("itv:slot", "Broadcastb1abce42-0299-4227-b0a9-38d9d37dcb3f") },
                { "c4:28405125", new Alias("c4:c4:slot", "28405125") },
                { "f4:28004063", new Alias("c4:f4:slot", "28004063") },
                { "4m:27849151", new Alias("c4:4m:slot", "27849151") },
                { "e4:27917775", new Alias("c4:e4:slot", "27917775") },
                { "m4:27916151", new Alias("c4:m4:slot", "27916151") },
                { "4s:28482091", new Alias("c4:4s:slot", "28482091") },
                { "1781166741", new Alias("rovicorp:slot", "1781166741") }
        });
    }

    @Parameter
    public String input;

    @Parameter(value = 1)
    public Alias expected;

    private final BroadcastIdAliasMapping mapping = new BroadcastIdAliasMapping();

    @Test
    public void testMapping() {
        Broadcast broadcast = new Broadcast(Id.valueOf(1L), new Interval(0, 0)).withId(input);
        assertEquals(expected, mapping.apply(broadcast));
    }

}
