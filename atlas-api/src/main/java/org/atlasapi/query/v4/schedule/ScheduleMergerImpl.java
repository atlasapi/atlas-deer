package org.atlasapi.query.v4.schedule;

import java.util.List;
import java.util.ListIterator;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.schedule.ChannelSchedule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkArgument;

class ScheduleMergerImpl implements ScheduleMerger {

    private final Ordering<ItemAndBroadcast> sort = Ordering.natural();
    private final Ordering<DateTime> dateTimeOrdering = Ordering.natural();

    @Override
    public ChannelSchedule merge(ChannelSchedule originalSchedule, ChannelSchedule overrideSchedule) {
        checkArgument(
                originalSchedule.getChannel().equals(overrideSchedule.getChannel()),
                String.format(
                        "Override should be for the same channel, got %s %s",
                        originalSchedule.getChannel().getId(),
                        overrideSchedule.getChannel().getId()
                )
        );

        List<ItemAndBroadcast> originalItems =
                sort.sortedCopy(originalSchedule.getEntries());
        List<ItemAndBroadcast> overrideItems =
                sort.sortedCopy(overrideSchedule.getEntries());

        ImmutableList.Builder<ItemAndBroadcast> mergedBuilder = ImmutableList.builder();

        ListIterator<ItemAndBroadcast> itOriginal = originalItems.listIterator();
        ListIterator<ItemAndBroadcast> itOverride = overrideItems.listIterator();

        if (!itOverride.hasNext()) {
            mergedBuilder.addAll(itOriginal);
            return new ChannelSchedule(
                    originalSchedule.getChannel(),
                    new Interval(
                            originalSchedule.getInterval().getStart(),
                            originalSchedule.getInterval().getEnd()
                    ),
                    mergedBuilder.build()
            );
        }

        ItemAndBroadcast curr;
        ItemAndBroadcast override = null;

        boolean removeFollowOn = false;
        boolean truncateFollowOnToOverride = false;

        while (itOriginal.hasNext()) {
            curr = itOriginal.next();
            Broadcast currBroadcast = curr.getBroadcast();

            /* duration == 0 means it's a follow-on. They need to be appended to the end of the
               previous broadcast, so just slap them on.
             */
            if (currBroadcast.getTransmissionInterval().toDuration().getMillis() == 0) {
                if (removeFollowOn) {
                    continue;
                }

                if (truncateFollowOnToOverride) {
                    mergedBuilder.add(new ItemAndBroadcast(
                            curr.getItem().copy(),
                            new Broadcast(
                                    currBroadcast.getChannelId(),
                                    override.getBroadcast().getTransmissionTime(),
                                    override.getBroadcast().getTransmissionTime()
                            )
                    ));
                } else {
                    mergedBuilder.add(copy(curr));
                }

                if (!itOriginal.hasNext() && override != null) {
                    mergedBuilder.add(override);
                }
                removeFollowOn = false;
                truncateFollowOnToOverride = false;
                continue;
            }

            removeFollowOn = false;
            truncateFollowOnToOverride = false;

            if (override == null) {
                override = itOverride.next();
            }

            Broadcast overrideBroadcast = override.getBroadcast();

            /* currStart >= overrideEnd, disjoint strictly after
               advance overrideSchedule iterator and try again
             */
            if (currBroadcast.getTransmissionTime()
                    .compareTo(overrideBroadcast.getTransmissionEndTime()) >= 0) {

                mergedBuilder.add(copy(override));

                if (itOverride.hasNext()) {
                    override = itOverride.next();
                    itOriginal.previous();
                    continue;
                } else {
                    mergedBuilder.add(copy(curr));
                    break;
                }
            }

            /* currEnd <= overrideStart, disjoint strictly before
               advance originalSchedule iterator and try again
             */
            if (currBroadcast.getTransmissionEndTime()
                    .compareTo(overrideBroadcast.getTransmissionTime()) <= 0) {
                mergedBuilder.add(copy(curr));
                continue;
            }

            /* currStart < overrideStart

               Two cases in theory, either the end is within overrideSchedule or past it. We handle both
               the same, by only leaving the prefix, because splitting a single item into two
               broadcasts on both sides of the overrideSchedule would be confusing.
             */
            if (currBroadcast.getTransmissionTime()
                    .isBefore(overrideBroadcast.getTransmissionTime())) {
                ItemAndBroadcast prefixItem = new ItemAndBroadcast(
                        curr.getItem(),
                        new Broadcast(
                                currBroadcast.getChannelId(),
                                currBroadcast.getTransmissionTime(),
                                overrideBroadcast.getTransmissionTime()
                        )
                );
                mergedBuilder.add(prefixItem);

                truncateFollowOnToOverride = true;

                /* normally we'd wait for more items that might be contained within or
                   hidden by this overrideSchedule. However, since the originalSchedule schedule
                   is exhausted the loop will terminate and the overrideSchedule itself will be
                   lost if we don't add it here
                  */
                if (!itOriginal.hasNext()) {
                    mergedBuilder.add(copy(override));
                }
            } else {
                /* currStart >= overrideStart and currEnd > overrideEnd
                   means we need to add the overrideSchedule first and then add the truncated suffix
                   of the originalSchedule item
                 */
                if (currBroadcast.getTransmissionEndTime()
                        .isAfter(overrideBroadcast.getTransmissionEndTime())) {
                    mergedBuilder.add(copy(override));

                    ItemAndBroadcast suffixItem = new ItemAndBroadcast(
                            curr.getItem(),
                            new Broadcast(
                                    currBroadcast.getChannelId(),
                                    overrideBroadcast.getTransmissionEndTime(),
                                    currBroadcast.getTransmissionEndTime()
                            )
                    );

                    if (itOverride.hasNext()) {
                        override = itOverride.next();
                        /* handles the case where

                           override  -----  -----
                           original     ------

                           by truncating the original item's start date and adding it back to be
                           re-processed as a prefix item in the next iteration
                         */
                        itOriginal.set(suffixItem);
                        itOriginal.previous();
                    } else {
                        mergedBuilder.add(suffixItem);
                        break;
                    }
                } else if (!itOriginal.hasNext()) {
                    mergedBuilder.add(copy(override));
                } else {
                    // the item is fully within the overrideSchedule, so it gets nuked
                    removeFollowOn = true;
                }
            }
        }

        // only 1 of these 2 while's will ever be executed, adds whatever tailing items are left
        while (itOriginal.hasNext()) {
            mergedBuilder.add(copy(itOriginal.next()));
        }

        while (itOverride.hasNext()) {
            mergedBuilder.add(copy(itOverride.next()));
        }

        return new ChannelSchedule(
                originalSchedule.getChannel(),
                new Interval(
                        dateTimeOrdering.min(
                                originalSchedule.getInterval().getStart(),
                                overrideSchedule.getInterval().getStart()
                        ),
                        dateTimeOrdering.max(
                                originalSchedule.getInterval().getEnd(),
                                overrideSchedule.getInterval().getEnd()
                        )
                ),
                mergedBuilder.build()
        );
    }

    private ItemAndBroadcast copy(ItemAndBroadcast iab) {
        return new ItemAndBroadcast(iab.getItem().copy(), iab.getBroadcast().copy());
    }
}
