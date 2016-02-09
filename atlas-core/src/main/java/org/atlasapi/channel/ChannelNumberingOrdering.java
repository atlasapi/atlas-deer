package org.atlasapi.channel;

import com.google.common.collect.Ordering;

/**
 * @author Oliver Hall
 *         <p>
 *         Ordering for Channel Numbering Objects in the simple model. This is required due to
 *         channel numbers being represented as Strings, and orders non-zero prefixed numerical
 *         strings first (in numerical order), followed by zero prefixed numerical strings (in
 *         numerical order), followed by Numberings with a null channel number. This is needed for
 *         Platforms such as Sky, which includes both channel numbers '101' and '0101'.
 */
public class ChannelNumberingOrdering extends Ordering<ChannelNumbering> {

    @Override
    public int compare(ChannelNumbering left, ChannelNumbering right) {
        String leftNumber = left.getChannelNumber().orElse(null);
        String rightNumber = right.getChannelNumber().orElse(null);

        if (leftNumber != null) {
            if (rightNumber != null) {
                return compareChannelNumbers(leftNumber, rightNumber);
            }
            return -1;
        }
        if (rightNumber != null) {
            return 1;
        }
        return 0;
    }

    private int compareChannelNumbers(String left, String right) {
        if (left.startsWith("0")) {
            if (right.startsWith("0")) {
                return Double.compare(Integer.parseInt(left), Integer.parseInt(right));
            }
            return 1;
        }
        if (right.startsWith("0")) {
            return -1;
        }
        return Double.compare(Integer.parseInt(left), Integer.parseInt(right));
    }

}