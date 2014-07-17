/*
 * Copyright (c) 2014, Zenoss and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Zenoss or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.zenoss.app.metricservice.buckets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.metricservice.api.impl.IHasShortcut;

import java.util.*;

public class LinearInterpolator implements Interpolator {
    private static final Logger log = LoggerFactory.getLogger(LinearInterpolator.class);
    private List<SeriesInterpolatingAccumulator> accumulators = new ArrayList<>();

    @Override
    public void interpolate(Buckets buckets) {

        /* Make two passes.
         *  First pass: gather the following information:
         *  * list of series in buckets
         *  * for each series: first timestamp, last timestamp, timestamps of missing values, points bracketing missing
         *
         *  Second pass: use data gathered in first pass to calculate and fill in missing values
         */
        for (IHasShortcut key : (Set<IHasShortcut>)buckets.getPrimaryKeys()) {
            accumulators.add(new SeriesInterpolatingAccumulator(buckets, key));
        }

        Map<Long, Buckets.Bucket> bucketList = buckets.getBucketList();
        for (Map.Entry<Long, Buckets.Bucket> bucketEntry : bucketList.entrySet()) {
            Buckets.Bucket bucket = bucketEntry.getValue();
            Long timestamp = bucketEntry.getKey();
            for (SeriesInterpolatingAccumulator accumulator : accumulators) {
                accumulator.accumulate(timestamp, bucket);
            }
        }
    }

    private class SeriesInterpolatingAccumulator {
        private final IHasShortcut key;
        private final Buckets buckets;
        private Long timestampForLastBucketWithValue = null;
        private Buckets.Bucket lastBucketWithValue = null;
        private List<Long> timestampsNeedingInterpolation = new ArrayList<>();

        public SeriesInterpolatingAccumulator(Buckets buckets, IHasShortcut key) {
            this.key = key;
            this.buckets = buckets;
        }

        public Object getKey() {
            return key;
        }

        public void accumulate(Long timestamp, Buckets.Bucket bucket) {
            // if no value:
                // if nothing seen yet, keep going
                // else: add to list of pending values
            // if has value;
                // if pending values are there, use to interpolate
                // update lastSeenValue
            // if no points seen yet and value has a value, store value
            if (!bucket.hasValue(key)) {
                if (null != lastBucketWithValue) {
                    timestampsNeedingInterpolation.add(timestamp);
                }
            } else {
                // Found a value. Interpolate if we can
                if (null != lastBucketWithValue && timestampsNeedingInterpolation.size() > 0) {
                    interpolateValues(timestamp, bucket);
                }
                lastBucketWithValue = bucket;
                timestampForLastBucketWithValue = timestamp;
            }
        }

        private void interpolateValues(Long timestamp, Buckets.Bucket bucket) {
            // if (x0, y0) is first point and (x1, y1) is last, and interpolated point is (x,y)
            // the formula looks like this:
            // y = y0 + ((x-x0) (y1-y0) / (x1 - x0)) , or y = y0 + (x-x0) * deltaY / deltaX
            Long x0 = timestampForLastBucketWithValue;
            Long x1 = timestamp;
            double y0 = lastBucketWithValue.getValue(key).getValue();
            double y1 = bucket.getValue(key).getValue();
            Long deltaX = x1 - x0;
            double deltaY = y1 - y0;

            for (Long x : timestampsNeedingInterpolation) {
                double y = y0 + ((x - x0) * deltaY / deltaX);
                buckets.addInterpolated(key, x, y);
            }
            timestampsNeedingInterpolation.clear();
        }
    }
}