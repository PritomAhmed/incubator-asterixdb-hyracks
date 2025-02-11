/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.sort.buffermanager;

import edu.uci.ics.hyracks.dataflow.std.structures.IResetableComparable;
import edu.uci.ics.hyracks.dataflow.std.structures.IResetableComparableFactory;
import edu.uci.ics.hyracks.dataflow.std.structures.MaxHeap;

public class FrameFreeSlotBiggestFirst implements IFrameFreeSlotPolicy {
    private static final int INVALID = -1;

    class SpaceEntryFactory implements IResetableComparableFactory {
        @Override
        public IResetableComparable createResetableComparable() {
            return new SpaceEntry();
        }
    }

    class SpaceEntry implements IResetableComparable<SpaceEntry> {
        int space;
        int id;

        SpaceEntry() {
            space = INVALID;
            id = INVALID;
        }

        @Override
        public int compareTo(SpaceEntry o) {
            if (o.space != space) {
                if (o.space == INVALID) {
                    return 1;
                }
                if (space == INVALID) {
                    return -1;
                }
                return space < o.space ? -1 : 1;
            }
            return 0;
        }

        @Override
        public void reset(SpaceEntry other) {
            space = other.space;
            id = other.id;
        }

        void reset(int space, int id) {
            this.space = space;
            this.id = id;
        }
    }

    private MaxHeap heap;
    private SpaceEntry tempEntry;

    public FrameFreeSlotBiggestFirst(int initialCapacity) {
        heap = new MaxHeap(new SpaceEntryFactory(), initialCapacity);
        tempEntry = new SpaceEntry();
    }

    @Override
    public int popBestFit(int tobeInsertedSize) {
        if (!heap.isEmpty()) {
            heap.peekMax(tempEntry);
            if (tempEntry.space >= tobeInsertedSize) {
                heap.getMax(tempEntry);
                return tempEntry.id;
            }
        }
        return -1;
    }

    @Override
    public void pushNewFrame(int frameID, int freeSpace) {
        tempEntry.reset(freeSpace, frameID);
        heap.insert(tempEntry);
    }

    @Override
    public void reset() {
        heap.reset();
    }
}
