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

import static junit.framework.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class FrameFreeSlotBiggestFirstTest {

    static int size = 10;

    FrameFreeSlotBiggestFirst policy;

    @Before
    public void intial() {
        policy = new FrameFreeSlotBiggestFirst(size);
    }

    @Test
    public void testAll() {

        for (int i = 0; i < size; i++) {
            policy.pushNewFrame(i, i);
            assertEquals(i, policy.popBestFit(i));
        }
        assertEquals(-1, policy.popBestFit(0));

        for (int i = 0; i < size; i++) {
            policy.pushNewFrame(i, i);
        }
        for (int i = 0; i < size; i++) {
            assertEquals(size - i - 1, policy.popBestFit(0));
        }

        for (int i = 0; i < size; i++) {
            policy.pushNewFrame(i, i);
        }
        for (int i = 0; i < size / 2; i++) {
            assertEquals(size - i - 1, policy.popBestFit(size / 2));
        }
        assertEquals(-1, policy.popBestFit(size / 2));
        for (int i = 0; i < size / 2; i++) {
            assertEquals(size / 2 - i - 1, policy.popBestFit(0));
        }

    }

    @Test
    public void testReset() {
        testAll();
        policy.reset();
        testAll();
    }

}