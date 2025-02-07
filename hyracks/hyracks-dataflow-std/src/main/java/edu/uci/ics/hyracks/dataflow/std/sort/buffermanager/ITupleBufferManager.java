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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public interface ITupleBufferManager {
    /**
     * Reset the counters and flags to initial status. This method should not release the pre-allocated resources
     *
     * @throws edu.uci.ics.hyracks.api.exceptions.HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * @return the number of tuples in this buffer
     */
    int getNumTuples();

    boolean insertTuple(IFrameTupleAccessor accessor, int idx, TuplePointer tuplePointer) throws HyracksDataException;

    void deleteTuple(TuplePointer tuplePointer) throws HyracksDataException;

    void close();

    ITupleBufferAccessor getTupleAccessor();
}
