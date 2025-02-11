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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IFrameBufferManager {

    /**
     * Reset the counters and flags to initial status. This method should not release the pre-allocated resources
     *
     * @throws edu.uci.ics.hyracks.api.exceptions.HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * @param frameIndex
     * @return the specified frame, from the set of memory buffers, being
     * managed by this memory manager
     */
    ByteBuffer getFrame(int frameIndex);

    /**
     * Get the startOffset of the specific frame inside buffer
     *
     * @param frameIndex
     * @return the start offset of the frame returned by {@link #getFrame(int)} method.
     */
    int getFrameStartOffset(int frameIndex);

    /**
     * Get the size of the specific frame inside buffer
     *
     * @param frameIndex
     * @return the length of the specific frame
     */
    int getFrameSize(int frameIndex);

    /**
     * @return the number of frames in this buffer
     */
    int getNumFrames();

    /**
     * Writes the whole frame into the buffer.
     *
     * @param frame source frame
     * @return the id of the inserted frame. if failed to return it will be -1.
     */
    int insertFrame(ByteBuffer frame) throws HyracksDataException;

    void close();

}