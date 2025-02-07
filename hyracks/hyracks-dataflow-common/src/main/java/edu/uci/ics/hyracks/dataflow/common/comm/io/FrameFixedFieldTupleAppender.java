package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameAppender;
import edu.uci.ics.hyracks.api.comm.IFrameFieldAppender;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAppender;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * This appender can appendTuple and appendField but at the expense of additional checks.
 * Please use this Field/Tuple mixed appender only if you don't know the sequence of append functions.
 * Try using {@link FrameFixedFieldAppender} if you only want to appendFields.
 * and using {@link FrameTupleAppender} if you only want to appendTuples.
 */
public class FrameFixedFieldTupleAppender implements IFrameTupleAppender, IFrameFieldAppender {

    private FrameFixedFieldAppender fieldAppender;
    private FrameTupleAppender tupleAppender;
    private IFrame sharedFrame;
    private IFrameAppender lastAppender;

    public FrameFixedFieldTupleAppender(int numFields) {
        tupleAppender = new FrameTupleAppender();
        fieldAppender = new FrameFixedFieldAppender(numFields);
        lastAppender = tupleAppender;
    }

    private void resetAppenderIfNecessary(IFrameAppender appender) throws HyracksDataException {
        if (lastAppender != appender) {
            if (lastAppender == fieldAppender) {
                if (fieldAppender.hasLeftOverFields()) {
                    throw new HyracksDataException("The previous appended fields haven't been flushed yet.");
                }
            }
            appender.reset(sharedFrame, false);
            lastAppender = appender;
        }
    }

    @Override
    public boolean appendField(byte[] bytes, int offset, int length) throws HyracksDataException {
        resetAppenderIfNecessary(fieldAppender);
        return fieldAppender.appendField(bytes, offset, length);
    }

    @Override
    public boolean appendField(IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        resetAppenderIfNecessary(fieldAppender);
        return fieldAppender.appendField(accessor, tid, fid);
    }

    @Override
    public boolean append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(tupleAccessor, tIndex);
    }

    @Override
    public boolean append(int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(fieldSlots, bytes, offset, length);
    }

    @Override
    public boolean append(byte[] bytes, int offset, int length) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(bytes, offset, length);
    }

    @Override
    public boolean appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length);
    }

    @Override
    public boolean append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(tupleAccessor, tStartOffset, tEndOffset);
    }

    @Override
    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1);
    }

    @Override
    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1,
            int offset1, int dataLen1) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1);
    }

    @Override
    public boolean appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendProjection(accessor, tIndex, fields);
    }

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        sharedFrame = frame;
        tupleAppender.reset(sharedFrame, clear);
        fieldAppender.reset(sharedFrame, clear);
    }

    @Override
    public int getTupleCount() {
        return lastAppender.getTupleCount();
    }

    @Override
    public ByteBuffer getBuffer() {
        return lastAppender.getBuffer();
    }

    @Override
    public void flush(IFrameWriter outWriter, boolean clear) throws HyracksDataException {
        lastAppender.flush(outWriter, clear);
    }
}
