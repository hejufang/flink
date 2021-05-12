package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.TaskState;

import java.util.Collection;

public class SavepointV3 implements Savepoint {

	/** The savepoint version. */
	public static final int VERSION = 3;

	@Override
	public int getVersion() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public void dispose() throws Exception {
		throw new UnsupportedOperationException("");

	}

	@Override
	public long getCheckpointId() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Collection<TaskState> getTaskStates() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Collection<MasterState> getMasterStates() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Collection<OperatorState> getOperatorStates() {
		throw new UnsupportedOperationException("");
	}
}
