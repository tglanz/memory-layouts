package tglanz.memorylayouts.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class DefaultAllocationListener implements AllocationListener {
    private static final Logger logger = LogManager.getLogger(DefaultAllocationListener.class);

    @Override
    public void onPreAllocation(long size) {
        logger.trace("onPreAllocation: {}", size);
    }

    @Override
    public void onAllocation(long size) {
        logger.trace("onAllocation: {}", size);
    }

    @Override
    public void onRelease(long size) {
        logger.trace("onRelease: {}", size);
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
        logger.trace("onFailedAllocation: {}, {}", size, outcome);
        return false;
    }

    @Override
    public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
        logger.trace("onChildAdded");
    }

    @Override
    public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
        logger.trace("onChildRemoved");
    }
}
