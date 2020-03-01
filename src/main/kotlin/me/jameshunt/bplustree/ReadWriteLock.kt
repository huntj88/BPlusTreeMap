package me.jameshunt.bplustree

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// TODO: create visualization tool for tree. also show whether nodes are locked or not

/**
 * Behaves as follows
 *
 * while write lock is not locked, multiple read locks from different threads can be acquired.
 * when there are no read locks, a write lock can be acquired
 *
 * if write lock is being acquired and there are existing read locks,
 * the write lock waits for all of the reads that were already added to the queue to finish,
 * and then does the writing operation
 *
 * if read locks are being acquired when the write lock is locked, the reads will take place after the write lock has unlocked
 */

class ReadWriteLock {
    private val numReadPermits = 1000
    private val read: Semaphore = Semaphore(numReadPermits, true)
    private val write: Semaphore = Semaphore(1, true)

    fun <T> withReadLock(block: () -> T): T {
        lockRead()
        return block().also { unlockRead() }
    }

    fun lockRead() {
        synchronized(this) {
            when (isWriteLocked()) {
                true -> {
                    // wait until write operation on node has finished, then acquire read lock
                    write.acquireOrError()
                    read.acquireOrError()
                    write.release()
                }
                false -> {
                    // write not locked
                    read.acquireOrError()
                }
            }
        }
    }

    fun unlockRead() {
        read.release()
    }

    fun lockWrite() {
        // wait for all read permits to be acquired. will mean all pending reads are done
        // acquire write lock, then release all read permits
        synchronized(this) {
            read.acquireOrError(numReadPermits)
            write.acquireOrError()
        }
        read.release(numReadPermits)
    }

    fun unlockWrite() {
        if (isWriteLocked()) {
            write.release()
        } else {
            throw IllegalStateException()
        }
    }

    fun isWriteLocked(): Boolean {
        return write.availablePermits() == 0
    }

    private fun Semaphore.acquireOrError(numPermits: Int = 1) {
        if (!this.tryAcquire(numPermits, 2, TimeUnit.SECONDS)) {
            throw IllegalStateException("Deadlock")
        }
    }
}
