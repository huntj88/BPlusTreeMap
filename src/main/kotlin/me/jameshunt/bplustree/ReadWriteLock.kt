package me.jameshunt.bplustree

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// TODO: create visualization tool for tree. also show whether nodes are locked or not

/**
 *
 */

class ReadWriteLock {
    private val numReadPermits = 1000
    private val read: Semaphore = Semaphore(numReadPermits, true)
    private val write: Semaphore = Semaphore(1, true)

    fun <T> withReadLock(block: () -> T): T {
        return when (write.availablePermits() == 0) {
            true -> {
                // wait until write operation on node has finished, then acquire read lock
                write.acquireOrError()
                read.acquireOrError()
                write.release()
                block().also {
                    read.release()
                }
            }
            false -> {
                read.acquireOrError()
                block().also {
                    read.release()
                }
            }
        }
    }

    fun lockRead() {
        when (write.availablePermits() == 0) {
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

    fun unlockRead() {
        read.release()
    }

    fun lockWrite() {
        // wait for all read permits to be acquired. will mean all pending reads are done
        // acquire write lock, then release all read permits
        read.acquireOrError(numReadPermits)
        write.acquireOrError()
        read.release(numReadPermits)
    }

    fun unlockWrite() {
        if (write.availablePermits() == 0) {
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
