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

    /**
     * finalize if you plan for the node to be garbage collected
     */
    private var isFinalized: Boolean = false
    fun finalize() {
        isFinalized = true
    }

    fun <T> withReadLock(block: () -> T): T {
        lockRead()
        return block().also { unlockRead() }
    }

    fun lockRead() {
        read.acquireOrError()
    }

    fun unlockRead() {
        read.release()
    }

    fun lockWrite() {
        // wait for all read permits to be acquired. will mean all pending reads are done
        read.acquireOrError(numReadPermits)
        write.acquireOrError()
    }

    fun unlockWrite() {
        if (isWriteLocked()) {
            read.release(numReadPermits)
            write.release()
        } else {
            throw IllegalStateException()
        }
    }

    fun isWriteLocked(): Boolean {
        return write.availablePermits() == 0
    }

    fun isReadLocked(): Boolean {
        return read.availablePermits() != numReadPermits && !isWriteLocked()
    }

    private fun Semaphore.acquireOrError(numPermits: Int = 1) {
        if (!this.tryAcquire(numPermits, 6, TimeUnit.SECONDS)) {
            synchronized(isFinalized) {
                if (isFinalized) throw IllegalStateException("Trying to access finalized lock")
            }
            val message = """
                Deadlock
                isWriteLocked:           ${isWriteLocked()}
                isReadLocked:            ${isReadLocked()}
                read permits remaining:  ${read.availablePermits()}
            """.trimIndent()

            throw IllegalStateException(message)
        }
    }
}
