package me.jameshunt.bplustree

import java.util.concurrent.Semaphore

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
                write.acquire()
                read.acquire()
                write.release()
                block().also {
                    read.release()
                }
            }
            false -> {
                read.acquire()
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
                write.acquire()
                read.acquire()
                write.release()
            }
            false -> {
                // write not locked
                read.acquire()
            }
        }
    }

    fun unlockRead() {
        read.release()
    }

    fun lockWrite() {
        // wait for all read permits to be acquired. will mean all pending reads are done
        // acquire write lock, then release all read permits
        read.acquire(numReadPermits)
        write.acquire()
        read.release(numReadPermits)
    }

    fun unlockWrite() {
        if(write.availablePermits() == 0) {
            write.release()
        } else {
            throw IllegalStateException()
        }
    }
}
