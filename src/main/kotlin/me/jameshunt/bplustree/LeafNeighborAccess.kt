package me.jameshunt.bplustree

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class LeafNeighborAccess {
    val access = ReentrantLock(true)

    private var onLeft: LeafNode<*, *>? = null
    private var onRight: LeafNode<*, *>? = null

    fun lockLeftWrite() {
        log("locking left $onLeft")
        onLeft?.rwLock?.lockWrite()
        log("LOCKED left $onLeft")
    }

    fun lockRightWrite() {
        log("locking right $onRight")
        onRight?.rwLock?.lockWrite()
        log("LOCKED right $onRight")
    }

    fun unlockLeftWrite() {
        log("unlocking left $onLeft")
        onLeft?.rwLock?.unlockWrite()
    }

    fun unlockRightWrite() {
        log("unlocking right $onRight")
        onRight?.rwLock?.unlockWrite()
    }

    fun setLeft(left: LeafNode<*, *>) {
        onLeft = left
    }

    fun setRight(right: LeafNode<*, *>) {
        onRight = right
    }

    fun getRight(): LeafNode<*, *>? {
        return onRight
    }

    fun lock() {
        access.lockOrFail()
    }

    private fun ReentrantLock.lockOrFail() {
        if (!this.tryLock(2, TimeUnit.SECONDS)) {
            val message = """
                Deadlock
                isLocked:       ${this.isLocked}
                leaf link:      ${this@LeafNeighborAccess}
                left:           $onLeft
                right:          $onRight
            """.trimIndent()

            throw IllegalStateException(message)
        }
    }
}
