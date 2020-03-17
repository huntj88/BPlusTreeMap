package me.jameshunt.bplustree

import java.util.concurrent.locks.ReentrantLock

class LeafNeighborAccess {
    val access = ReentrantLock(true)

    private var onLeft: LeafNode<*, *>? = null
    private var onRight: LeafNode<*, *>? = null

    fun lockLeftWrite() {
//        println("locking left $this, with thread: ${Thread.currentThread()}")
        onLeft?.rwLock?.lockWrite()
//        println("LOCKED left $this, with thread: ${Thread.currentThread()}")
    }

    fun lockRightWrite() {
//        println("locking right $this, with thread: ${Thread.currentThread()}")
        onRight?.rwLock?.lockWrite()
//        println("LOCKED right $this, with thread: ${Thread.currentThread()}")
    }

    fun unlockLeftWrite() {
//        println("unlocking left $this, with thread: ${Thread.currentThread()}")
        onLeft?.rwLock?.unlockWrite()
    }

    fun unlockRightWrite() {
//        println("unlocking right $this, with thread: ${Thread.currentThread()}")
        onRight?.rwLock?.unlockWrite()
    }

    fun setLeft(left: LeafNode<*, *>) {
        onLeft = left
    }

    fun setRight(right: LeafNode<*, *>) {
        onRight = right
    }
}
