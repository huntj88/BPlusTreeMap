package me.jameshunt.bplustree

import java.util.concurrent.locks.ReentrantLock

class LeafNeighborAccess {
    val access = ReentrantLock(true)

    private var onLeft: LeafNode<*, *>? = null
    private var onRight: LeafNode<*, *>? = null

    fun lockLeftWrite() {
        onLeft?.rwLock?.lockWrite()
    }

    fun lockRightWrite() {
        onRight?.rwLock?.lockWrite()
    }

    fun unlockLeftWrite() {
        access.unlock()
        onLeft?.rwLock?.unlockWrite()
    }

    fun unlockRightWrite() {
        access.unlock()
        onRight?.rwLock?.unlockWrite()
    }

    fun setLeft(left: LeafNode<*, *>) {
        onLeft = left
    }

    fun setRight(right: LeafNode<*, *>) {
        onRight = right
    }
}
