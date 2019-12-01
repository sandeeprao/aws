package com.sandeep.dynamodb
import com.amazonaws.services.dynamodbv2.document.Item
import com.google.common.collect.Queues
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


class Randomizer(val readQueue:ArrayBlockingQueue<Item>,
                 val randomizedQueue: ArrayBlockingQueue<Item>,
                 val activeProducers: AtomicInteger,
                 val activeRandomizer: AtomicInteger) : Runnable {

    private val LOOP_COUNTER = 30
    override fun run() {
        val randomList = ArrayList<Item>()
        try {
            while (true) {
                if (activeProducers.get() == 0 && readQueue.peek() == null) {
                    return
                }
                randomList.clear()
                var i = 0
                while (i < LOOP_COUNTER) {
                    Queues.drain(readQueue, randomList, 1000, 2, TimeUnit.SECONDS)
                    i++
                }
                randomList.sortWith(compareBy { it.getNumber("timestamp") })
                randomList.forEach { randomizedQueue.put(it) }
            }
        }
        catch (e : Exception){
            throw e
        }
        finally {
            activeRandomizer.decrementAndGet()
        }
    }
}