package com.sandeep.dynamodb
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.TableWriteItems
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import com.google.common.collect.Queues
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


data class Write(val readQueue: ArrayBlockingQueue<Item>,
                 val randomizedQueue: ArrayBlockingQueue<Item>,
                 val dynamoDB: DynamoDB,
                 val tableName: String,
                 val activeProducers: AtomicInteger,
                 val activeRandomizer: AtomicInteger)

/**
 * This class drains items of batches in 25 and writes the data
 */

class WriteConsumer(val write: Write) : Runnable {

    override fun run() {
        val items = ArrayList<Item>()
        var isDone = true
        try {
            while (true ) {
                if (write.activeProducers.get() == 0 && write.activeRandomizer.get() == 0 &&
                    write.randomizedQueue.peek() == null && write.readQueue.peek() == null &&
                    items.isEmpty()) {
                    return
                }
                // if previous write is not successful try it again without draining the readQueue.
                    if(isDone) {
                        items.clear()
                        Queues.drain(write.randomizedQueue, items, 25, 30, TimeUnit.SECONDS)
                    }
                    if(items.isNotEmpty()) {
                        isDone = writeData(items)
                    }
            }
        } catch (e: InterruptedException) {
            items.map { write.randomizedQueue.put(it) }
            Thread.currentThread().interrupt()
        }
    }


    /**
     * Writes the data and returns true if successful or false if it fails to write the data.
     */
    private fun writeData(items: List<Item>) : Boolean {
        TimeUnit.SECONDS.sleep(1)
        val tableWriteItems = TableWriteItems(write.tableName).withItemsToPut(items)
        var outcome: BatchWriteItemOutcome? = null
        do {
            try {
                outcome = write.dynamoDB.batchWriteItem(tableWriteItems)
                if (outcome.unprocessedItems.isNotEmpty()) {
                    TimeUnit.SECONDS.sleep(2)
                    outcome = write.dynamoDB.batchWriteItemUnprocessed(outcome.unprocessedItems)
                }
            } catch (ex: ProvisionedThroughputExceededException) {
                Thread.sleep(30000)
            }
            catch (ex: Exception){
                return false
            }

        } while (outcome == null || outcome.unprocessedItems.isNotEmpty())

        return true
    }

}