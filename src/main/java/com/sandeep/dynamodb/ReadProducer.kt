package com.sandeep.dynamodb
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger


data class Read(val queue: ArrayBlockingQueue<Item>,
                val dynamoDB: DynamoDB,
                val id : Pair<String, String>,
                val tableName : String,
                val activeProducers: AtomicInteger)

class ReadProducer(val read: Read) : Runnable {

    override fun run() {
        var isDone = false
        while (isDone.not()) {
            isDone = readData()
        }
        read.activeProducers.decrementAndGet()
    }

    /**
     * Reads the data using query spec and return true when done
     */
    fun readData(): Boolean{

        val table = read.dynamoDB.getTable(read.tableName)

        val spec = getQuerySpec(read.id.first)
        try {
            val items = table.query(spec)
            val pages = items.pages()
            for (page in pages.iterator()) {
                // Process each item on the current page
                val item = page.iterator()
                while (item.hasNext()) {
                    read.queue.put(item.next().withPrimaryKey("id",read.id.second))
                }
            }
        }
        catch (e1: ProvisionedThroughputExceededException) {
            Thread.sleep(6000)
            return false
        }
        catch (ex: Exception){
            return  false
        }
        return true
    }


    private fun getQuerySpec(id: String) : QuerySpec {

        val spec = QuerySpec().withKeyConditionExpression("id = :id")
                .withScanIndexForward(false)
                .withMaxPageSize(1000)
                .withValueMap(ValueMap()
                        .withString(":id", id))
        return  spec
    }
}