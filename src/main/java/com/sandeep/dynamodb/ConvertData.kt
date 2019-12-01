package com.sandeep.dynamodb


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger


class  DynamoBatchOperations {

    var client = AmazonDynamoDBClientBuilder.standard().build()
    var dynamoDB = DynamoDB(client)


    /**
     * This method will create two executor pools one for each read and write based on size and will use producer
     * consumer model to read and write data with array blocking readQueue.
     */
    fun readAndWriteData(ids : List<Pair<String, String>>,
                         readTableName: String,
                         readSize: Int,
                         writeTableName: String,
                         writeSize: Int) {
        val activeProducers = AtomicInteger()
        val activeRandomizer = AtomicInteger()
        val queue = ArrayBlockingQueue<Item>(50000)
        val randomizedQueue = ArrayBlockingQueue<Item>(100000)


        val readExecutorService =  Executors.newFixedThreadPool(20)
        when (readExecutorService) {
            is ThreadPoolExecutor -> {
                readExecutorService.corePoolSize = readSize
                readExecutorService.maximumPoolSize = readSize
            }
        }
        ids.forEach {
            activeProducers.incrementAndGet()
            readExecutorService.execute(ReadProducer(Read(queue, dynamoDB, it, readTableName, activeProducers)))
        }

        val randomExecutorService = Executors.newSingleThreadExecutor()
        activeRandomizer.incrementAndGet()
        randomExecutorService.execute(Randomizer(queue,randomizedQueue,activeProducers,activeRandomizer))

        val writeExecutorService =  Executors.newFixedThreadPool(20)
        when (writeExecutorService) {
            is ThreadPoolExecutor -> {
                writeExecutorService.corePoolSize = writeSize
                writeExecutorService.maximumPoolSize = writeSize
            }
        }
        (1..writeSize).forEach {
            writeExecutorService.execute(
                    WriteConsumer(
                            Write(queue, randomizedQueue, dynamoDB, writeTableName,
                                    activeProducers,
                                    activeRandomizer
                            )
                    )
            )
        }

        shutDown(readExecutorService)
        shutDown(randomExecutorService)
        shutDown(writeExecutorService)

    }


    fun shutDown(executorService: ExecutorService) {
        executorService.shutdown()
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE , TimeUnit.NANOSECONDS)){
                executorService.shutdownNow()
            }
        } catch (e: InterruptedException) {
            executorService.shutdownNow()
        }

    }

}