package com.sandeep.s3select;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.stream.IntStream;

public class LoggingEvents {


  public static final String JSON_FORMAT = "{ \"id\" : %1$d , \"message\" : \"Test message %1$d\", \"userName\" : \"user %1$d\", \"timeStamp\" : %2$d } \n";

  public static void testLogger() throws IOException {
    try (FileWriter file = new FileWriter("/Users/srao1/Downloads/log/logworker/test.log")) {
      IntStream.range(0,2000).forEach(s -> {
        try {
         file.write(String.format(JSON_FORMAT,s, Instant.now().getNano()));
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  public static void main(String[] args) throws IOException {

    // Local logging
    // LoggingEvents.testLogger();


    //Scanning for logs
    S3Select s3Select = new S3SelectImpl();
    EventQuery eventQuery = new EventQuery();
    eventQuery.setUserName("user 10");
    eventQuery.setFromDate(1564286210000L);
    eventQuery.setToDate(1564458219000L);
    s3Select.getEvents(eventQuery).forEach(System.out::println);
  }
}
