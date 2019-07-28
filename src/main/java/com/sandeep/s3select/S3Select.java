package com.sandeep.s3select;

import java.util.List;

public interface S3Select {

  String BUCKET_NAME = "s3select-log";
  List<Event> getEvents(EventQuery eventQuery);
}
