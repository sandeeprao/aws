package com.sandeep.s3select;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.JSONType;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;

public class S3SelectImpl implements S3Select {

  private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider("srao")).build();
  private static final Gson gson = new Gson();
  public static final String PREFIX = "logs/year=%d/month=%02d/day=%02d";

  @Override
  public List<Event> getEvents(EventQuery eventQuery) {

    List<String> fileNamePrefix = generateS3BucketPrefix(eventQuery);
    String query = buildQuery(eventQuery.getUserName());
    return fileNamePrefix.stream()
        .map(v -> getFileNames(v))
        .flatMap( list -> list.stream())
        .parallel()
        .map(s ->
        {
          try {
                return scanEventsInFile(s, query);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .flatMap(Function.identity())
    .collect(Collectors.toList());
  }

  protected  Stream<Event> scanEventsInFile(
      final String fileName,
      final String query) throws IOException{

    SelectObjectContentRequest request = generateBaseJsonRequest(BUCKET_NAME, fileName, query);
    try (InputStream result = s3Client.selectObjectContent(request)
        .getPayload()
        .getRecordsInputStream()) {
      List<String> lines = IOUtils.readLines(result, StandardCharsets.UTF_8.toString());
      return lines.stream().filter(s -> !s.isEmpty()).map(s -> gson.fromJson(s, Event.class));
    }
  }

  protected String buildQuery(String userId){
    StringBuilder baseQuery = new StringBuilder("select * from S3Object s where 1=1 ");

    if(userId != null){
      baseQuery.append("and s.userName = '").append(userId).append("' ");
    }
    return  baseQuery.toString();
  }

  protected static SelectObjectContentRequest generateBaseJsonRequest(
      String bucket,
      String key,
      String query) {
    SelectObjectContentRequest request = new SelectObjectContentRequest();
    request.setBucketName(bucket);
    request.setKey(key);
    request.setExpression(query);
    request.setExpressionType(ExpressionType.SQL);

    InputSerialization inputSerialization = new InputSerialization();
    inputSerialization.setJson(new JSONInput().withType(JSONType.LINES));
    inputSerialization.setCompressionType(CompressionType.GZIP);
    request.setInputSerialization(inputSerialization);

    OutputSerialization outputSerialization = new OutputSerialization();
    outputSerialization.setJson(new JSONOutput());
    request.setOutputSerialization(outputSerialization);

    return request;
  }

  protected List<String> getFileNames(String prefix){

    ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(BUCKET_NAME).withPrefix(prefix);
    ListObjectsV2Result result;
    List<String> value = new ArrayList<>();
    do {
      result = s3Client.listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        value.add(objectSummary.getKey());
      }
       String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());
    return value;
  }


  protected List<String> generateS3BucketPrefix(EventQuery eventQuery){

    LocalDate fromDate =  Instant.ofEpochMilli(eventQuery.getFromDate())
        .atZone(ZoneId.systemDefault())
        .toLocalDate();
    LocalDate toDate =  Instant.ofEpochMilli(eventQuery.getToDate())
        .atZone(ZoneId.systemDefault())
        .toLocalDate();
    long numOfDaysBetween = ChronoUnit.DAYS.between(fromDate, toDate);

    return IntStream.iterate(0, i -> i + 1)
        .limit(numOfDaysBetween)
        .mapToObj(i -> fromDate.plusDays(i))
        .map(d -> String.format(PREFIX,d.getYear(),d.getMonthValue(),d.getDayOfMonth()))
        .collect(Collectors.toList());
  }
}
