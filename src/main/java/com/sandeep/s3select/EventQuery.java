package com.sandeep.s3select;

public class EventQuery {

  private long fromDate;
  private long toDate;
  private String userName;
  private String type;

  public long getFromDate() {
    return fromDate;
  }

  public void setFromDate(long fromDate) {
    this.fromDate = fromDate;
  }

  public long getToDate() {
    return toDate;
  }

  public void setToDate(long toDate) {
    this.toDate = toDate;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
