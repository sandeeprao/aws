package com.sandeep.s3select;

public class Event {

  public int id;
  public String message;
  public String userName;
  public long timeStamp;

  public Event(int id, String message, String userName, long timeStamp) {
    this.id = id;
    this.message = message;
    this.userName = userName;
    this.timeStamp = timeStamp;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  @Override
  public String toString() {
    return "Event{" +
        "id='" + id + '\'' +
        ", message='" + message + '\'' +
        ", userName='" + userName + '\'' +
        ", timeStamp=" + timeStamp +
        '}';
  }
}
