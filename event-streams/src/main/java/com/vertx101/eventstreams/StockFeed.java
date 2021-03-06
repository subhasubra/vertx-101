package com.vertx101.eventstreams;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockFeed {
  private String name;
  private double value;
  private double prevValue;
  private String updatedOn;

  // Replace with lombok
  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  public double getPrevValue() {
    return prevValue;
  }

  public void setPrevValue(double prevValue) {
    this.prevValue = prevValue;
  }

  public String getUpdatedOn() {
    return updatedOn;
  }

  public void setUpdatedOn(String updatedOn) {
    this.updatedOn = updatedOn;
  }

  public void setValues(String name, double value) {
    this.prevValue = this.value;
    this.name = name;
    this.value = value;
    this.updatedOn = DateTimeFormatter.ofPattern("yyyy-mm-dd HH:mm:ss").format(LocalDateTime.now());
  }
}
