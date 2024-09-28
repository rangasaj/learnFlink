package com.example;

import java.time.LocalDateTime;
import java.util.Date;

public class Activity {
    public Integer accountNumber;
    public Date eventDate;
    public LocalDateTime eventDateTime;
    public String activityType;
    public Double amount;

    public Activity(Integer accountNumber, Date eventDate, LocalDateTime eventDateTime, String activityType, Double amount) {
        setFields(accountNumber, eventDate, eventDateTime, activityType, amount);
    }

    private void setFields(Integer accountNumber, Date eventDate, LocalDateTime eventDateTime, String activityType, Double amount) {
        this.accountNumber = accountNumber;
        this.eventDate = eventDate;
        this.eventDateTime = eventDateTime;
        if(activityType ==null ||
                !( activityType.equals("Dr") ||  activityType.equals("Cr"))) {
            throw new IllegalArgumentException("Invalid activity type, got value=" + activityType);
        }
        this.activityType = activityType;
        this.amount = amount;
    }

    public Activity(Integer accountNumber, String eventDate, String eventDateTime, String activityType, Double amount) {

        Date eventDateFormat = new Date();
        LocalDateTime eventDateTimeFormat = LocalDateTime.parse(eventDateTime);

        setFields(accountNumber, eventDateFormat, eventDateTimeFormat, activityType, amount);
    }

}
