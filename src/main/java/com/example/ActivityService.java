package com.example;

import java.util.ArrayList;
import java.util.List;

public class ActivityService {

    public static List<Activity> getHistoricalActivities() {
        List<Activity> activities = new ArrayList<>();

        activities.add(new Activity(1, "20200101","20200101 10:00:00","Dr",100.0));
        activities.add(new Activity(1, "20200101","20200101 10:00:00","Dr",100.0));
        activities.add(new Activity(1, "20200101","20200101 10:00:00","Dr",100.0));
        activities.add(new Activity(1, "20200101","20200101 10:00:00","Dr",100.0));

        return activities;
    }
}
