package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;

public class CabAssignmentUdemy {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> cabData = env.readTextFile("/Users/jagar/projects/mine/gitHub/learnFlink/src/main/java/com/example/cab_flink.txt");

     /*
    Using Datastream/Dataset transformations find the following for each ongoing trip.
    1.) Popular destination.  | Where more number of people reach.
    2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
    3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made
     //cab id, cab number plate, cab type (2), cab driver name(3), ongoing trip/not, pickup location(5), destination,passenger count(7)
 */
        DataSet<Tuple9<String, String, String, String, String, String, String, Integer,Integer>> cabData1 = cabData.map(new Splitter());
        // Popular destination
        ReduceOperator<Tuple9<String, String, String, String, String, String, String, Integer, Integer>> a = cabData1.groupBy(6).sum(7).maxBy(7);
        a.print();

        DataSet<Tuple2<String,Double>> avgPassenger = cabData1.map(new MapFunction<Tuple9<String, String, String, String, String, String, String, Integer, Integer>, Tuple3<String,Integer,Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Tuple9<String, String, String, String, String, String, String, Integer, Integer> data) throws Exception {
                //pickupLocation, passengerCount, tripCount
                return new Tuple3<>(data.f5,data.f7,data.f8);
            }
        }).groupBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> aggregate, Tuple3<String, Integer, Integer> t1) throws Exception {
                        return new Tuple3<>(aggregate.f0,t1.f1+aggregate.f1,t1.f2+aggregate.f2  );
                    }
                })
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> data) throws Exception {
                        return new Tuple2<>(data.f0,(data.f1*1.0)/data.f2);
                    }
                });
        avgPassenger.sortPartition(1, Order.ASCENDING);
        avgPassenger.print();
    }

    public static  class Splitter implements MapFunction<String, Tuple9<String, String, String, String, String, String, String, Integer,Integer>> {
        @Override
        public Tuple9<String, String, String, String, String, String, String, Integer,Integer> map(String s) throws Exception {
            String [] words = s.split(",");
            //Tuple9 tuple9 = new Tuple9<>(words[0], words[1], words[2], words[3], words[4], words[5], words[6], Integer.parseInt(words[7], Integer.valueOf(1)));
            Tuple9 tuple9 = new Tuple9<>();
            tuple9.f0 = words[0];
            tuple9.f1 = words[1];
            tuple9.f2 = words[2];
            tuple9.f3 = words[3];
            tuple9.f4 = words[4];
            tuple9.f5 = words[5];
            tuple9.f6 = words[6];
            tuple9.f7 = words[7] ==null || words[7].equals("'null'")? 0:Integer.parseInt(words[7]);
            tuple9.f8 = Integer.valueOf(1);
            return tuple9;
        }
    }

}
