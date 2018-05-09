import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class WeatherCombiner extends MapReduceBase implements Reducer<Text, WeatherRecord,Text,WeatherRecord> {

    void compute_average(Iterator<WeatherRecord> iterator, int maxCount, int minCount, Double maxSum, Double minSum){

        while(iterator.hasNext()){
            WeatherRecord record = iterator.next();
            if(record.type.toString().equals("TMAX")){
                maxSum += record.getReading();
                maxCount += record.getCount();
            }
            else if(record.type.toString().equals("TMIN")){
                minSum += record.getReading();
                minCount+=record.getCount();
            }
        }
    }

    public void reduce(Text text, Iterator<WeatherRecord> iterator, OutputCollector<Text, WeatherRecord> outputCollector,
                       Reporter reporter) throws IOException {

        // initializing local variables to compute average
        int  maxCount =0;
        int minCount=0;
        double maxSum=0;
        double minSum=0;

        // method for computing the average
        compute_average(iterator, maxCount, minCount, maxSum, minSum);

        // emitting aggregated records
        IntWritable maximumCount = new IntWritable(maxCount);
        IntWritable minimumCount = new IntWritable(minCount);

        DoubleWritable maximumSum = new DoubleWritable(maxSum);
        DoubleWritable minimumSum = new DoubleWritable(minSum);

        Text tMax = new Text("TMAX");
        Text tMin = new Text("TMIN");

        WeatherRecord tmaxRecord = new WeatherRecord(tMax, maximumSum, maximumCount);
        WeatherRecord tminRecord = new WeatherRecord(tMin, minimumSum, minimumCount);

        outputCollector.collect(text,tmaxRecord);
        outputCollector.collect(text,tminRecord);



    }

}