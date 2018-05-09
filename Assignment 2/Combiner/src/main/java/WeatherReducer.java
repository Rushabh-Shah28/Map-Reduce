//import  statements

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


// Reducer class . implements reducer interface
// Emits records which holds stationID and mean of TMIN and TMAX values for respective stationID.
public class WeatherReducer extends MapReduceBase implements Reducer <Text, WeatherRecord,Text,Text> {

    // implementing the unimplemented methods in reducer interface
    public void reduce(Text key, Iterator<WeatherRecord> iterator, OutputCollector<Text, Text> outputCollector,
                       Reporter reporter) throws IOException {

        // initializing local variables to compute average
        int  maxCount =0;
        int minCount=0;
        double maxSum=0;
        double minSum=0;

        //iterating over list of values to compute average
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

        String maxAvg,minAvg = "";


        // logic to handle divide by zero case

        if(minCount==0){
            minAvg= "Null";
        }else{
            minAvg = Double.toString(minSum/minCount);
        }
        if(maxCount>0){
            maxAvg=Double.toString(maxSum/maxCount);
        }else{
            maxAvg = "Null";
        }

        outputCollector.collect(key, new Text(","+minAvg+","+maxAvg));

    }

}