import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


class WeatherRecord implements Writable {
    public DoubleWritable maxSum; // running sum of TMAX records
    public IntWritable maxCount;  // running count of TMAX records
    public DoubleWritable minSum; // running sum of TMIN records
    public IntWritable minCount; // running count of TMIN records

    // default constructor
    public WeatherRecord(){
        maxSum = new DoubleWritable();
        maxCount= new IntWritable();
        minSum = new DoubleWritable();
        minCount = new IntWritable();
    }

    // parameterized constructor
    public WeatherRecord(DoubleWritable ms, IntWritable mc, DoubleWritable ms1, IntWritable mc1){
        maxSum = ms;
        maxCount= mc;
        minSum = ms1;
        minCount = mc1;
    }

    //method to get running total of temperature
    public double getMaxSum(){
        return Double.parseDouble(maxSum.toString());
    }

    //method to get running total of temperature
    public double getMinSum(){
        return Double.parseDouble(minSum.toString());
    }

    //method to get Count
    public int getMaxCount(){
        return Integer.parseInt(maxCount.toString());
    }

    //method to get Count
    public int getMinCount(){
        return Integer.parseInt(minCount.toString());
    }


    public void setMaxCount(int c){
        maxCount = new IntWritable(c);
    }

    // method to set count
    public void setMinCount(int c){
        minCount = new IntWritable(c);
    }

    //method to set reading sum
    public void setMaxSum(double r){
        maxSum = new DoubleWritable(r);
    }

    //method to set reading sum
    public void setMinSum(double r){
        minSum = new DoubleWritable(r);
    }

    // method to serialize object
    public void write(DataOutput dataOutput) throws IOException {
        maxSum.write(dataOutput);
        maxCount.write(dataOutput);
        minSum.write(dataOutput);
        minCount.write(dataOutput);
    }

    //method to deserialize object
    public void readFields(DataInput dataInput) throws IOException {
        maxSum.readFields(dataInput);
        maxCount.readFields(dataInput);
        minSum.readFields(dataInput);
        minCount.readFields(dataInput);
    }

} //end of class WeatherRecord


//Driver class for im-mapper combining
public class WeatherDriver{

    public static class WeatherMapper extends Mapper<LongWritable, Text, Text,WeatherRecord > {

        HashMap<String,WeatherRecord> recordMap= new HashMap<String,WeatherRecord>();

        protected void map(LongWritable key, Text value, Mapper.Context context) {
            
            // Splitting values on "," 
            String[] record = value.toString().split(",");

            //station-id is the first field in the file
            String stationId = record[0];

            //record-type(TMAX,TMIN,..) is the third field in the csv file
            String type = record[2];

            //temperature readings are fourth column in the csv file
            double temperature = Double.parseDouble(record[3]);

            // Logic to process records which are either TMIN or TMAX
            if(type.equalsIgnoreCase("TMAX") || type.equalsIgnoreCase("TMIN")){

                // updating hashmap if the station-id  is already present
                if(recordMap.containsKey(stationId)){
                    WeatherRecord w = recordMap.get(stationId);
                    if(type.equalsIgnoreCase("TMAX")){
                        w.setMaxCount(1 + w.getMaxCount());
                        w.setMaxSum(w.getMaxSum() + temperature);
                    }
                    else if(type.equalsIgnoreCase("TMIN")){
                        w.setMinCount(1+w.getMinCount());
                        w.setMinSum(w.getMinSum() + temperature);
                    }
                    recordMap.put(stationId,w);
                }
                else{
                    // adding new entry to hashmap if station-id is not present
                    if(type.equalsIgnoreCase("TMAX")){
                        recordMap.put(stationId, new WeatherRecord(new DoubleWritable(temperature), new IntWritable(1),
                                new DoubleWritable(0), new IntWritable(0)));
                    }
                    else if(type.equalsIgnoreCase("TMIN")){
                        recordMap.put(stationId, new WeatherRecord(new DoubleWritable(0), new IntWritable(0),
                                new DoubleWritable(temperature), new IntWritable(1)));
                    }

                }
            }

        } // end of map method

        //Cleanup function is used to emit aggregated records
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator i =  recordMap.keySet().iterator();
            String stationId="";
            while(i.hasNext()){
                stationId = i.next().toString();

                context.write(new Text(stationId),recordMap.get(stationId));
            }
        } // end of cleanup
    } // end of mapper class

    public static class WeatherReducer extends Reducer<Text, WeatherRecord, Text, Text> {


        protected void reduce(Text key, Iterable<WeatherRecord> values, Context context) throws IOException, InterruptedException {
            // initializing local variables to compute average
            int maxCount =0;
            int minCount=0;
            double maxSum=0;
            double minSum=0;
            String maxAvg ="";
            String minAvg = "";


            for(WeatherRecord w: values){
                WeatherRecord record = w;

                maxSum += Double.parseDouble(record.maxSum.toString());
                maxCount += Integer.parseInt(record.maxCount.toString());
                minSum += Double.parseDouble(record.minSum.toString());
                minCount+=Integer.parseInt(record.minCount.toString());

            }

            // logic to handle divide by zero case

            if(minCount==0){
                minAvg="Null";
            }else{
                minAvg = Double.toString(minSum/minCount);
            }
            if(maxCount==0){
                maxAvg="";
            }else{
                maxAvg= Double.toString(minSum/minCount);
            }


            context.write(new Text(key), new Text(","+minAvg+","+maxAvg));


        }
    }

    //Entry point of  code
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = args[0];
        String output = args[1];

        Job job = new Job(conf, "weather average");
        job.setJarByClass(WeatherMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WeatherRecord.class);

        job.setReducerClass(WeatherReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
    }
}