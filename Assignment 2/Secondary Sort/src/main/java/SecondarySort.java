import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import java.io.*;



public class SecondarySort {

    public static class WeatherRecord implements Writable{

        double maxTempSum;
        double minTempSum;
        int maxTempCount;
        int minTempCount;
        String year;

        //default constructor
        public WeatherRecord(){
            maxTempSum = 0;
            minTempSum = 0;
            maxTempCount = 0;
            minTempCount = 0;
            year =  "";
        }

        // parametrized constructor
        public WeatherRecord(double maxTempSum,
                             int maxTempCount,
                             double minTempSum,
                             int minTempCount,
                             String year){

            this.maxTempSum = maxTempSum;
            this.maxTempCount = maxTempCount;
            this.minTempSum = minTempSum;
            this.minTempCount = minTempCount;
            this.year = year;
        }

        // function to serialize data
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(maxTempSum);
            dataOutput.writeDouble(minTempSum);
            dataOutput.writeInt(maxTempCount);
            dataOutput.writeInt(minTempCount);
            dataOutput.writeUTF(year);
        }

        //function to deserialize data
        public void readFields(DataInput dataInput) throws IOException {

            maxTempSum = dataInput.readDouble();
            minTempSum = dataInput.readDouble();
            maxTempCount = dataInput.readInt();
            minTempCount = dataInput.readInt();
            year = dataInput.readUTF();

        }
    } 

    //Function to implement custom combiner.
    public static class WeatherCustomCombiner extends Reducer<MapEmitKey,
            WeatherRecord,MapEmitKey,WeatherRecord>{

        protected void reduce(MapEmitKey key,Iterable<WeatherRecord>  values,Context context)throws IOException,InterruptedException {

            double maxTemp = 0;
            int maxTempCount = 0;
            double minTemp = 0;
            int minTempCount = 0;
            String year = null;

            //Iterating over values and aggregating data
            for (WeatherRecord w : values){

                maxTemp += w.maxTempSum;
                maxTempCount += w.maxTempCount;
                minTemp += w.minTempSum;
                minTempCount += w.minTempCount;
                year = w.year;

            }

            //emitting aggregate data
            context.write(key, new WeatherRecord( maxTemp,
                    maxTempCount,
                    minTemp,
                    minTempCount,
                    year));
        }
    }

    
    // Class to define custom key . The key for our secondary sort is
    // (station-id,year). We are also overriding the compareTo function
    // and defining our own custom comparator
     

    public static class MapEmitKey implements Writable , WritableComparable{
        public String year;
        public String stationId;

        public MapEmitKey(){
            year="";
            stationId="";
        }

        public MapEmitKey(String stationId,String year){
            this.stationId = stationId;
            this.year = year;
        }


        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(year);
            dataOutput.writeUTF(stationId);
        }

        public void readFields(DataInput dataInput) throws IOException {
            year = dataInput.readUTF();
            stationId = dataInput.readUTF();
        }

        public int compareTo(Object o) {
            MapEmitKey w = (MapEmitKey)o;
            int compareValue = this.stationId.compareTo(w.stationId);
            if(compareValue == 0)
                return this.year.compareTo(w.year);
            return compareValue;
        }
    } // end of MapEmitKey class



    // Mapper Class
    public static class SecondarySortMapper extends Mapper<Object,Text,MapEmitKey,WeatherRecord>{

        protected void map(Object key, Text values , Context context){

            String[] record = values.toString().split(",");

            //retrieving data from relevant positions
            String stationId = record[0].trim();
            String  year = record[1].trim().substring(0,4);
            String  type = record[2].trim();

            if(type.equalsIgnoreCase("TMAX") || type.equalsIgnoreCase("TMIN")){

                String tempValue = record[3];
                MapEmitKey k = new MapEmitKey(stationId,year);

                if(type.equalsIgnoreCase("TMIN")){
                    try {
                        context.write(k,new WeatherRecord(0.0,0,Double.parseDouble(tempValue),1,year));
                    } catch (Exception e) {}
                }
                else if(type.equalsIgnoreCase("TMAX")){
                    try {
                        context.write(k,new WeatherRecord(Double.parseDouble(tempValue),1,0.0, 0,year));
                    } catch (Exception e) {}
                }
            }
        }
    }

    // Reducer Class
    public static class SecondarySortReducer extends Reducer<MapEmitKey,WeatherRecord,NullWritable,Text>{

        protected void reduce(MapEmitKey key, Iterable<WeatherRecord>  values,Context context) throws IOException, InterruptedException {

            double maxTemp = 0;
            int maxTempCount = 0;
            double minTemp = 0;
            int minTempCount = 0;
            String year = null;

            String outputString = key.stationId + ", [ ";


            for (WeatherRecord w : values){

                if(year!=null && !year.equals(w.year)){

                    outputString += "(" + year + ", ";
                    outputString += ( minTempCount > 0) ?(minTemp / minTempCount)+ "," : "Null ,";
                    outputString += ( maxTempCount > 0) ?(maxTemp / maxTempCount)+ ",)" : "Null ,)";
                    maxTemp = 0;
                    minTemp = 0;
                    maxTempCount = 0;
                    minTempCount = 0;
                }
                maxTemp += w.maxTempSum;
                maxTempCount += w.maxTempCount;
                minTemp += w.minTempSum;
                minTempCount += w.minTempCount;
                year = w.year;

            }
            outputString += "(" + year + ", ";
            outputString += ( minTempCount > 0) ?(minTemp / minTempCount)+ "," : "Null ,";
            outputString += (maxTempCount > 0) ?(maxTemp / maxTempCount)+ ",)]" : "Null ,)]";

            context.write(NullWritable.get(), new Text(outputString));

        }
    }
    
    // Custom Grouing Comparator . 
    // Used to group all records having similar station-id
    public static class WeatherGroupingComparator extends WritableComparator{

        public WeatherGroupingComparator(){
            super(MapEmitKey.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2){
            MapEmitKey key1 = (MapEmitKey)w1;
            MapEmitKey key2 = (MapEmitKey)w2;
            return (key1.stationId.compareTo(key2.stationId));
        }

    }

    // Class which contains custom partitioner
    public static class WeatherCustomPartitioner extends Partitioner
            <MapEmitKey,WeatherRecord>{
        public int getPartition(MapEmitKey key,
                                WeatherRecord temp,
                                int reducers){
            return ((key.stationId.hashCode()& Integer.MAX_VALUE)%reducers);
        }
    }


    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();


        Job job = new Job(conf, "Secondary Sort");
        job.setJarByClass(SecondarySort.class);
        //setting mapper class
        job.setMapperClass(SecondarySortMapper.class);
        //setting reducer class
        job.setReducerClass(SecondarySortReducer.class);
        //setting combiner class
        job.setCombinerClass(WeatherCustomCombiner.class);
        //setting partitioner class
        job.setPartitionerClass(WeatherCustomPartitioner.class);
        //setting grouping Comparator
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);

        job.setOutputKeyClass(MapEmitKey.class);
        job.setOutputValueClass(WeatherRecord.class);

        for(int i=0; i<args.length-1 ; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        Path outPath = new Path(args[args.length-1]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
    }
}