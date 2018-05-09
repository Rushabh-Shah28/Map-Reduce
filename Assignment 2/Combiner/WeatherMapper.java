import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.*;

// Custom class to hold weather record

class WeatherRecord implements Writable{
    public Text type;  //type can be TMAX or TMIN
    public DoubleWritable readingSum; // weather reading
    public IntWritable count; // number of records

    // default constructor
    public WeatherRecord(){
        type = new Text();
        readingSum = new DoubleWritable();
        count= new IntWritable();
    }

    // custom constructor
    public WeatherRecord(Text t, DoubleWritable r, IntWritable c){
        type = t;
        readingSum = r;
        count= c;
    }

    //method to get Reading
    public double getReading(){
        return Double.parseDouble(readingSum.toString());
    }

    //method to get Count
    public int getCount(){
        return Integer.parseInt(count.toString());
    }

    // method to serialize object
    public void write(DataOutput dataOutput) throws IOException {
        type.write(dataOutput);
        readingSum.write(dataOutput);
        count.write(dataOutput);
    }

    //method to deserialize object
    public void readFields(DataInput dataInput) throws IOException {
        type.readFields(dataInput);
        readingSum.readFields(dataInput);
        count.readFields(dataInput);
    }
}


// Mapper class emits (key,value) pairs where key is the stationID and value a weatherRecord object holding
// type of the record(TMIN, TMAX), temperature and count of the record which can be used to calculate the mean.
public class WeatherMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text, WeatherRecord> {

    //Implementing methods of interface
    public void map(LongWritable key, Text value, OutputCollector<Text, WeatherRecord> outputCollector, Reporter reporter)
            throws IOException {


        // Splitting records on ","
        String[] record = value.toString().split(",");

        //station-id is the first field in the file
        String stationId = record[0];

        //record-type(TMAX,TMIN,..) is the third field in the csv file
        String type = record[2];

        //temperature readings are fourth column in the csv file
        double temperature = Double.parseDouble(record[3]);

        //ignoring all records other than TMAX and TMIN
        if(type.equalsIgnoreCase("TMAX") || type.equalsIgnoreCase("TMIN")) {
            DoubleWritable temp = new DoubleWritable((temperature));
            Text recordType = new Text(type);
            IntWritable count = new IntWritable(1);
            WeatherRecord weatherRecord = new WeatherRecord(recordType, temp, count);
            Text stationID = new Text(stationId);
            outputCollector.collect(stationID, weatherRecord);
        }


    }
}