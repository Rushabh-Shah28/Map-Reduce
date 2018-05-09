import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.*;

// Custom class to hold weather record
class WeatherRecord implements Writable{
    public Text type;  //type can be TMAX or TMIN
    public DoubleWritable reading; // weather reading

    //default constructor
    public WeatherRecord(){
        type = new Text();
        reading = new DoubleWritable();
    }

    // parameterized constructor to initialize type and the corresponding reading
    public WeatherRecord(Text t, DoubleWritable r){
        type = t;
        reading = r;
    }

    public void write(DataOutput dataOutput) throws IOException {
        type.write(dataOutput);
        reading.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        type.readFields(dataInput);
        reading.readFields(dataInput);
    }
}

// Mapper class emits (key,value) pairs where key is the stationID and value is a weatherRecord which holds type of the 
// record and the temperature.
public class WeatherMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,WeatherRecord> {

    //Implementing methods of interface
    public void map(LongWritable key, Text value, OutputCollector<Text, WeatherRecord> outputCollector, Reporter reporter)
            throws IOException {


        //Splitting the record on ','
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
            WeatherRecord weatherRecord = new WeatherRecord(recordType, temp);
            Text stationID = new Text(stationId);
            outputCollector.collect(stationID, weatherRecord);
        }
    }
}