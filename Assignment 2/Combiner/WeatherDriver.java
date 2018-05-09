/***
 *
 *  This program calculates mean maximum and mean minimum of temperatures recorded by various stations. the mean is
 *  calculated per station-id . we are calculating means for the temperatures for the year 1991
 */

//import statements
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

// The driver class
public class WeatherDriver {

    public static void main(String[] args)throws Exception
    {
        //Creating jobConf object
        JobConf conf = new JobConf(WeatherDriver.class);

        //setting file input and output parameters
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //specifying mapper and reducer class
        conf.setMapperClass(WeatherMapper.class);
        conf.setReducerClass(WeatherReducer.class);

        //setting data types of mapper outputs
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(WeatherRecord.class);

        //running the job
        JobClient.runJob(conf);
    }

}