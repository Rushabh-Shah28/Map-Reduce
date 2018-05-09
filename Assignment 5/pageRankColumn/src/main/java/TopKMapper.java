import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper <LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
    private TreeMap<DoubleWritable, LongWritable> repToRecordMap;

    public void setup(Context con){
        this.repToRecordMap = new TreeMap<DoubleWritable, LongWritable>();
    }

    @Override
    public void map(LongWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        repToRecordMap.put(new DoubleWritable(value.get()), new LongWritable(key.get()));
        if (repToRecordMap.size() > 100) {
            repToRecordMap.remove(repToRecordMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<DoubleWritable, LongWritable> entry : repToRecordMap.entrySet()) {
            context.write(entry.getValue(), entry.getKey());
        }
    }
}