import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

// Main Class Driver Program
public class PageRankDriver{


    static enum counter{
        NumberOfPages,
        delta
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
            System.exit(2);

        Job preprocessJob =  createPreProcessJob(otherArgs, conf);
        preprocessJob.waitForCompletion(true);
        long numberOfPages = preprocessJob.getCounters().findCounter(counter.NumberOfPages).getValue();
        conf.setLong(CONSTANTS.NUMBEROFPAGES, numberOfPages);

        Job pageNameToNum = getPreProcessStringToNumberMappingJob(conf, numberOfPages);
        pageNameToNum.waitForCompletion(true);

        Job sparseMatrix = getSparseMatrixJob(conf);
        sparseMatrix.waitForCompletion(true);


        double delta = 0.0;

        for (int i = 0; i < 10; i++){
            long pageRankStartTime = System.currentTimeMillis();
            conf.setInt(CONSTANTS.IterationNumber, i);
            Job matrixmulJob =  getMatrixMultiJob(conf, i);
            matrixmulJob.waitForCompletion(true);
            delta = Double.longBitsToDouble(matrixmulJob.getCounters().findCounter(counter.delta).getValue());
            conf.setDouble(CONSTANTS.DanglingMass, delta);
            long pageRankendEndTime = System.currentTimeMillis();
            long pageRankexecTime = pageRankendEndTime - pageRankStartTime;
            System.out.println("Step 3 : PageRank For Iteration "+ i + " : " + pageRankexecTime + " ms");
        }

        long topKRankStartTime = System.currentTimeMillis();
        Job topKPages = getTopKJob(otherArgs[otherArgs.length - 1], conf);
        topKPages.waitForCompletion(true);

    }

     public static Job createPreProcessJob(String [] args, Configuration conf) throws IOException{
        Job job = new Job(conf, "Pre Process");
        job.setJarByClass(PageRankDriver.class);
        job.setReducerClass(ParseReducer.class);
        job.setMapperClass(ParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        MultipleOutputs.addNamedOutput(job, CONSTANTS.NumOffsetFileIdentifier,
                SequenceFileOutputFormat.class,
                IntWritable.class, LongWritable.class);

        MultipleOutputs.addNamedOutput(job, CONSTANTS.AdjacencyListFileIdentifier,
                SequenceFileOutputFormat.class,
                Text.class, Node.class);

        MultipleOutputs.addNamedOutput(job, CONSTANTS.StringToNumberMapFileIdentifier,
                SequenceFileOutputFormat.class,
                IntWritable.class, MapWritable.class);

        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path("output"));
        return job;
    }

}