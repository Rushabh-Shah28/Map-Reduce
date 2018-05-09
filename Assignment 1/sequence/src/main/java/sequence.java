import java.util.*;
import java.io.*;

public class sequence {

    // Fibonacci Method for intentional delay
    public static long fibonacci(int n){
        if(n<0)
            return (long)-1;
        else if (n==0)
            return(long)0;
        else if (n==1)
            return(long) 1;
        else
            return (long)(fibonacci(n-1) + fibonacci(n-2));
    }

    //Reading input data and storing it in accumulation Data object (weather_data)
    public static void generating_accumulation_data_object(List<String> weather_data, String inputPath){
        String line ="";
        try {
            BufferedReader br = new BufferedReader(new FileReader(inputPath));
            while((line = br.readLine() ) != null){
                weather_data.add(line);
            }
            br.close();
        }
        catch(Exception e){
            System.out.println("Error statement : " +e);
        }
    }

    // Method for generating the final average temperatures for each station.
    public static void generating_average_temperature(HashMap<String, Double> station_temperature,
                                                      HashMap<String, Integer> station_count,
                                                      HashMap<String, Double> station_average){
        Iterator iter = station_temperature.keySet().iterator();

        while (iter.hasNext()) {

            String stationID = iter.next().toString();
            double temp_sum = Double.parseDouble(station_temperature.get(stationID).toString());
            int count = Integer.parseInt(station_count.get(stationID).toString());
            double average = temp_sum/count;
            station_average.put(stationID, average);
        }
    }

    // Method for generating output files with and without intentional delay caused by fibonacci function.
    public static void generating_output_files(Boolean fibonacciFlag, HashMap<String, Double> station_average,
                                               Double maxTime, Double minTime, int totalExecutionCount, Double totalTime){
            try {
                    FileWriter fw;
                    if (fibonacciFlag) {
                        fw = new FileWriter("fibonacci_sequence_output.txt");
                    } else {
                        fw = new FileWriter("sequence_output.txt");
                    }
                    PrintWriter pw = new PrintWriter(fw);
                    pw.println("STATION ID : AVERAGE_TMAX_VALUE\n");
                    Iterator iterator = station_average.keySet().iterator();

                    while (iterator.hasNext()) {
                        String stationID = iterator.next().toString();
                        pw.println(stationID + " : " + station_average.get(stationID));
                    }
                    pw.println("\n******************** Metrics *********************");
                    pw.println("Maximum execution time: " + maxTime + " seconds");
                    pw.println("Minimum execution time: " + minTime + " seconds");
                    pw.println("Average execution time: " + totalTime / totalExecutionCount + " seconds");

                    pw.close();
                    fw.close();
                }
            catch(Exception e){
                System.out.println("Error generating metrics: "+e);
            }
    }

    public static void main(String[] args){

        List<String> weather_data = new ArrayList<String>();
        HashMap<String, Double> station_temperature = new HashMap<String, Double>();
        HashMap<String, Integer> station_count = new HashMap<String, Integer>();
        HashMap<String, Double> station_average = new HashMap<String,Double>();
        Boolean fibonacciFlag;
        String inputPath="";
        int totalExecutionCount = 10;
        int fibonacciConstant = 17;
        double maxTime =0;
        double minTime =0;
        double totalTime=0;

        //Printing Command line arguments.
        System.out.println("Input Data Location: " +args[0]);
        inputPath = args[0];
        System.out.println("Fibonacci Flag (true/false)?:" +args[1]);
        fibonacciFlag = Boolean.parseBoolean(args[1]);

        //Function call for reading input data and storing it in accumulation Data object (weather_data)
        generating_accumulation_data_object(weather_data, inputPath);

        int executionCount = 0;
        while(executionCount < totalExecutionCount) {
            double executionTime = 0;
            long startTime = System.currentTimeMillis();
            for (String record : weather_data) {

                // Splitting each record field from the data and storing only relevant fields.
                String[] recordFields = record.split(",");
                String stationID = recordFields[0].trim();
                String type = recordFields[2].trim();

                // Only treating a record with "TMAX" field
                if (type.equalsIgnoreCase("TMAX")) {
                    double temperature = Double.parseDouble(recordFields[3]);

                    if (!station_temperature.containsKey(stationID)) {
                        station_temperature.put(stationID, temperature);
                        station_count.put(stationID, 1);
                    } else {
                        if (fibonacciFlag) {
                            long result = fibonacci(fibonacciConstant);
                        }
                        station_temperature.put(stationID, (temperature + Double.parseDouble(station_temperature.get(stationID).toString())));
                        station_count.put(stationID, (1 + Integer.parseInt(station_count.get(stationID).toString())));
                    }
                }
            }


            generating_average_temperature(station_temperature, station_count, station_average);

            long endTime = System.currentTimeMillis();

            executionTime = (double)(endTime-startTime)/1000;
            totalTime+=executionTime;
            if(executionTime>maxTime){
                maxTime=executionTime;
            }
            if(executionCount==1){
                minTime = executionTime;
            }
            if(executionTime < minTime){
                minTime=executionTime;
            }

            executionCount++;

        }

        // Function call for generating output files with and without intentional delay caused by fibonacci function.
        generating_output_files(fibonacciFlag, station_average, maxTime, minTime, totalExecutionCount, totalTime);

    }
}
