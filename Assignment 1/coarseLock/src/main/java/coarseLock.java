import java.util.*;
import java.io.*;

public class coarseLock {


    // Method for generating output files with and without intentional delay caused by fibonacci function.
    public static void generating_output_files(Boolean fibonacciFlag, HashMap<String, Double> station_average,
                                               Double maxTime, Double minTime, int totalExecutionCount, Double totalTime){
        try {
            FileWriter fw;
            if (fibonacciFlag) {
                fw = new FileWriter("fibonacci_CoarseLock_output.txt");
            } else {
                fw = new FileWriter("CoarseLock_output.txt");
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
                                                      HashMap<String, Double> station_average) {
        Iterator iter = station_temperature.keySet().iterator();

        while (iter.hasNext()) {

            String stationID = iter.next().toString();
            double temp_sum = Double.parseDouble(station_temperature.get(stationID).toString());
            int count = Integer.parseInt(station_count.get(stationID).toString());
            double average = temp_sum / count;
            station_average.put(stationID, average);
        }
    }

    public static void main(String[] args){

        // Declaring variables and data structures for further use.
        List<String> weather_data = new ArrayList<String>();
        HashMap<String, Double> station_temperature = new HashMap<String, Double>();
        HashMap<String, Integer> station_count = new HashMap<String, Integer>();
        HashMap<String, Double> station_average = new HashMap<String,Double>();
        Boolean fibonacciFlag;
        String inputPath = "";
        int totalExecutionCount = 10;
        double maxTime =0;
        double minTime =0;
        double totalTime=0;
        int threadCount = Runtime.getRuntime().availableProcessors();

        //Printing Command line arguments.
        System.out.println("Input Data Location: " +args[0]);
        inputPath = args[0];
        System.out.println("Fibonacci Flag (true/false)?:" +args[1]);
        fibonacciFlag = Boolean.parseBoolean(args[1]);

        //Reading input data and storing it in accumulation Data object (weather_data)
        try{

            generating_accumulation_data_object(weather_data, inputPath);

            int dataSize = weather_data.size();
            int executionCount = 0;
            while(executionCount < totalExecutionCount) {
                double executionTime = 0;
                long startTime = System.currentTimeMillis();
                ArrayList<Thread> threadArray = new ArrayList<Thread>();
                int incrementCount=dataSize/threadCount;
                int start = 0;

                for(int i=0 ; i<threadCount; i++){
                    coarseLock_thread clt = new coarseLock_thread(start,start+incrementCount,(ArrayList<String>)weather_data,
                            station_temperature, station_count,fibonacciFlag);
                    Thread t = new Thread(clt);
                    threadArray.add(t);
                    start+=incrementCount;
                }

                for(int i=0; i<threadArray.size(); i++){
                    threadArray.get(i).start();
                }

                for(int i=0; i<threadArray.size(); i++){
                    threadArray.get(i).join();
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

            generating_output_files(fibonacciFlag, station_average, maxTime, minTime, totalExecutionCount, totalTime);

        }
        catch (Exception e){
            System.out.println("Error statement: " +e);
        }

    }
}
