import java.util.*;
import java.io.*;

public class noSharing {

    // Method for generating output files with and without intentional delay caused by fibonacci function.
    public static void generating_output_files(Boolean fibonacciFlag, HashMap<String, Double> station_average,
                                               Double maxTime, Double minTime, int totalExecutionCount, Double totalTime){
        try {
            FileWriter fw;
            if (fibonacciFlag) {
                fw = new FileWriter("Fibonacci_No Sharing_output.txt");
            } else {
                fw = new FileWriter("No Sharing_output.txt");
            }
            PrintWriter pw = new PrintWriter(fw);
            pw.println("STATION ID : AVERAGE TMAX VALUE\n");
            Iterator iterator = station_average.keySet().iterator();

            while(iterator.hasNext()) {
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
    public static void generating_average_temperature(HashMap<String, Double> final_station_temperature,
                                                      HashMap<String, Integer> final_station_count,
                                                      HashMap<String, Double> station_average){
        Iterator iter = final_station_temperature.keySet().iterator();

        while(iter.hasNext()) {

            String stationID = iter.next().toString();
            double temp_sum = Double.parseDouble(final_station_temperature.get(stationID).toString());
            int count = Integer.parseInt(final_station_count.get(stationID).toString());
            double average = temp_sum / count;
            station_average.put(stationID, average);
        }
    }

    public static void main(String[] args){

        // Declaring variables and data structures for further use.
        List<String> weather_data = new ArrayList<String>();

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


        generating_accumulation_data_object(weather_data, inputPath);

        int dataSize = weather_data.size();
        int executionCount = 0;
        while(executionCount < totalExecutionCount) {
            double executionTime = 0;
            ArrayList<Thread> threadArray = new ArrayList<Thread>();
            ArrayList<HashMap<String,Double>> station_temperature = new ArrayList<HashMap<String,Double>>();
            ArrayList<HashMap<String,Integer>> station_count = new ArrayList<HashMap<String,Integer>>();
            int incrementCount=dataSize/threadCount;
            int start = 0;
            long startTime = System.currentTimeMillis();
            try {
                for (int i = 0; i < threadCount; i++) {
                    station_temperature.add(new HashMap<String,Double>());
                    station_count.add(new HashMap<String,Integer>());
                    noSharing_thread nst = new noSharing_thread(start, start + incrementCount, (ArrayList<String>) weather_data,
                            station_temperature.get(i), station_count.get(i), fibonacciFlag);
                    Thread t = new Thread(nst);
                    threadArray.add(t);
                    start += incrementCount;
                }

                for (int i = 0; i < threadArray.size(); i++) {
                    threadArray.get(i).start();
                }
                for (int i = 0; i < threadArray.size(); i++) {
                    threadArray.get(i).join();
                }
            }
            catch(Exception e){
                System.out.println("Error Statement: "+e);
            }

            // HAshMaps used to store the partial results given by the threads.
            HashMap<String, Double> final_station_temperature = new HashMap<String, Double>();
            HashMap<String, Integer> final_station_count = new HashMap<String, Integer>();

            int i = 0;
            while(i < station_temperature.size()){

                HashMap<String, Double> partial_temperature_hashmap =  station_temperature.get(i);
                HashMap<String, Integer> partial_count_hashmap =  station_count.get(i);

                Iterator<String> iterator = partial_temperature_hashmap.keySet().iterator();

                while(iterator.hasNext()){
                    String stationID = iterator.next().toString();
                    double partial_temperature_sum = Double.parseDouble(partial_temperature_hashmap.get(stationID).toString());
                    int partial_count = Integer.parseInt(partial_count_hashmap.get(stationID).toString());

                    if(!final_station_temperature.containsKey(stationID)){
                        final_station_temperature.put(stationID, partial_temperature_sum);
                    }
                    else{
                        Double sum = Double.parseDouble(partial_temperature_hashmap.get(stationID).toString()) +
                                partial_temperature_sum;
                        final_station_temperature.put(stationID, sum);
                    }

                    if(!final_station_count.containsKey(stationID)){
                        final_station_count.put(stationID, partial_count);
                    }
                    else{
                        Integer c = Integer.parseInt(partial_count_hashmap.get(stationID).toString()) +
                                partial_count;
                        final_station_count.put(stationID, c);
                    }

                }
                i++;
            }

            generating_average_temperature(final_station_temperature, final_station_count, station_average);

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
}
