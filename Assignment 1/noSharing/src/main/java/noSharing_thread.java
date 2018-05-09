import java.util.*;

public class noSharing_thread implements Runnable {


    int start ;
    int end;
    ArrayList<String> weatherData;
    HashMap<String,Double> station_temperature ;
    HashMap<String,Integer> station_count;
    Boolean fibonacciFlag;


    public noSharing_thread(int start , int end , ArrayList<String> weatherData, HashMap<String,Double> station_temperature,
                         HashMap<String,Integer> station_count, Boolean fibonacciFlag ){
        this.start=start;
        this.end=end;
        this.weatherData = weatherData;
        this.station_temperature = station_temperature;
        this.station_count = station_count;
        this.fibonacciFlag=fibonacciFlag;

    }

    // Fibonacci Method for intentional delay
    public static long fibonacci(int n) {
        if(n<0)
            return (long)-1;
        else if (n==0)
            return(long)0;
        else if (n==1)
            return(long) 1;
        else
            return (long)(fibonacci(n-1) + fibonacci(n-2));
    }


    public void run() {
        String record = "";
        int i = start;
        while(i<=end){
            record = weatherData.get(i);

            String[] recordFields = record.split(",");

            String stationID = recordFields[0].trim();

            String type = recordFields[2].trim();

            if(type.equalsIgnoreCase("TMAX")){

                double maxTemp = Double.parseDouble(recordFields[3].trim());

                if(!(station_temperature.containsKey(stationID))){
                    station_temperature.put(stationID, maxTemp);
                    if(!(station_count.containsKey(stationID))){
                        station_count.put(stationID, 1);
                    }
                }
                else{
                    if(fibonacciFlag){
                        long c = fibonacci(17);
                    }

                    station_temperature.put(stationID,(Double.parseDouble(station_temperature.get(stationID).toString())+maxTemp));
                    if(!station_count.containsKey(stationID)){
                        station_count.put(stationID, 1);
                    }
                    else
                        {
                            if(fibonacciFlag){
                                long c = fibonacci(17);
                            }
                            station_count.put(stationID,(Integer.parseInt(station_count.get(stationID).toString())+1));
                        }
                    }
            }
            i++;
        }
    }

}
