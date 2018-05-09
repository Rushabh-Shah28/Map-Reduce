import java.util.*;

public class coarseLock_thread implements Runnable {

    public int start ;
    public int end;
    public ArrayList<String> weatherData;
    public HashMap<String,Double> station_temperature ;
    public HashMap<String,Integer> station_count;
    public Boolean fibonacciFlag;


    public coarseLock_thread(int start , int end , ArrayList<String> weatherData, HashMap<String,Double> station_temperature,
                         HashMap<String,Integer> station_count, Boolean fibonacciFlag ){
        this.start=start;
        this.end=end;
        this.weatherData = weatherData;
        this.station_temperature = station_temperature;
        this.station_count = station_count;
        this.fibonacciFlag=fibonacciFlag;

    }

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
                synchronized (station_count) {
                    synchronized (station_temperature) {
                        if (!(station_temperature.containsKey(stationID))) {
                            station_temperature.put(stationID, maxTemp);
                            if (!(station_count.containsKey(stationID))) {
                                station_count.put(stationID, 1);
                            }
                        } else {
                            if (fibonacciFlag) {
                                long c = fibonacci(17);
                            }

                            station_temperature.put(stationID, (Double.parseDouble(station_temperature.get(stationID).toString()) + maxTemp));
                            if (!station_count.containsKey(stationID)) {
                                station_count.put(stationID, 1);

                            } else {
                                if (fibonacciFlag) {
                                    long c = fibonacci(17);
                                }
                                station_count.put(stationID, (Integer.parseInt(station_count.get(stationID).toString()) + 1));
                            }
                        }
                    }
                }
            }
            i++;
        }
    }

}
