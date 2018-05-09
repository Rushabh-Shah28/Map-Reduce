import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class TopKMapper extends Mapper<Text, Node, NullWritable, Text> {

    private TreeMap<TreeMapObject, String> topKNodes;

    public void setup(Context con){
        //initializing Tree map . the key is an object that contains page name and page rank
        this.topKNodes = new TreeMap<TreeMapObject, String>(new MapComparator());
    }


    public void map(Text key, Node value, Context context)
            throws IOException, InterruptedException {
        //adding pageranks values to tree-map
        topKNodes.put(new TreeMapObject(value.pageRank,key.toString()),key.toString());
        if (topKNodes.size() > 100) {
            topKNodes.remove(topKNodes.firstKey());
        }
        int x = topKNodes.size();
    }


    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<TreeMapObject, String> node : topKNodes.entrySet()) {
            context.write(NullWritable.get(),  new Text(node.getKey().pageRank + "," + node.getValue()));
        }
    }

}