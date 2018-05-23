import java.util.Comparator;

public class MapComparator implements Comparator<TreeMapObject> {

    //Custom comparator
    public int compare(TreeMapObject t1, TreeMapObject t2) {
        if(t1.pageRank>t2.pageRank){
            return 1;
        }else if(t1.pageRank<t2.pageRank){
            return -1;
        }else{
            return  (t1.word.compareTo(t2.word));
        }

    }
}