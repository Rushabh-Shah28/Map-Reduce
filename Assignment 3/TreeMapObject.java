public class TreeMapObject {

    public double pageRank;
    public String word;

    TreeMapObject(){
        pageRank = 0;
        word ="";
    }

    TreeMapObject(double pageRank, String word){

        this.pageRank = pageRank;
        this.word = word;

    }
}