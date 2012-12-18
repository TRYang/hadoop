import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
  
import org.apache.hadoop.io.Writable;
  
public class Document implements Writable{
    public static final int kDimention = 300;
    long id;
    double length;
    public Word word[];
    public TreeMap<String, Double> dict;
      
    public Document(){
        word = null;
        dict = null;
    }
    
    public Document(String in) {
    	 String str[] = in.split("\\s+");
        id = Long.parseLong(str[0]);     
        dict = new TreeMap<String, Double>();
        dict.clear();
        word = new Word [str.length - 1];
        String [] ss;
        length = 0;
        for (int i = 1; i < str.length; i++) {
     	   ss = str[i].split(",");
     	   word[i - 1] = new Word();
     	   word[i - 1].word = ss[0];
     	   word[i - 1].tfidf = Double.parseDouble(ss[1]);
     	   dict.put(word[i - 1].word, word[i - 1].tfidf);
     	   length += word[i - 1].tfidf * word[i - 1].tfidf;
         }
    }
      
    public static double getEulerDist(Document doc1, Document doc2){
    	double result = 0;
    	for (int i = 0; i < doc1.word.length; i++) {
    		if (doc2.dict.containsKey(doc1.word[i].word)) {
    			result += doc1.word[i].tfidf * doc2.dict.get(doc1.word[i].word);
    		}
    	}
    	result /= doc1.length;
    	result /= doc2.length;
    	return result;
    }
      
    public void clear(){
        word = null;
        dict.clear();
    }
      
    @Override
    public String toString(){
        String result = String.valueOf(id);
        for (int i = 0; i < word.length; i++)
        	result = result + String.format(" %s,%f", word[i].word, word[i].tfidf);
        return result;
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
       String str[] = in.readUTF().split("\\s+");
       id = Long.parseLong(str[0]);     
       dict = new TreeMap<String, Double>();
       dict.clear();
       word = new Word [str.length - 1];
       String [] ss;
       length = 0;
       for (int i = 1; i < str.length; i++) {
    	   ss = str[i].split(",");
    	   word[i - 1].word = ss[0];
    	   word[i - 1].tfidf = Double.parseDouble(ss[1]);
    	   dict.put(word[i - 1].word, word[i - 1].tfidf);
    	   length += word[i - 1].tfidf * word[i - 1].tfidf;
        }
    }
  
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.toString());
    }
}