	
public class Word implements Comparable<Word> {
			
	Word() {
		tfidf = 0;
		word = "";
	}
	
	Word(String str) {
		String [] tm = str.split(" ");
		tfidf = Double.parseDouble(tm[0]);
		word = new String(tm[1]);
	}
		
	public int compareTo(Word b) {
		if (Math.abs(this.tfidf - b.tfidf) < 1e-12) return 0;
		return this.tfidf > b.tfidf ? 1 : -1;
	}
		
	double tfidf;
	String word;
}