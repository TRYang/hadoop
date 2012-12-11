import java.util.*;
import java.io.*;
import java.lang.Character;

public class test{
	static BufferedReader reader;
	static StringTokenizer tokenizer;
	static final String chinese = "[\u0391-\uFFE5]";
	public static boolean isChinese(String str){
		return str.matches(chinese);
	}
	public static void main(String [] args)
		throws IOException{
		String path;
		String ans = "";
		for (int i = 1; i <= 3; i++){
			try{
				path = "./test" + String.valueOf(i) + ".in";
				reader = new BufferedReader(new FileReader(new File(path)));
				tokenizer = null;
				String buf;
				while (true){
					buf = nextToken();
					for (int t = 0; t < 10000; t++) ans = ans + buf;
				}
			}
			catch (Exception e){
			}
		}
		System.out.println(ans);
	}

	public static String nextToken()
		throws IOException{
		while (tokenizer == null || !tokenizer.hasMoreTokens()){
			tokenizer = new StringTokenizer(reader.readLine());
		}
		return tokenizer.nextToken();
	}
}
