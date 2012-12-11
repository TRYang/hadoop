import java.util.*;
import java.io.*;
import java.lang.Character;

public class Separator {
  static BufferedReader TextReader;
  static StringTokenizer tokenizer;
  static TreeSet<String> dictionary;
  static TreeSet<String> StopWordDictionary;

	static final String chinese = "[\u0391-\uFFE5]";

	public static boolean isChinese(String str) {
		return str.matches(chinese);
	}

  public static boolean isLower(char c) {
    return c >= 'a' && c <= 'z';
  }

  public static boolean isUpper(char c) {
    return c >= 'A' && c <= 'Z';
  }

  public static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  public static void InitialDictionary() 
    throws IOException {
    try {
      String path = "./dict/word.txt";
      TextReader = new BufferedReader(new FileReader(new File(path)));
      tokenizer = null;
      dictionary = new TreeSet<String>();
      dictionary.clear();
      String buf;
      while (true) {
        buf = nextToken();
        dictionary.add(buf);
      }
    }
    catch (Exception e) {
    }

    try {
      String path = "./dict/stopword.txt";
      TextReader = new BufferedReader(new FileReader(new File(path)));
      tokenizer = null;
      StopWordDictionary = new TreeSet<String>();
      StopWordDictionary.clear();
      String buf;
      while (true) {
        buf = nextToken();
        StopWordDictionary.add(buf);
      }
    }
    catch (Exception e) {
    }
  }

  public static String InvertMaximumMatching(String Text) {
    String result = "", buf;
    int length = Text.length();
    for (int i = length - 1; i >= 0; ) {
      if (isChinese(Text.substring(i, i + 1))) {
        int flag = 0;
        for (int j = 6; j > 0; j--) 
          if ( i - j + 1 >= 0) {
            buf = Text.substring(i - j + 1, i + 1);
            if (dictionary.contains(buf)) {
              result = buf + " " + result;
              flag = 1;
              i -= j;
              break;
            }
          }
        if (flag == 0) {
          result = Text.substring(i, i + 1) + " " + result;
          --i;
        }
      } else {
        char ch = Text.charAt(i);
        if (isLower(ch) || isUpper(ch) || isDigit(ch)) {
          int j = i - 1;
          while (j >= 0) {
            ch = Text.charAt(j);
            if (isLower(ch) || isUpper(ch) || isDigit(ch))
              j--;
            else
              break;
          }
          result = Text.substring(j + 1, i + 1) + " " + result;
          i = j;
        } else {
          result = Text.substring(i, i + 1) + " " + result;
          --i;
        }
      }
    }
    return result;
  }

  public static String Split(String Text) {
    int length = Text.length();
    String buf;
    String result = "";
    int p = 0;
    for (int i = 0; i < length; ) {
      int flag = 0;
      for (int j = 7; j > 0; j--)
        if (i + j <= length) {
          buf = Text.substring(i, i + j);
          if (StopWordDictionary.contains(buf)) {
            flag = j;
            break;
          }
        }
      if (flag == 0) {
        ++i;
      } else {
        if (p < i) result = result + " " + InvertMaximumMatching(Text.substring(p, i));
        result = result + " " + Text.substring(i, i + flag);
        p = i + flag;
        i += flag;
      }
    }
    if (p < length) result = result + " " + InvertMaximumMatching(Text.substring(p, length));
    return result;
  }

  public static void main(String [] args) 
    throws IOException {
    String Text = "";
    try {
      String path = "./test.in";
      TextReader = new BufferedReader(new FileReader(new File(path)));
      tokenizer = null;
      while (true) {
        Text += nextToken();
      }
    }
    catch (Exception e) {
    }
    InitialDictionary();
    System.out.println(Split(Text));
  }

  public static String nextToken() 
    throws IOException {
    while (tokenizer == null || !tokenizer.hasMoreTokens()) {
      tokenizer = new StringTokenizer(TextReader.readLine());
    }
    return tokenizer.nextToken();
  }
}
