import java.io.*;

import org.htmlparser.beans.*;
import org.htmlparser.util.ParserException;

public class getText {
  public static String get(String url)
    throws ParserException {
    StringBean sb = new StringBean();
    sb.setLinks(false);
    sb.setReplaceNonBreakingSpaces(true);
    sb.setCollapse(true);
    sb.setURL(url);
    return sb.getStrings();
  }
  public static void main(String [] argv) {
    try {
      String output = get("file:/home/terry/test.html");
      FileWriter writer = new FileWriter(new File("./test.out"));
      writer.write(output, 0, output.length());
      writer.close();
    }
    catch (Exception e) {
    }
  }
}

