import java.io.IOException;
import java.io.InputStream;

public class Resource {
	public InputStream getResouce(String path) throws IOException {
		InputStream in = this.getClass().getResourceAsStream(path);
		return in;
	}
}
