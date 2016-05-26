import java.util.HashMap;
import java.util.Map;

public class XMLParserUtils {
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<>();

		try {
			String afterTrim = xml.trim();
			String[] tokens = afterTrim.substring(5, afterTrim.length() - 3)
					.split("\"");
			
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];

				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException siobe) {
			// Wrong tag <***> occurs.
			// We do it here on purpose, just ingore.
			System.err.println(xml);
		}

		return map;
	}
}
