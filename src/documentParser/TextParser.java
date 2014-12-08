package documentParser;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParser {
	
	public String regExpParse(String content) {
		Pattern p = Pattern.compile("\\{{2}(?:C|c)ite[^\\}]+\\}{2}|" +	/*ignore {{C(c)ite ...}}*/
				"\\[{2}(?:File|Image)[^\\[]+|" +						/*ignore [[File/Image... */
				"<[^>]+>|\\W");			/*ignore punctuation*/
		//Pattern p = Pattern.compile("<[^>]+>");
		Matcher m = p.matcher(content);
		String parsedStr = m.replaceAll(" ").toLowerCase();
		return parsedStr;
	}
	
	public void parse(String content) {
		String parsedStr;
		parsedStr = regExpParse(content);
		StringTokenizer tokenizer = new StringTokenizer(parsedStr);
		String tmp;
		while (tokenizer.hasMoreTokens())
		{
			tmp = tokenizer.nextToken();
			System.out.println(tmp);
		}
	}
}