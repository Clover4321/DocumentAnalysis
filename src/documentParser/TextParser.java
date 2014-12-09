package documentParser;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParser {
	private static ArrayList<String> cateArrayList;
	
	public ArrayList<String> getCategory() {
		return cateArrayList;
	}
	
	public void extractCategory(String content) {
		cateArrayList = new ArrayList<String>();
		Pattern pattern = Pattern.compile("\\[{2}Category:([^\\]]*)]]");
		Matcher matcher = pattern.matcher(content);
		while (matcher.find())
		{
			cateArrayList.add(matcher.group(1));
		}
	}
	
	public String regExpParse(String content) {
		extractCategory(content);
		//remove the useless information
		Pattern p = Pattern.compile("\\{{2}(?:C|c)ite[^\\}]+\\}{2}|" +	/*ignore {{C(c)ite ...}}*/
				"http\\S+|" +											/*ignore http url*/
				"\\[{2}(?:File|Image)[^\\[]+|" +						/*ignore [[File/Image... */
				"&amp|&nbsp|" +											/*ignore &amp &nbsp*/
				"<[^>]+>|\\W");	/*ignore punctuation*/
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