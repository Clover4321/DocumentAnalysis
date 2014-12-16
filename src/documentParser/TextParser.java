package documentParser;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParser {
	private static ArrayList<String> cateArrayList;
	
	public ArrayList<String> getCategory(String content) {
		cateArrayList = new ArrayList<String>();
		Pattern pattern = Pattern.compile("\\[{2}Category:([^\\]]*)]]");
		Matcher matcher = pattern.matcher(content);
		while (matcher.find())
		{
			cateArrayList.add(matcher.group(1));
		}
		return cateArrayList;
	}
	
	public String regExpParse(String content) {
		//extractCategory(content);
		//remove the useless information
		Pattern p = Pattern.compile("\\{{2}(?:C|c)ite[^\\}]+\\}{2}|" +	//ignore {{C(c)ite ...}}
				"http\\S+|" +											//ignore http url
				"\\[{2}(?:File|Image)[^\\[]+|" +						//ignore [[File/Image... 
				"&amp|&nbsp|" +											//ignore &amp &nbsp
				"<[^>]+>|\\W");	//ignore punctuation
		//Pattern p = Pattern.compile("\\W");
		Matcher m = p.matcher(content);
		String parsedStr = m.replaceAll(" ").toLowerCase();
		//remove pure number word
		Pattern p2 = Pattern.compile("\\b[0-9]+\\b|\\b_+\\b");
		Matcher m2 = p2.matcher(parsedStr);
		String parsedStr2 = m2.replaceAll("");
		return parsedStr2;
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