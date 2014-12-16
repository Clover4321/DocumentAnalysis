package documentParser;

public class XMLParser {
	public String getTitle(String content) {
		String titleString;
		int titleIndexStart = content.indexOf("<title>");
		int titleIndexEnd = content.indexOf("</title>");
		titleString = content.substring(titleIndexStart+7,titleIndexEnd);
		return titleString;
	}
	
	public String getText(String content) {
		String textString;
		int textIndexStart = content.indexOf("<text xml:space=\"preserve\">");
		if (textIndexStart < 0) {
			return null;
		}
		int textIndexEnd = content.indexOf("</text>");
		textString = content.substring(textIndexStart+27,textIndexEnd);
		return textString;
	}
	
	public String getId(String content) {
		String idString;
		int idIndexStart = content.indexOf("<id>");
		int idIndexEnd = content.indexOf("</id>");
		idString = content.substring(idIndexStart+4,idIndexEnd);
		return idString;
	}
}
