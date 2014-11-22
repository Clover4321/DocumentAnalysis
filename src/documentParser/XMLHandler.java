package documentParser;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.spi.XmlReader;

import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class XMLHandler extends DefaultHandler {
	private static List<String> idInPage;
	private static String pageTitle;
	private static String pageContent;
	private boolean page = false;
	private boolean title = false;
	private boolean id = false;
	private boolean text = false;
	
	private void clearContent() {
		idInPage.clear();
		pageContent = "";
		pageTitle = "";
	}
	
	public String getId() {
		//System.out.println(idInPage.get(0));
		return idInPage.get(0);
	}
	
	public String getTitle() {
		//System.out.println(pageTitle);
		return pageTitle;
	}
	
	public String getText() {
		//System.out.println(pageContent);
		return pageContent;
	}
	
	@Override
	public void startDocument() throws SAXException {
		//System.out.println("Start parsing document...");
		idInPage = new ArrayList<String>();
	}
	
	@Override
	public void endDocument() throws SAXException {
		//System.out.println("End");
	}
	
	@Override
	public void startElement(String uri, String localName, String qName,
							 Attributes atts) throws SAXException {
		if (qName.equals("page")){
			clearContent();
			page = true;
		}
		if (qName.equals("title")){
			title = true;
		}
		if (qName.equals("id")){
			id = true;
		}
		if (qName.equals("text")){
			text = true;
		}
	}
	
	@Override
	public void endElement(String namespaceURI, String localName, String qName)
		throws SAXException{
		if (page){
			//System.out.println("end:" + pageTitle);
			page = false;
		}
		if (title){
			title = false;
		}
		if (id){
			id = false;
		}
		if (text){
			text = false;
		}
	}
	
	@Override
	public void characters(char[] ch, int start, int length) {
		if (title){
			pageTitle = new String(ch,start,length);
			//System.out.println(pageTitle);
		}
		if (id){
			String id = new String(ch,start,length);
			idInPage.add(id);
			//System.out.println(idInPage.get(0));
		}
		if (text){
			String temp = new String(ch,start,length);
			pageContent += temp;
			//System.out.println(pageContent);
		}
	}
	
	public void parse(String content) throws SAXException, IOException
	{
		XMLReader parser = XMLReaderFactory.createXMLReader();
		XMLHandler xmlHandler = new XMLHandler();
		parser.setContentHandler(xmlHandler);
		parser.parse(new InputSource(new StringReader(content)));
	}
}