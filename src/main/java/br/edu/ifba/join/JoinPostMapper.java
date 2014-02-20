package br.edu.ifba.join;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinPostMapper extends Mapper<Object, Text, Text, Text> {
	
	public static final String XML_TAG_NAME = "row";
	
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String linha = value.toString();

        Map<String, String> xmlAttributes = new HashMap<String, String>();
        try {
            if (linha.contains("<row")) {
                xmlAttributes = convertXmlBufferToMap(new ByteArrayInputStream(linha.getBytes()));
            }
        } catch (XMLStreamException e) {
            System.out.println(e.getMessage());
        }
		
        System.out.println("POST: O que tem em linha: " + linha);

        if (xmlAttributes.size() > 0) {
            if (xmlAttributes.containsKey("UserId")) {
                this.outKey.set(xmlAttributes.get("UserId"));
                this.outValue.set("P" + linha);
                context.write(this.outKey, this.outValue);
            }
        }
	}
	
	private Map<String, String> convertXmlBufferToMap(InputStream input) throws XMLStreamException {

        XMLEventReader eventReader = XMLInputFactory.newFactory().createXMLEventReader(input);
        final Map<String, String> attributesMap = new HashMap<String, String>();

        while(eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();

            if (event.isStartElement()) {
                final StartElement startElement = event.asStartElement();

                if (startElement.getName().getLocalPart() == XML_TAG_NAME) {
                    Iterator<Attribute> attributes = startElement.getAttributes();

                    while (attributes.hasNext()) {
                        final Attribute attr = attributes.next();
                        attributesMap.put(attr.getName().toString(), attr.getValue().toString());
                    }
                }

            }

        }

        return attributesMap;
    }
}