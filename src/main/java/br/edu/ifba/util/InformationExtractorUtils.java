package br.edu.ifba.util;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 18/02/14
 * Time: 22:15
 * To change this template use File | Settings | File Templates.
 */
public class InformationExtractorUtils {


    public static final String STACK_TAG_NAME = "<row";

    public static final String REGEX_FILTER = "bra[sz]il";
    public static final String STACK_CANNONICAL_TAG_NAME = "row";


    public static Map<String, String> extractAttributesFromXmlTagIntoMap(InputStream input, String tagName) throws XMLStreamException {

        XMLEventReader eventReader = XMLInputFactory.newFactory().createXMLEventReader(input);
        final Map<String, String> attributesMap = new HashMap<String, String>();

        while(eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();

            if (event.isStartElement()) {
                final StartElement startElement = event.asStartElement();

                if (startElement.getName().getLocalPart() == tagName) {
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
