package br.edu.ifba.reduceSideJoin;

import br.edu.ifba.util.InformationExtractorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 19/02/14
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */
public class UsersMapper extends Mapper<Object, Text, Text, Text> {


    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("UsersMapper.map()");
        String line = value.toString();
        Map<String, String> mappedAttributes = new HashMap<String, String>();

        try {
            if (line.contains(InformationExtractorUtils.STACK_TAG_NAME)) {
                mappedAttributes = InformationExtractorUtils.extractAttributesFromXmlTagIntoMap(new
                        ByteArrayInputStream(line.getBytes()), InformationExtractorUtils.STACK_CANNONICAL_TAG_NAME);
            }
        } catch (XMLStreamException e) {
            System.out.println(e.getMessage());
        }

        final String keyName = "Id";
        if (mappedAttributes.containsKey(keyName) && mappedAttributes.containsKey("DisplayName")) {

            System.out.println("chave: " + mappedAttributes.get(keyName));
            outKey.set(mappedAttributes.get(keyName));
            System.out.println("valor: " + mappedAttributes.get("DisplayName"));

            outValue.set("U" + mappedAttributes.get("DisplayName"));

            context.write(outKey, outValue);
        }

    }


}
