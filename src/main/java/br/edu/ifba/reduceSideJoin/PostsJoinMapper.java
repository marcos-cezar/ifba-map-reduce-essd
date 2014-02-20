package br.edu.ifba.reduceSideJoin;

import br.edu.ifba.util.InformationExtractorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 19/02/14
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */
public class PostsJoinMapper extends Mapper<Object, Text, Text, Text> {


    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String linha = value.toString();

        System.out.println("Linha lida: " + linha);

        try {

            if (linha.contains(InformationExtractorUtils.STACK_TAG_NAME)) {

                Map<String, String> mappedAttributesFromXml = InformationExtractorUtils
                        .extractAttributesFromXmlTagIntoMap(new ByteArrayInputStream(linha.getBytes()), InformationExtractorUtils.STACK_TAG_NAME);


                final String userId = "UserId";

                if (mappedAttributesFromXml.containsKey(userId)) {
                    outKey.set(mappedAttributesFromXml.get(userId));
                    outValue.set("P" + mappedAttributesFromXml.get(linha));


                }

                context.write(outKey, outValue);

            }


        } catch (XMLStreamException e) {
            System.out.println(e.getMessage());
        }

    }
}
