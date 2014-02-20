package br.edu.ifba.filtering;

import br.edu.ifba.util.InformationExtractorUtils;
import org.apache.hadoop.io.NullWritable;
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
 * Time: 21:09
 * To change this template use File | Settings | File Templates.
 */
public class FilterBraziliansMapper extends Mapper<Object, Text, NullWritable, String> {

    public static final String XML_TAG_NAME = "row";
    private String regexFilter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //HACK: O ideal é pegar de um parametro de conf.
        regexFilter = "bra[sz]il";
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String linha = value.toString();

        Map<String, String> xmlAttributes = new HashMap<String, String>();
        try {
            if (linha.contains("<row")) {
                xmlAttributes = InformationExtractorUtils.extractAttributesFromXmlTagIntoMap(new
                        ByteArrayInputStream(linha.getBytes()), XML_TAG_NAME);
            }
        } catch (XMLStreamException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("O que tem em linha: " + linha);

        if (xmlAttributes.size() > 0) {
            if (xmlAttributes.containsKey("Location")) {
                String locationExtracted = xmlAttributes.get("Location");
                if (locationExtracted.toLowerCase().matches(regexFilter)) {
                    FilterBrazilians.countUsers++;
                    System.out.println("Usuario " + xmlAttributes.get("DisplayName") + " e brasileiro");
                    context.write(NullWritable.get(), xmlAttributes.get("DisplayName"));
                }
                System.out.println("Tem no map? " + locationExtracted);

            }
        }

    }

}
