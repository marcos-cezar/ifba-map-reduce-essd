package br.edu.ifba.reduceSideJoin;

import br.edu.ifba.filtering.FilterBraziliansMapper;
import br.edu.ifba.util.InformationExtractorUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 19/02/14
 * Time: 15:28
 * To change this template use File | Settings | File Templates.
 */
public class TopTenBrazilianPosters extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {


        Job job = new Job();

//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, );
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, );


        return 0;
    }

    public static class TopTenBrazilianPostersMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            Map<String, String> mappedAttributes = new HashMap<String, String>();

            try {
                if (line.contains(InformationExtractorUtils.STACK_TAG_NAME)) {
                    mappedAttributes = InformationExtractorUtils.extractAttributesFromXmlTagIntoMap(new ByteArrayInputStream(line.getBytes()), InformationExtractorUtils.STACK_TAG_NAME);
                }
            } catch (XMLStreamException e) {
                System.out.println(e.getMessage());
            }

            if (mappedAttributes.containsKey("UserId")) {
                if (mappedAttributes.containsKey("Location") && mappedAttributes.get("Location").matches(InformationExtractorUtils.REGEX_FILTER)) {

                    outKey.set(mappedAttributes.get("UserId"));
                    outValue.set("TBP" + value.toString());

                }
            }


        }
    }


}
