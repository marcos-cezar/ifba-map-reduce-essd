package br.edu.ifba.reduceSideJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 20/02/14
 * Time: 23:42
 * To change this template use File | Settings | File Templates.
 */
public class BrazilianUsersReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_LIST = new Text("");

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();

    private String joinType = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = "innerjoin";
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        listA.clear();
        listB.clear();


        for (java.util.Iterator<Text> it = values.iterator(); it.hasNext(); ) {
            Text element =  it.next();

            if (element.charAt(0) == 'U') {
                listA.add(new Text(element.toString().substring(1)));
            } else if (element.charAt(0) == 'P') {
                listB.add(new Text(element.toString().substring(1)));
            }

            executeJoinLogic(context);

        }

    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        if (joinType.equalsIgnoreCase("innerjoin")) {
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text elementListA : listA) {
                    for (Text elementListB : listB) {
                        context.write(elementListA, elementListB);
                    }
                }
            }
        }
    }
}
