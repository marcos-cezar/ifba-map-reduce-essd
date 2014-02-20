package br.edu.ifba.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer <Text, Text, Text, Text>{

	private Text tmp = new Text();
	private ArrayList<Text> userList = new ArrayList<Text>();
	private ArrayList<Text> postList = new ArrayList<Text>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		this.userList.clear();
		this.postList.clear();

		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			this.tmp = it.next();
			if(this.tmp.charAt(0) == 'U') {
				this.userList.add(new Text(this.tmp.toString().substring(1)));
			}
			else if(tmp.charAt(0) == 'P') {
				this.postList.add(new Text(this.tmp.toString().substring(1)));
			}
		}

		//		if(this.joinType.equalsIgnoreCase("inner")) {
		if(!this.userList.isEmpty() && !this.postList.isEmpty()) {
			for(Text post : this.postList) {
				for(Text user : this.userList) {
					user.set(key + ", " + user);
					context.write(user, post);
				}
			}
		}
		//		}
	}
}
