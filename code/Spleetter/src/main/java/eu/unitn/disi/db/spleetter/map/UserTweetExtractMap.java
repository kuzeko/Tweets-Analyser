/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Filters the tweet record keeping only the user id the tweet id and the timestamp
 * 0 - tweet id
 * 1 - user id
 * 2 - timestamp [h]
 */
@FunctionAnnotation.ConstantFields({0,1})
public class UserTweetExtractMap extends MapFunction  implements Serializable{
    private static final Log LOG = LogFactory.getLog(UserTweetExtractMap.class);
    private long counter = 0;
    private Record pr2 = new Record(3);

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        pr2.setField(0, pr.getField(0, LongValue.class ));
        pr2.setField(1, pr.getField(1, LongValue.class ));
        pr2.setField(2, pr.getField(4, StringValue.class ));
        records.collect(pr2);
        if(TweetCleanse.UserTweetExtractMapLog){
          this.counter++;
        }

    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.UserTweetExtractMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
