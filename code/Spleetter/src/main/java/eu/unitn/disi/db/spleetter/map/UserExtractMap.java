/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Filters the tweet record keeping only the user id
 * 0 - user id
 */
@FunctionAnnotation.ConstantFields({})
public class UserExtractMap extends MapFunction  implements Serializable{
    private static final Log LOG = LogFactory.getLog(UserExtractMap.class);
    private long counter = 0;
    private Record pr2 = new Record(1);


    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        pr2.setField(0, pr.getField(1, LongValue.class ));
        records.collect(pr2);
        if(TweetCleanse.UserExtractMapLog){
          this.counter++;
        }
    }


    @Override
    public void close() throws Exception {
        if(TweetCleanse.UserExtractMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }


}
