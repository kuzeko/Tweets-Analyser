package eu.unitn.disi.db.spleetter.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweets' first and last appearance date
 *
 * 0 - hashtag id
 * 1 - first timestamp [h]
 * 2 - last timestamp [h]
 *
 */
@FunctionAnnotation.ConstantFieldsFirst({1})
@FunctionAnnotation.ConstantFieldsSecond({1})
public class HashtagLifespanJoin extends JoinFunction {
    private static final Log LOG = LogFactory.getLog(DictionaryFilterJoin.class);
    private long counter = 0;

    private Record pr2 = new Record(3);

    @Override
    public void join(Record first, Record last, Collector<Record> records) throws Exception {

        pr2.setField(0, first.getField(0, IntValue.class));
        pr2.setField(1, first.getField(1, StringValue.class));
        pr2.setField(2, last.getField(1, StringValue.class));
        records.collect(pr2);
        if(TweetCleanse.HashtagLifespanJoinLog){
            this.counter++;
        }

    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.HashtagLifespanJoinLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
