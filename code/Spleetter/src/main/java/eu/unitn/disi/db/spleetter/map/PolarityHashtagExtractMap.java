package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.DoubleValue;
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
 * Filters the tweet record keeping only the tweet id, user id, timestamp and polarity
 * 0 - tweet id
 * 1 - user id
 * 2 - timestamp [h]
 * 3 - neg polarity
 * 4 - pos polarity
 */

@FunctionAnnotation.ConstantFields({0,1})
public class PolarityHashtagExtractMap extends MapFunction  implements Serializable{
    private static final Log LOG = LogFactory.getLog(PolarityHashtagExtractMap.class);
    private long counter = 0;
    private Record pr2 = new Record(5);

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        pr2.setField(0, pr.getField(0, LongValue.class ));
        pr2.setField(1, pr.getField(1, LongValue.class ));
        pr2.setField(2, pr.getField(4, StringValue.class ));
        pr2.setField(3, pr.getField(5, DoubleValue.class ));
        pr2.setField(4, pr.getField(6, DoubleValue.class ));
        records.collect(pr2);
        if(TweetCleanse.PolarityHashtagExtractMapLog){
          this.counter++;
        }

    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.PolarityHashtagExtractMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
