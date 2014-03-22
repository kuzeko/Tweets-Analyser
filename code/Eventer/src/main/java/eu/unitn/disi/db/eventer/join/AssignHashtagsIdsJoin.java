package eu.unitn.disi.db.eventer.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.eventer.TweetPeaks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweet record with its corresponding date appending it to the end of
 * the record
 *
 * 0 - timestamp[h] id<br />
 * 1 - hashtag<br />
 * 2 - count<br />
 */
@FunctionAnnotation.ConstantFieldsFirst({1})
@FunctionAnnotation.ConstantFieldsSecond({0,2})
public class AssignHashtagsIdsJoin extends JoinFunction {
    private static final Log LOG = LogFactory.getLog(AssignHashtagsIdsJoin.class);
    private long counter = 0;
    private Record output = new Record(3);

    @Override
    public void join(Record ids, Record counts, Collector<Record> records) throws Exception {
        output.setField(0, counts.getField(0, StringValue.class));
        output.setField(1, ids.getField(1, StringValue.class));
        output.setField(2, counts.getField(2, IntValue.class));


        records.collect(output);
        if (TweetPeaks.AssignHashtagsIdsJoinLog) {
            //System.out.printf("TDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetPeaks.AssignHashtagsIdsJoinLog) {
            LOG.fatal(counter);
        }
        super.close();
    }


}
