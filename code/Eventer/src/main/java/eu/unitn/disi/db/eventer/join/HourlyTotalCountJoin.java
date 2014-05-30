package eu.unitn.disi.db.eventer.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.eventer.TweetPeaks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins ngram apperance count with the total number of tweets in that hour
 * the record
 *
 * 0 - timestamp[h]
 * 1 - word
 * 2 - count
 * 3 - fake?
 * 4 - tweets count
 */


@FunctionAnnotation.ConstantFieldsFirst({0,1,2,3})
@FunctionAnnotation.ConstantFieldsSecond({0})
public class HourlyTotalCountJoin extends JoinFunction {
    private static final Log LOG = LogFactory.getLog(HourlyTotalCountJoin.class);
    private long counter = 0;

    @Override
    public void join(Record word, Record counts, Collector<Record> records) throws Exception {
        word.addField(counts.getField(1, IntValue.class));
        records.collect(word);
        if (TweetPeaks.HourlyTotalCountJoinLog) {
            //System.out.printf("TDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetPeaks.HourlyTotalCountJoinLog) {
            LOG.fatal(counter);
        }
        super.close();
    }


}
