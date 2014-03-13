package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Converts a Record containing one string in to multiple into a tweet record
 * containing the date of the tweet
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - timestamp[h]<br />
 */
@FunctionAnnotation.ConstantFields({})
public class LoadTweetDatesMap extends MapFunction {
    // initialize reusable mutable objects
    private static final Log LOG = LogFactory.getLog(LoadTweetDatesMap.class);
    private long counter = 0;

    private final Record outputRecord = new Record(3);
    private final LongValue tid = new LongValue();
    private final LongValue uid = new LongValue();
    private final StringValue timestampH = new StringValue();
    private Pattern datePattern = Pattern.compile(",\"(2[0-9]{3}-[0-9]{2}-[0-9]{2}\\W[0-9]{2}).+\"$");

    @Override
    public void map(Record record, Collector<Record> collector) {
        String tweetText = record.getField(0, StringValue.class).toString();
        String tweetTimestamp = "";
        String[] splittedLine = tweetText.split(",");
        Matcher matchRecordDate = datePattern.matcher(tweetText);

        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Long.valueOf(splittedLine[1]));

        if (matchRecordDate.find()) {
            tweetTimestamp = matchRecordDate.group(1).replace(' ', '-');
        } else {
            tweetTimestamp = "";
        }
        timestampH.setValue(tweetTimestamp);


        this.outputRecord.setField(0, tid);
        this.outputRecord.setField(1, uid);
        this.outputRecord.setField(2, timestampH);

        collector.collect(this.outputRecord);
        if(TweetCleanse.LoadTweetDatesMapLog){
            //System.out.printf("LTD out\n");
            this.counter++;
        }


    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.LoadTweetDatesMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
