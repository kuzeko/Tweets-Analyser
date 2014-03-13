package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Converts a Record containing one string in to multiple string/integer
 * pairs. The string is tokenized by whitespaces. For each token a new record is
 * emitted
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - tweet text<br />
 * 3 - number of words<br />
 */
@FunctionAnnotation.ConstantFields({})
public class LoadTweetMap extends MapFunction{
    // initialize reusable mutable objects

    private static final Log LOG = LogFactory.getLog(LoadTweetMap.class);
    private long counter = 0;

    private final Record outputRecord = new Record(4);
    private final LongValue tid = new LongValue();
    private final LongValue uid = new LongValue();
    private final StringValue tweet = new StringValue();
    private final IntValue numWords = new IntValue();
    private Pattern recordTextPattern = Pattern.compile(",\"(.+)\"$");

    @Override
    public void map(Record record, Collector<Record> collector) {

        String tweetText = record.getField(0, StringValue.class).toString();
        String[] splittedLine = tweetText.split(",");
        Matcher matchRecordText = recordTextPattern.matcher(tweetText);

        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Long.valueOf(splittedLine[1]));


        if (matchRecordText.find()) {
            tweetText = matchRecordText.group(1);
        } else {
            tweetText = "";
        }
        tweet.setValue(tweetText);

        numWords.setValue(StringUtils.numWords(tweetText));

        this.outputRecord.setField(0, tid);
        this.outputRecord.setField(1, uid);
        this.outputRecord.setField(2, tweet);
        this.outputRecord.setField(3, numWords);

        collector.collect(this.outputRecord);
        if(TweetCleanse.LoadTweetMapLog){
            //System.out.printf("LTM out\n");
            this.counter++;
        }


    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.LoadTweetMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
