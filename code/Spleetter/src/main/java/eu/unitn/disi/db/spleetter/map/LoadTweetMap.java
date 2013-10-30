package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Converts a PactRecord containing one string in to multiple string/integer
 * pairs. The string is tokenized by whitespaces. For each token a new record is
 * emitted
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - tweet text<br />
 * 3 - number of words<br />
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class LoadTweetMap extends MapStub {
    // initialize reusable mutable objects

    private static final Log LOG = LogFactory.getLog(LoadTweetMap.class);
    private long counter = 0;

    private final PactRecord outputRecord = new PactRecord(4);
    private final PactLong tid = new PactLong();
    private final PactLong uid = new PactLong();
    private final PactString tweet = new PactString();
    private final PactInteger numWords = new PactInteger();
    private Pattern recordTextPattern = Pattern.compile(",\"(.+)\"$");

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) {

        String tweetText = record.getField(0, PactString.class).toString();
        String[] splittedLine = tweetText.split(",");
        Matcher matchRecordText = recordTextPattern.matcher(tweetText);

        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Long.parseLong(splittedLine[1]));


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
