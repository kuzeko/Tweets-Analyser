package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.map.LoadTweetMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweets' authors to the hashtags present in their tweets
 * appending them to the end of the record
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - user id
 *
 */
@StubAnnotation.ConstantFieldsFirst(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class HashtagUserMatch extends MatchStub {
    private PactRecord pr2 = new PactRecord(3);
    private static final Log LOG = LogFactory.getLog(HashtagUserMatch.class);
    private long counter = 0;

    @Override
    public void match(PactRecord userTweet, PactRecord hashtagRecord, Collector<PactRecord> records) throws Exception {
        pr2.setField(0, userTweet.getField(2, PactString.class));
        pr2.setField(1, hashtagRecord.getField(1, PactInteger.class));
        pr2.setField(2, userTweet.getField(1, PactInteger.class));
        records.collect(pr2);
        if(TweetCleanse.HashtagUserMatchLog){
            //System.out.printf("HPM out %s\n", pr2.getField(0, PactString.class));
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.HashtagUserMatchLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
