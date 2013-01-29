package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Joins the tweet recird with its correspondet date
 * appending it to the end of the record
 *
 * 0 - tweet id
 * 1 - user id
 * 2 - tweet text
 * 3 - num of words in the original tweet
 * 4 - timestamp [h]
 */
@StubAnnotation.ConstantFieldsFirst(fields = {0,1,2,3})
@StubAnnotation.ConstantFieldsSecond(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class TweetDateMatch extends MatchStub {

    @Override
    public void match(PactRecord tweet, PactRecord tweetDate, Collector<PactRecord> records) throws Exception {
        tweet.setField(4, tweetDate.getField(2, PactString.class));
        records.collect(tweet);
    }

}
