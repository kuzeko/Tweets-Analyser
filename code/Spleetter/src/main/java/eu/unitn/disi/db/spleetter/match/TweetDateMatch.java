package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Joins the tweet record with its corresponding date
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
    private PactRecord output = new PactRecord(5);

    @Override
    public void match(PactRecord tweet, PactRecord tweetDate, Collector<PactRecord> records) throws Exception {
        output.setField(0, tweet.getField(0, PactLong.class));
        output.setField(1, tweet.getField(1, PactInteger.class));
        output.setField(2, tweet.getField(2, PactString.class));
        output.setField(3, tweet.getField(3, PactInteger.class));
        output.setField(4, tweetDate.getField(2, PactString.class));
        records.collect(output);
    }

}
