package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the cleaned english tweets with their correspondent polarities
 * appending them to the end of the record
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - tweet text<br />
 * 3 - num of words in the original tweet<br />
 * 4 - timestamp [h]<br />
 * 5 - negative polarity<br />
 * 6 - positive polarity<br />
 */
@StubAnnotation.ConstantFieldsFirst({0, 2, 3, 4})
public class TweetPolarityMatch extends MatchStub {

    private static final Log LOG = LogFactory.getLog(TweetPolarityMatch.class);
    long counter = 0;
    private PactRecord output = new PactRecord(7);

    @Override
    public void match(PactRecord englishTweet, PactRecord polarityValue, Collector<PactRecord> records) throws Exception {
        output.setField(0, polarityValue.getField(0, PactLong.class));
        output.setField(1, polarityValue.getField(1, PactLong.class));
        output.setField(2, englishTweet.getField(2, PactString.class));
        output.setField(3, englishTweet.getField(3, PactInteger.class));
        output.setField(4, englishTweet.getField(4, PactString.class));
        output.setField(5, polarityValue.getField(2, PactDouble.class));
        output.setField(6, polarityValue.getField(3, PactDouble.class));

        records.collect(output);
        if (TweetCleanse.TweetPolarityMatchLog) {
            //System.out.printf("TPM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.TweetPolarityMatchLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
