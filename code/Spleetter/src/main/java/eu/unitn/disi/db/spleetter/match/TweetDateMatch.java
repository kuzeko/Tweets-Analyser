package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweet record with its corresponding date appending it to the end of
 * the record
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - tweet text<br />
 * 3 - num of words in the original tweet<br />
 * 4 - timestamp [h]<br />
 */
@StubAnnotation.ConstantFieldsFirst({0, 1, 2, 3})
@StubAnnotation.ConstantFieldsSecond({})
public class TweetDateMatch extends MatchStub implements Serializable {
    private static final Log LOG = LogFactory.getLog(TweetDateMatch.class);
    private long counter = 0;
    private PactRecord output = new PactRecord(5);



    @Override
    public void match(PactRecord tweet, PactRecord tweetDate, Collector<PactRecord> records) throws Exception {
        output.setField(0, tweet.getField(0, PactLong.class));
        output.setField(1, tweet.getField(1, PactLong.class));
        output.setField(2, tweet.getField(2, PactString.class));
        output.setField(3, tweet.getField(3, PactInteger.class));
        output.setField(4, tweetDate.getField(2, PactString.class));

        records.collect(output);
        if (TweetCleanse.TweetDateMatchLog) {
            //System.out.printf("TDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.TweetDateMatchLog) {
            LOG.fatal(counter);
        }
        super.close();
    }


}
