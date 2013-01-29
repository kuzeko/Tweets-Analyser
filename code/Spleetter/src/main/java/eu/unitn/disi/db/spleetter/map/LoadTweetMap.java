package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a PactRecord containing one string in to multiple string/integer
 * pairs. The string is tokenized by whitespaces. For each token a new record is
 * emitted,
 * 0 - tweet id
 * 1 - user id
 * 2 - tweet text
 * 3 - number of words
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class LoadTweetMap extends MapStub {
    // initialize reusable mutable objects

    private final PactRecord outputRecord = new PactRecord(4);
    private final PactLong tid = new PactLong();
    private final PactInteger uid = new PactInteger();
    private final PactString tweet = new PactString();
    private final PactInteger numWords = new PactInteger();
    private Pattern recordTextPattern = Pattern.compile(",\"(.+)\"$");

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) {

        String tweetText = record.getField(0, PactString.class).toString();
        String[] splittedLine = tweetText.split(",");
        Matcher matchRecordText = recordTextPattern.matcher(tweetText);

        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Integer.parseInt(splittedLine[1]));


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
    }
}