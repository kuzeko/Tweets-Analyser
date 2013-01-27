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
 * emitted, 0 - tweet id 1 - user id 2 - tweet text 3 - number of words 4 -
 * timestamp[h]
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class LoadTweetMap extends MapStub {
    // initialize reusable mutable objects

    private final PactRecord outputRecord = new PactRecord();
    private final PactString line = new PactString();
    private final PactLong tid = new PactLong();
    private final PactInteger uid = new PactInteger();
    private final PactString tweet = new PactString();
    private final PactInteger numWords = new PactInteger();
    private final PactString timestampH = new PactString();
    private Pattern recordTextPattern = Pattern.compile(",\"(.+)\",");
    private Pattern datePattern = Pattern.compile(",\"(2[0-9]{3}-[0-9]{2}-[0-9]{2}\\W[0-9]{2}).+\"$");

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) {
        line.setValue(record.getField(0, PactString.class));
        String tweetText = line.toString();
        String tweetTimestamp = "";
        String[] splittedLine = tweetText.split(",");
        Matcher matchRecordText = recordTextPattern.matcher(tweetText);
        Matcher matchRecordDate = datePattern.matcher(tweetText);


        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Integer.parseInt(splittedLine[1]));


        if (matchRecordText.find()) {
            tweetText = matchRecordText.group(1);
        } else {
            tweetText = "";

        }
        tweet.setValue(tweetText);

        numWords.setValue(StringUtils.numWords(tweetText));

        if (matchRecordDate.find()) {
            tweetTimestamp = matchRecordDate.group(1).replace(' ', '-');

        } else {

            tweetTimestamp = "";
        }
        timestampH.setValue(tweetTimestamp);
        

        this.outputRecord.setField(0, tid);
        this.outputRecord.setField(1, uid);
        this.outputRecord.setField(2, tweet);
        this.outputRecord.setField(3, numWords);
        this.outputRecord.setField(4, timestampH);
        collector.collect(this.outputRecord);
    }
}