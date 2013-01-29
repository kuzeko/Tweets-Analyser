package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a PactRecord containing one string in to multiple into a tweet record
 * containing the date of the tweet
 *
 * 0 - tweet id
 * 1 - user id
 * 2 - timestamp[h]
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class LoadTweetDatesMap extends MapStub {
    // initialize reusable mutable objects

    private final PactRecord outputRecord = new PactRecord(3);
    private final PactLong tid = new PactLong();
    private final PactInteger uid = new PactInteger();
    private final PactString timestampH = new PactString();
    private Pattern datePattern = Pattern.compile(",\"(2[0-9]{3}-[0-9]{2}-[0-9]{2}\\W[0-9]{2}).+\"$");

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) {
        String tweetText = record.getField(0, PactString.class).toString();
        String tweetTimestamp = "";
        String[] splittedLine = tweetText.split(",");
        Matcher matchRecordDate = datePattern.matcher(tweetText);

        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Integer.parseInt(splittedLine[1]));

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
    }
}