package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.StringUtils;

/**
 * Converts a PactRecord containing one string in to multiple string/integer pairs.
 * The string is tokenized by whitespaces. For each token a new record is emitted,
 * where the token is the first field and an Integer(1) is the second field.
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class TokenizeLineMap extends MapStub {
   // initialize reusable mutable objects
   private final PactRecord outputRecord = new PactRecord();
   private final PactString line = new PactString(); 
   private final PactLong tid = new PactLong();
   private final PactInteger uid = new PactInteger();
   private final PactString tweet = new PactString();
   private final PactInteger numWords = new PactInteger();

   @Override
   public void map(PactRecord record, Collector<PactRecord> collector) {
       line.setValue(record.getField(0, PactString.class));
       String[] splittedLine = line.toString().split(",");

       tid.setValue(Long.valueOf(splittedLine[0]));
       uid.setValue(Integer.parseInt(splittedLine[1]));
       tweet.setValue(splittedLine[2]);
       numWords.setValue(StringUtils.numWords(splittedLine[2]));

       this.outputRecord.setField(0, tid);
       this.outputRecord.setField(1, uid);
       this.outputRecord.setField(2, tweet);
       this.outputRecord.setField(3, numWords);
       collector.collect(this.outputRecord);
   }
}