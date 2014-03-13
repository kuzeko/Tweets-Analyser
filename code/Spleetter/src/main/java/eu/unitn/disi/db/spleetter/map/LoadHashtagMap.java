package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Converts a Record containing one string in to multiple integer pairs.
 * 0 - tweet id
 * 1 - user id
 * 2 - hashtag id
 */
@FunctionAnnotation.ConstantFields({})
public class LoadHashtagMap extends MapFunction{

    private static final Log LOG = LogFactory.getLog(LoadHashtagMap.class);
    private long counter = 0;


    private Record outputRecord = new Record();
    private LongValue tid = new LongValue();
    private LongValue uid = new LongValue();
    private IntValue hid = new IntValue();
    private final StringValue line = new StringValue();

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        line.setValue(pr.getField(0, StringValue.class));
        String[] splittedLine = line.toString().split(",");

        tid.setValue(Long.valueOf(splittedLine[0]));
        uid.setValue(Long.valueOf(splittedLine[1]));
        hid.setValue(Integer.parseInt(splittedLine[2]));

        outputRecord.setField(0, tid);
        outputRecord.setField(1, uid);
        outputRecord.setField(2, hid);

        records.collect(outputRecord);
        if (TweetCleanse.LoadHashtagMapLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.LoadHashtagMapLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
