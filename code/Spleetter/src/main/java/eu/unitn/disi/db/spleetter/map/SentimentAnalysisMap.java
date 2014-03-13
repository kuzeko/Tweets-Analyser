package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.SentiStrengthWrapper;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import weka.core.logging.Logger;
import weka.core.logging.Logger;

/**
 * Analyze the polairties from the tweet text, appends negative and positive
 * polarities to the end of the record
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - negative polarity<br />
 * 3 - positive polarity<br />
 */
@FunctionAnnotation.ConstantFields({0, 1})
public class SentimentAnalysisMap extends MapFunction {

    private long counter = 0;
    private String sentimentDirPath = "";
    private StringValue tweet = new StringValue();
    private DoubleValue negPolarity = new DoubleValue();
    private DoubleValue posPolarity = new DoubleValue();
    private Record pr2 = new Record(4);
    private static final Log LOG = LogFactory.getLog(SentimentAnalysisMap.class);

    /**
     * Reads the filter literals from the configuration.
     *
     * @see
     * eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
     */
    @Override
    public void open(Configuration parameters) {
        this.sentimentDirPath = parameters.getString(TweetCleanse.SENTIMENT_PATH, "/tmp/SentiStrength_Data/");
        SentiStrengthWrapper.setSentiStrengthData(this.sentimentDirPath);
    }

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        tweet = pr.getField(2, StringValue.class);
        String text = tweet.getValue();

//        System.out.print(" acessing: " + this.sentimentDirPath );

        SentiStrengthWrapper analyzer = SentiStrengthWrapper.getInstance();

//        System.out.println("Current relative path is: " + s);
//        System.err.println("Current relative path is: " + s);
//        Logger.log(Logger.Level.SEVERE, "XYZ Current relative path is: " + s);

        if (analyzer != null && text != null) {
            double[] polarities = analyzer.analyze(text);
            // negPolarity.setValue(-Math.random()*5);
            // posPolarity.setValue(Math.random()*5);

            negPolarity.setValue(polarities[0]);
            posPolarity.setValue(polarities[1]);

            pr2.setField(0, pr.getField(0, LongValue.class));
            pr2.setField(1, pr.getField(1, LongValue.class));
            pr2.setField(2, negPolarity);
            pr2.setField(3, posPolarity);
            records.collect(pr2);
            if (TweetCleanse.SentimentAnalysisMapLog) {
                this.counter++;
            }


        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.SentimentAnalysisMapLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
