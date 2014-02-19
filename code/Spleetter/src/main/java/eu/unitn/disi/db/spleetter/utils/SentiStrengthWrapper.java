/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.utils;


import java.io.File;
import java.net.URL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.wlv.sentistrength.SentiStrength;

/**
 * Wrapper for the SentiStrength library
 *
 */
public class SentiStrengthWrapper {

    private SentiStrength classifier;
    private static String sentiDataFolder = "/tmp/EXPORT/SentiStrength_Data/"; // file:// ?
    private static final Log LOG = LogFactory.getLog(SentiStrengthWrapper.class);


    public static void setSentiStrengthData(String dirPath) {
        if (dirPath != null && !dirPath.isEmpty()) {
            //sentiDataFolder = dirPath;
            LOG.fatal( "Changed Sentiment Directory to: " + SentiStrengthWrapper.sentiDataFolder);
        }
    }

    /**
     * The unique instance *
     */
    private volatile static SentiStrengthWrapper instance;

    /**
     * The private constructor *
     */
    private SentiStrengthWrapper() {
        String[] args = {"sentidata", sentiDataFolder}; //, "text", "i  hate you. I really hate you"};
        LOG.fatal( "Instantiating Sentiment Classifier with Sentiment Directory to: " + SentiStrengthWrapper.sentiDataFolder);

        this.classifier = new SentiStrength();
        //LOG.fatal(classifier.computeSentimentScores("i hate hate you") );
        this.classifier.initialise(args);
        LOG.fatal(classifier.computeSentimentScores("i hate hate you") );
    }

    public static SentiStrengthWrapper getInstance() {
        if (instance == null) {
            synchronized (SentiStrengthWrapper.class) {
                if (instance == null) {
                    File dirFile = null;
                    try{
                        dirFile = new File(new URL(SentiStrengthWrapper.sentiDataFolder).toURI());
                        dirFile.canRead();
                    } catch(Exception e){
                        LOG.fatal( "Error acessing: " + SentiStrengthWrapper.sentiDataFolder + " err:" + e.getMessage() );
                    }

                    if (dirFile.isDirectory()) {
                        instance = new SentiStrengthWrapper();
                    } else {
                        LOG.fatal( "Error acessing: " + SentiStrengthWrapper.sentiDataFolder + " is not Directory");
                    }

                }
            }
        }
        return instance;
    }




    public double[] analyze(String text) {
        double[] polarities = new double[2];

        String result = this.classifier.computeSentimentScores(text);
        String[] tokens = result.split(" ");
        polarities[0] = Double.parseDouble(tokens[1]);
        polarities[1] = Double.parseDouble(tokens[0]);
        return polarities;
    }
}
