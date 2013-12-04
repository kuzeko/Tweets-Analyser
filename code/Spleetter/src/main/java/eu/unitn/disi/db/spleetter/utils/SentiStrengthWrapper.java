/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.utils;


import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.wlv.sentistrength.SentiStrength;

/**
 * Wrapper for the SentiStrength library
 *
 */
public class SentiStrengthWrapper {

    private SentiStrength classifier;
    private static String sentiDataFolder = "/tmp/SentiStrength_Data/";
    private static final Log LOG = LogFactory.getLog(SentiStrengthWrapper.class);


    public static void setSentiStrengthData(String dirPath) {
        if (sentiDataFolder != null && !sentiDataFolder.isEmpty()) {
            sentiDataFolder = dirPath;
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
        String[] args = {"sentidata", sentiDataFolder, "text", "i don't hate you. I really hate you"};
        classifier = new SentiStrength(args);
    }

    public static SentiStrengthWrapper getInstance() {
        if (instance == null) {
            synchronized (SentiStrengthWrapper.class) {
                if (instance == null) {
                    if (new File(SentiStrengthWrapper.sentiDataFolder).isDirectory()) {
                        instance = new SentiStrengthWrapper();
                    } else {
                        LOG.fatal( "Error acessing: " + SentiStrengthWrapper.sentiDataFolder);
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
