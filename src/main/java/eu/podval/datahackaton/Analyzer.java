package eu.podval.datahackaton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.math.BigInteger;
import java.util.Set;

/**
 * java -cp ".;.\dependency\*;.\FlightRadarAnalyzer-1.0-SNAPSHOT.jar" eu.podval.datahackaton.Analyzer
 */
public class Analyzer {
    private static final Log logger = LogFactory.getLog(Analyzer.class);

    public static void main(String[] args) {
        Jedis jedis = new Jedis("mpaphsv100.hpswlabs.adapps.hp.com");

        try {
            Set<String> planes = jedis.smembers("planes");
            for (String planeNumber : planes) {
                int occurrence = 0;
                BigInteger altSum = BigInteger.ZERO;
                while (jedis.exists("plane:" + planeNumber + ":lat:" + occurrence)) {
                    float alt = Float.parseFloat(jedis.get("plane:" + planeNumber + ":alt:" + occurrence));
                    altSum.add(BigInteger.valueOf((int) alt));
                    occurrence++;
                }
                BigInteger avgAlt = altSum.divide(BigInteger.valueOf(occurrence + 1));
                logger.error("Average altitude for plane " + planeNumber + " is " + avgAlt
                        + " according to " + occurrence + " values.");
                jedis.set("plane:" + planeNumber + ":avgLat", avgAlt.toString());
            }
        } catch (Exception e) {
            logger.error("Processing failed.", e);
        } finally {
            jedis.quit();
        }
    }
}
