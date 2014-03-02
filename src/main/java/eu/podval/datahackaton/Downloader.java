package eu.podval.datahackaton;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URL;

public class Downloader {
    private static final Log logger = LogFactory.getLog(Downloader.class);
    public static final String FlightRadarUrl = "http://www.flightradar24.com/zones/full_all.json";

    public static void main(String[] args) {
        Validate.isTrue(args.length == 1, "You must supply at least count of downloads.");

        JsonFactory factory = new JsonFactory();
        Jedis jedis = null;//new Jedis("mpaphsv100.hpswlabs.adapps.hp.com");
        int downloadsCount = Integer.parseInt(args[0]);

        try {
            for (int i = 0; i < downloadsCount; i++) {
                logger.error("Starting to parse a json, attempt: " + i);
                parseAndPutToRedis(factory, jedis);
                try {
                    Thread.sleep(8000);
                } catch (InterruptedException e) {
                    logger.error("Thread was interrupted.", e);
                }
            }
        } finally {
            if (jedis != null) {
                jedis.quit();
            }
        }
    }

    private static void parseAndPutToRedis(JsonFactory factory, Jedis jedis) {
        int planeCount = 0;
        try {
//            JsonParser parser = factory.createJsonParser(new File("c:\\Data\\Downloads\\full_all.json"));
            JsonParser parser = factory.createJsonParser(new URL(FlightRadarUrl));
            parser.nextToken();

            while (parser.nextToken() != JsonToken.END_OBJECT) {

                String planeNumber = parser.getText();
                logger.error("Processing plane: " + planeNumber);

                if (parser.nextToken() == JsonToken.START_ARRAY) {
                    parser.nextToken();
                    String hex = parser.getText();

                    parser.nextToken();
                    float lat = parser.getFloatValue();

                    parser.nextToken();
                    float lng = parser.getFloatValue();

                    parser.nextToken();
                    int aircraftTrack = parser.getIntValue();

                    parser.nextToken();
                    int altitude = parser.getIntValue();

                    parser.nextToken();
                    int speed = parser.getIntValue();

                    putToRedis(jedis, planeNumber, lat, lng, aircraftTrack, altitude, speed);
                    planeCount++;

                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        // Skip the rest of all items
                    }
                } else {
                    logger.error("Other property: " + parser.getText());
                }
            }
        } catch (IOException e) {
            logger.error("Can't process a json.", e);
        }
        logger.error(planeCount + " planes were inserted.");
    }

    private static void putToRedis(Jedis jedis, String planeNumber, float lat, float lng, int aircraftTrack, int altitude, int speed) {
        Long occurrence = jedis.incr("cnt:" + planeNumber);
        jedis.set("plane:" + planeNumber + ":lat:" + occurrence, Float.toString(lat));
        jedis.set("plane:" + planeNumber + ":lng:" + occurrence, Float.toString(lng));
        jedis.set("plane:" + planeNumber + ":track:" + occurrence, Integer.toString(aircraftTrack));
        jedis.set("plane:" + planeNumber + ":alt:" + occurrence, Integer.toString(altitude));
        jedis.set("plane:" + planeNumber + ":speed:" + occurrence, Integer.toString(speed));

        jedis.sadd("planes", planeNumber);
    }
}
