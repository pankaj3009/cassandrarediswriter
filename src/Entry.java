/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.jquantlib.time.BusinessDayConvention;
import org.jquantlib.time.DateParser;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanResult;
import org.jquantlib.time.Date;
import org.jquantlib.time.calendars.India;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.response.QueryResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 *
 * @author Pankaj
 */
public class Entry {

    private static final Logger logger = Logger.getLogger(Entry.class.getName());
    public static String FutureMetric = "india.nse.future.s4.daily";
    public static String EquityMetric = "india.nse.equity.s4.daily";
    public static String OptionMetric = "india.nse.option.s4.daily";
    public static JedisPool symbolDB;
    public static India ind = new India();
    public static String timeZone;

    public static void main(String[] args) {
        try {
            if (args.length == 1) {
                Properties globalProperties = loadParameters(args[0]);
                timeZone = globalProperties.getProperty("timezone", "Asia/Kolkata").toString().trim();
                String holidayFile = globalProperties.getProperty("holidayfile", "").toString().trim();
                if (holidayFile != null && !holidayFile.equals("")) {
                    File inputFile = new File(holidayFile);
                    if (inputFile.exists() && !inputFile.isDirectory()) {
                        try {
                            List<String> holidays = Files.readAllLines(Paths.get(holidayFile), StandardCharsets.UTF_8);
                            for (String h : holidays) {
                                ind.addHoliday(new Date(getFormattedDate(h, "yyyyMMdd", timeZone)));
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "No Holiday File Found");
                        }
                    }
                }
                String kairosIP = globalProperties.getProperty("kairosip", "127.0.0.1");
                int kairosPort = Integer.valueOf(globalProperties.getProperty("kairosport", "8085"));
                FutureMetric = globalProperties.getProperty("futuremetric");
                EquityMetric = globalProperties.getProperty("equitymetric");
                OptionMetric = globalProperties.getProperty("optionmetric");
                String redisIP = globalProperties.getProperty("redisip", "127.0.0.1");
                int redisPort = Integer.valueOf(globalProperties.getProperty("redisport", "6379"));
                int redisSymbolDB = Integer.valueOf(globalProperties.getProperty("redissymbolsdb", "2"));
                int redisWriteDB = Integer.valueOf(globalProperties.getProperty("rediswritedb", "9"));
                symbolDB = new JedisPool(new JedisPoolConfig(), redisIP, redisPort, 2000, null, redisSymbolDB);
                JedisPool dataDB = new JedisPool(new JedisPoolConfig(), redisIP, redisPort, 2000, null, redisWriteDB);
                //Get list of all symbols
                ArrayList<String> symbols = loadAllSymbols();
                //calculate prior business day and get settle for that date
                String today = getFormattedDate(new java.util.Date().getTime(), "yyyy-MM-dd", timeZone);
                String priorBusinessDate = getPriorBusinessDay(today, "yyyy-MM-dd");
                java.util.Date startingDate = getFormattedDate(priorBusinessDate, "yyyy-MM-dd", timeZone);
                //Get Settle for the equity symbols
                Jedis jedis = dataDB.getResource();
                for (String s : symbols) {
                    HashMap<Long, String> prices = getPricesFromKDB(kairosIP, kairosPort, s, null, null, null, startingDate, new java.util.Date(), EquityMetric + ".settle");
                    for (Map.Entry<Long, String> price : prices.entrySet()) {
                        //write to redis
                        jedis.zadd(s + "_STK___" + ":daily:settle", price.getKey(), new Pair(price.getKey(), price.getValue()).getJson());
                    }
                }

                //Get Settle for all futures
                for (String s : symbols) {
                    List<String> expiries = getExpiriesFromKDB(kairosIP, kairosPort, s, startingDate, new java.util.Date(), FutureMetric + ".settle");
                    if (expiries != null) {
                        for (String expiry : expiries) {
                            HashMap<Long, String> prices = getPricesFromKDB(kairosIP, kairosPort, s, expiry, null, null, startingDate, new java.util.Date(), FutureMetric + ".settle");
                            for (Map.Entry<Long, String> price : prices.entrySet()) {
                                //write to redis
                                jedis.zadd(s + "_FUT_" + expiry + "__" + ":daily:settle", price.getKey(), new Pair(price.getKey(), price.getValue()).getJson());
                            }
                        }
                    }
                }

                //Get Settle for all options
                int i = 0;
                for (String s : symbols) {
                    i++;
                    List<String> expiries = getExpiriesFromKDB(kairosIP, kairosPort, s, startingDate, new java.util.Date(), OptionMetric + ".settle");
                    System.out.println(s);
                    if (expiries != null) {
                        for (String expiry : expiries) {
                            List<String> strikes = getOptionStrikesFromKDB(kairosIP, kairosPort, s, expiry, startingDate, new java.util.Date(), OptionMetric + ".settle");
                            if (strikes != null) {
                                for (String strike : strikes) {
                                    System.out.println(i + ":" + symbols.size() + ":" + s + ":" + expiry + ":" + strike);
                                    HashMap<Long, String> prices = getPricesFromKDB(kairosIP, kairosPort, s, expiry, "PUT", strike, startingDate, new java.util.Date(), OptionMetric + ".settle");
                                    for (Map.Entry<Long, String> price : prices.entrySet()) {
                                        //write to redis
                                        jedis.zadd(s + "_OPT_" + expiry + "_PUT_" + strike + ":daily:settle", price.getKey(), new Pair(price.getKey(), price.getValue()).getJson());

                                    }
                                    prices = getPricesFromKDB(kairosIP, kairosPort, s, expiry, "CALL", strike, startingDate, new java.util.Date(), OptionMetric + ".settle");
                                    for (Map.Entry<Long, String> price : prices.entrySet()) {
                                        //write to redis
                                        jedis.zadd(s + "_OPT_" + expiry + "_CALL_" + strike + ":daily:settle", price.getKey(), new Pair(price.getKey(), price.getValue()).getJson());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    public static HashMap<Long, String> getPricesFromKDB(String cassandraIP, int kairosPort, String exchangeSymbol, String expiry, String right, String optionStrike, java.util.Date startDate, java.util.Date endDate, String metric) {
        HashMap<Long, String> out = new HashMap<>();
        try {
            HttpClient client = new HttpClient("http://" + cassandraIP + ":" + kairosPort);
            String strike = formatDouble(getDouble(optionStrike, 0), new DecimalFormat("#.##"));
            QueryBuilder builder = QueryBuilder.getInstance();
            String symbol = null;
            symbol = exchangeSymbol.toLowerCase();
            builder.setStart(startDate)
                    .setEnd(endDate)
                    .addMetric(metric)
                    .addTag("symbol", symbol);
            if (expiry != null && !expiry.equals("")) {
                builder.getMetrics().get(0).addTag("expiry", expiry);
            }
            if (right != null && !right.equals("")) {
                builder.getMetrics().get(0).addTag("option", right);
                builder.getMetrics().get(0).addTag("strike", strike);
            }
            builder.getMetrics().get(0).setOrder(QueryMetric.Order.DESCENDING);
            long time = new java.util.Date().getTime();
            QueryResponse response = client.query(builder);

            List<DataPoint> dataPoints = response.getQueries().get(0).getResults().get(0).getDataPoints();
            for (DataPoint dataPoint : dataPoints) {
                long lastTime = dataPoint.getTimestamp();
                out.put(lastTime, dataPoint.getValue().toString());
            }
        } catch (Exception e) {
            logger.log(Level.INFO, null, e);
        }
        return out;
    }

    public static List<String> getExpiriesFromKDB(String cassandraIP, int kairosPort, String symbol, java.util.Date startTime, java.util.Date endTime, String metric) {
        List<String> out = new ArrayList<>();
        HashMap<String, Object> param = new HashMap();
        param.put("TYPE", Boolean.FALSE);
        String strike = null;
        String expiry = null;
        HistoricalRequestJson request = new HistoricalRequestJson(metric,
                new String[]{"symbol"},
                new String[]{symbol.toLowerCase()},
                null,
                null,
                null,
                String.valueOf(startTime.getTime()),
                String.valueOf(endTime.getTime()));
        //http://stackoverflow.com/questions/7181534/http-post-using-json-in-java
        //        String json_string = JsonWriter.objectToJson(request, param);
        Gson gson = new GsonBuilder().create();
        String json_string = gson.toJson(request);
        StringEntity requestEntity = new StringEntity(
                json_string,
                ContentType.APPLICATION_JSON);

        HttpPost postMethod = new HttpPost("http://" + cassandraIP + ":" + kairosPort + "/api/v1/datapoints/query/tags");
        postMethod.setEntity(requestEntity);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpResponse rawResponse = httpClient.execute(postMethod);
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((rawResponse.getEntity().getContent())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                JsonElement jelement = new JsonParser().parse(output);
                JsonObject jobject = jelement.getAsJsonObject();
                JsonArray jarray = jobject.getAsJsonArray("queries");
                jobject = jarray.get(0).getAsJsonObject();
                jarray = jobject.getAsJsonArray("results");
                jobject = jarray.get(0).getAsJsonObject();
                jobject = jobject.getAsJsonObject("tags");
                jarray = jobject.getAsJsonArray("expiry");
                Type listType = new TypeToken<List<String>>() {
                }.getType();
                out = new Gson().fromJson(jarray, listType);
                return out;
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
        return out;
    }

    public static List<String> getOptionStrikesFromKDB(String cassandraIP, int kairosPort, String symbol, String expiry, java.util.Date startTime, java.util.Date endTime, String metric) {
        List<String> out = new ArrayList<>();
        HashMap<String, Object> param = new HashMap();
        param.put("TYPE", Boolean.FALSE);
        HistoricalRequestJson request = new HistoricalRequestJson(metric,
                new String[]{"symbol", "expiry"},
                new String[]{symbol.toLowerCase(), expiry},
                null,
                null,
                null,
                String.valueOf(startTime.getTime()),
                String.valueOf(endTime.getTime()));
        //http://stackoverflow.com/questions/7181534/http-post-using-json-in-java
        Gson gson = new GsonBuilder().create();
        String json_string = gson.toJson(request);
        StringEntity requestEntity = new StringEntity(
                json_string,
                ContentType.APPLICATION_JSON);

        HttpPost postMethod = new HttpPost("http://" + cassandraIP + ":" + kairosPort + "/api/v1/datapoints/query/tags");
        postMethod.setEntity(requestEntity);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpResponse rawResponse = httpClient.execute(postMethod);
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((rawResponse.getEntity().getContent())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                JsonElement jelement = new JsonParser().parse(output);
                JsonObject jobject = jelement.getAsJsonObject();
                JsonArray jarray = jobject.getAsJsonArray("queries");
                jobject = jarray.get(0).getAsJsonObject();
                jarray = jobject.getAsJsonArray("results");
                jobject = jarray.get(0).getAsJsonObject();
                jobject = jobject.getAsJsonObject("tags");
                jarray = jobject.getAsJsonArray("strike");
                Type listType = new TypeToken<List<String>>() {
                }.getType();
                out = new Gson().fromJson(jarray, listType);
                return out;
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }


        return out;
    }

    public static String getPriorBusinessDay(String date, String outputFormat) {
        SimpleDateFormat sdfOutput = new SimpleDateFormat(outputFormat);
        Date today = DateParser.parseISO(date);
        Date yesterday = today.sub(1);
        yesterday = ind.adjust(yesterday, BusinessDayConvention.Preceding);
        String yesterdayString = (sdfOutput.format(yesterday.isoDate()));
        return yesterdayString;

    }

    public static String formatDouble(double d, DecimalFormat df) {
        return df.format(d);
    }

    public static boolean isDouble(String value) {
        //String decimalPattern = "([0-9]*)\\.([0-9]*)";  
        //return Pattern.matches(decimalPattern, value)||Pattern.matches("\\d*", value);
        if (value != null) {
            value = value.trim();
            return value.matches("-?\\d+(\\.\\d+)?");
        } else {
            return false;
        }
    }

    public static double getDouble(Object input, double defvalue) {
        try {
            if (isDouble(input.toString())) {
                return Double.parseDouble(input.toString().trim());
            } else {
                return defvalue;
            }
        } catch (Exception e) {
            return defvalue;
        }
    }

    public static ArrayList<String> loadAllSymbols() {
        ArrayList<String> out = new ArrayList<>();
        String cursor = "";
        String shortlistedkey = "";
        while (!cursor.equals("0")) {
            cursor = cursor.equals("") ? "0" : cursor;
            try (Jedis jedis = symbolDB.getResource()) {
                ScanResult s = jedis.scan(cursor);
                cursor = s.getCursor();
                for (Object key : s.getResult()) {
                    if (key.toString().contains("ibsymbols")) {
                        if (shortlistedkey.equals("")) {
                            shortlistedkey = key.toString();
                        } else {
                            int date = Integer.valueOf(shortlistedkey.split(":")[1]);
                            int newdate = Integer.valueOf(key.toString().split(":")[1]);
                            if (newdate > date) {
                                shortlistedkey = key.toString();//replace with latest nifty setup
                            }
                        }
                    }
                }
            }
        }
        Map<String, String> ibsymbols = new HashMap<>();
        try (Jedis jedis = symbolDB.getResource()) {
            ibsymbols = jedis.hgetAll(shortlistedkey);
            for (Map.Entry<String, String> entry : ibsymbols.entrySet()) {
                String exchangeSymbol = entry.getKey().trim().toUpperCase();
                out.add(exchangeSymbol);
            }
        
        }
        
        return out;
    }

    public static java.util.Date getFormattedDate(String date, String format, String timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
        java.util.Date d = new java.util.Date(0);
        try {
            d = sdf.parse(date);
        } catch (ParseException e) {
            logger.log(Level.SEVERE, null, e);
        }
        c.setTime(d);
        return c.getTime();
    }

    public static String getFormattedDate(long timeMS, String format, String timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        TimeZone tmz = TimeZone.getTimeZone(timeZone);
        sdf.setTimeZone(tmz);
        String date = sdf.format(new java.util.Date(timeMS));
        return date;
    }

    public static Properties loadParameters(String parameterFile) {
        Properties p = new Properties();
        FileInputStream propFile;
        File f = new File(parameterFile);
        if (f.exists()) {
            try {
                propFile = new FileInputStream(parameterFile);
                p.load(propFile);

            } catch (Exception ex) {
                logger.log(Level.INFO, "101", ex);
            }
        }

        return p;
    }
}
