package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */

    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // Get store tag
    private String getTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        System.out.println("Store category: " + cate + ", tag: " + tag);
        return tag;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        //Initialize store tags set
        initSets();

        //Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder
     * and save data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                int userId = (Integer) mapResult.get("userId");
                Set<String> initialTags = calculateUserTags(
                        (Integer) mapResult.get("blood_sugar"),
                        (Integer) mapResult.get("mood"),
                        (Integer) mapResult.get("stress"),
                        (Integer) mapResult.get("active")
                );
                mapResult.put("tags", new ArrayList<>(initialTags));

                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            Map<String, Object> eventMessage = (Map<String, Object>) envelope.getMessage();
            String type = (String) eventMessage.get("type");

            if ("RIDER_STATUS".equals(type)) {
                handleRiderStatus(eventMessage);
            } else if ("RIDER_INTEREST".equals(type)) {
                handleRiderInterest(eventMessage);
            } else if ("RIDE_REQUEST".equals(type)) {
                handleAdMatch(eventMessage, collector);
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void handleRiderStatus(Map<String, Object> event) {
        // Update user status in userInfo store
        int userId = (Integer) event.get("userId");
        Map<String, Object> user = userInfo.get(userId);
        if (user == null) return;

        int mood = (Integer) event.get("mood");
        int bloodSugar = (Integer) event.get("blood_sugar");
        int stress = (Integer) event.get("stress");
        int active = (Integer) event.get("active");

        user.put("mood", mood);
        user.put("blood_sugar", bloodSugar);
        user.put("stress", stress);
        user.put("active", active);

        // Update user tags
        Set<String> tags = new HashSet<>();
        tags = calculateUserTags(bloodSugar, mood, stress, active);
        user.put("tags", new ArrayList<>(tags));
        userInfo.put(userId, user);

    }

    private void handleRiderInterest(Map<String, Object> event) {
        // Update user interest in userInfo store
        int duration = (Integer) event.get("duration");
        if (duration > 5 * 60 * 1000) { // update only > 5 mins
            int userId = (Integer) event.get("userId");
            Map<String, Object> user = userInfo.get(userId);
            if (user == null) return;

            String interest = (String) event.get("interest");
            user.put("interest", interest);
            userInfo.put(userId, user);

        }
    }

    private void handleAdMatch(Map<String, Object> event, MessageCollector collector) {
        // Get user info from userInfo store
        // Get store info from yelpInfo store
        // Match the ad and send out the matched ad through collector
        int userId = (Integer) event.get("clientId");
        double userLat = (Double) event.get("latitude");
        double userLon = (Double) event.get("longitude");

        Map<String, Object> user = userInfo.get(userId);
        if (user == null) return;
        List<String> userTags = (List<String>) user.get("tags");
        if (userTags == null) userTags = Arrays.asList("others");

        String device = (String) user.get("device");
        String userInterest = (String) user.get("interest");
        int age = (Integer) user.get("age");
        int travelCount = (Integer) user.get("travel_count");

        double maxScore = -1.0;
        String bestStoreId = null;
        String bestStoreName = null;

        // match ads using for loop
        for (KeyValueIterator<String, Map<String, Object>> it = yelpInfo.all(); it.hasNext(); ) {
                Entry<String, Map<String, Object>> entry = it.next();
                Map<String, Object> store = entry.getValue();
                String storeTag = (String) store.get("tag");

                if (userTags.contains(storeTag)) {
                    double score = calculateScore(userInterest, device, userLat, userLon, age, travelCount, store);
                    if (score > maxScore) {
                        maxScore = score;
                        bestStoreId = (String) store.get("storeId");
                        bestStoreName = (String) store.get("name");
                    }
                }
        }

        if (bestStoreId != null) {
            Map<String, Object> adMatchResult = new HashMap<>();
            adMatchResult.put("userId", userId);
            adMatchResult.put("storeId", bestStoreId);
            adMatchResult.put("name", bestStoreName);

            collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, adMatchResult));
        }

    }

    private double calculateScore(String userInterest, String device, double userLat, double userLon,
                                  int age, int travelCount, Map<String, Object> store) {

        int reviewCount = (Integer) store.get("review_count");
        double rating = (Double) store.get("rating");
        double score = reviewCount * rating;

        if (store.get("categories").equals(userInterest)) {
            score += 10;
        }

        int deviceVal = getDeviceValue(device);
        int priceVal = getPriceValue((String) store.get("price"));
        score = score * (1 - Math.abs(priceVal - deviceVal) * 0.1);

        double sLat = (Double) store.get("latitude");
        double sLon = (Double) store.get("longitude");
        double dist = distance(userLat, userLon, sLat, sLon);
        double threshold = (age == 20 || travelCount > 50) ? 10.0 : 5.0;
        if (dist > threshold) {
            score = score * 0.1;
        }
        return score;
    }

    private int getDeviceValue(String device) {
        if ("iPhone XS".equals(device)) return 3;
        if ("iPhone 7".equals(device)) return 2;
        if ("iPhone 5".equals(device)) return 1;
        return 0;
    }

    private int getPriceValue(String price) {
        if (price == null) return 0;
        if (price.equals("$$$$") || price.equals("$$$")) return 3;
        if (price.equals("$$")) return 2;
        if (price.equals("$")) return 1;
        return 0;
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            return (dist);
        }
    }
    private Set<String> calculateUserTags(int bloodSugar, int mood, int stress, int active) {
        Set<String> tags = new HashSet<>();

        if (bloodSugar > 4 && mood > 6 && active == 3) tags.add("lowCalories");
        if (bloodSugar < 2 || mood < 4) tags.add("energyProviders");
        if (active == 3) tags.add("willingTour");
        if (stress > 5 || active == 1 || mood < 4) tags.add("stressRelease");
        if (mood > 6) tags.add("happyChoice");

        // others
        if (tags.isEmpty()) {
            tags.add("others");
        }

        return tags;
    }
}
