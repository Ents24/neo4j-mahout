package similarity;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by markwood on 23/06/2017.
 */
public class Affinity {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * Calculate the affinity of a user for all active venues, and then adds these relationships
     * to the graph
     *
     * @param user the user Node in question
     */
    @Procedure(value = "similarity.calculateAllVenueAffinity", mode = Mode.WRITE)
    @Description("Create an affinity edge between the given user and all active venues")
    public void calculateAllVenueAffinity( @Name("user") Node user)
    {
        if (user == null) {
            return;
        }

        Map<String, Object> params = new HashMap<>();
        params.put( "id", user.getProperty("id") );

        // calculate Cartesian distance contribution
        db.execute("MATCH (u:User {id: {id}})\n" +
                "WHERE exists(u.latitude) AND exists(u.longitude)\n" +
                "MATCH (v:Venue:Active)\n" +
                "WHERE exists(v.latitude) AND exists(v.longitude)\n" +
                "MERGE (u)-[aff:HAS_AFFINITY_FOR]->(v)\n" +
                "ON CREATE SET aff.distanceM    = distance(point(u), point(v)), \n" +
                "              aff.distanceNorm = 0.85 + (0.15 * (apoc.scoring.pareto(0, 50000, 200000, toInteger(aff.distanceM)) / 200000.0))", params);

        // calculate TRACKS contribution
        db.execute("MATCH (u:User {id: {id}})-[t:TRACKS]->(:Venue)-[s:SIMILAR_TO]-(b:Venue:Active)\n" +
                "WHERE not(exists((u)-[:TRACKS]->(b)))\n" +
                "WITH u, b, min(s.NMID) AS min, avg(s.NMID) AS avg, similarity.product(collect(s.NMID)) AS prod, count(s.NMID) AS count\n" +
                "MERGE (u)-[aff:HAS_AFFINITY_FOR]->(b)\n" +
                "SET aff.min = min, aff.avg = avg, aff.prod = prod, aff.count = count", params);

        // calculate PROXY_TRACKS contribution
        db.execute("MATCH (u:User {id: {id}})-[t:PROXY_TRACKS]->(:Venue)-[s:SIMILAR_TO]-(b:Venue:Active)\n" +
                "WHERE not(exists((u)-[:TRACKS]->(b)))\n" +
                "WITH u, b, min(s.NMID) AS min, avg(s.NMID) AS avg, similarity.product(collect(s.NMID)) AS prod, count(s.NMID) AS count\n" +
                "MERGE (u)-[aff:HAS_AFFINITY_FOR]->(b)\n" +
                "SET aff.proxyMin = min, aff.proxyAvg = avg, aff.proxyProd = prod, aff.proxyCount = count", params);

        // other contributions here (eg. PURCHASED, etc.)

        // calculate combined affinity score
        db.execute("MATCH (u:User {id: {id}})-[aff:HAS_AFFINITY_FOR]->(v:Venue:Active)\n" +
                "WITH aff, CASE WHEN aff.distanceNorm IS NOT NULL THEN aff.distanceNorm ELSE 1.0 END AS distance, \n" +
                    "CASE WHEN aff.avg IS NOT NULL THEN aff.avg WHEN aff.proxyAvg IS NOT NULL THEN aff.proxyAvg^0.1 ELSE 1.0 END AS similarity\n" +
                "WITH aff, apoc.coll.min([distance, similarity]) AS venueAffinity\n" +
                "WHERE venueAffinity <> 1.0\n" +
                "SET aff.affinity = venueAffinity", params);
    }

    /**
     * Calculate the affinity of a user for all active artists, and then adds these relationships
     * to the graph
     *
     * @param user the user Node in question
     */
    @Procedure(value = "similarity.calculateAllArtistAffinity", mode = Mode.WRITE)
    @Description("Create an affinity edge between the given user and all active artists")
    public void calculateAllArtistAffinity( @Name("user") Node user)
    {
        if (user == null) {
            return;
        }

        Map<String, Object> params = new HashMap<>();
        params.put( "id", user.getProperty("id") );

        // calculate TRACKS contribution
        db.execute("MATCH (u:User {id: {id}})-[t:TRACKS]->(:Artist)-[s:SIMILAR_TO]-(b:Artist:Active)\n" +
                "WHERE not(exists((u)-[:TRACKS]->(b)))\n" +
                "WITH u, b, min(s.NMID) AS min, avg(s.NMID) AS avg, similarity.product(collect(s.NMID)) AS prod, count(s.NMID) AS count\n" +
                "MERGE (u)-[aff:HAS_AFFINITY_FOR]->(b)\n" +
                "SET aff.min = min, aff.avg = avg, aff.prod = prod, aff.count = count", params);

        // calculate PROXY_TRACKS contribution
        db.execute("MATCH (u:User {id: {id}})-[t:PROXY_TRACKS]->(:Artist)-[s:SIMILAR_TO]-(b:Artist:Active)\n" +
                "WHERE not(exists((u)-[:TRACKS]->(b)))\n" +
                "WITH u, b, min(s.NMID) AS min, avg(s.NMID) AS avg, similarity.product(collect(s.NMID)) AS prod, count(s.NMID) AS count\n" +
                "MERGE (u)-[aff:HAS_AFFINITY_FOR]->(b)\n" +
                "SET aff.proxyMin = min, aff.proxyAvg = avg, aff.proxyProd = prod, aff.proxyCount = count", params);

        // other contributions here (eg. PURCHASED, etc.)

        // calculate combined affinity score
        db.execute("MATCH (u:User {id: {id}})-[aff:HAS_AFFINITY_FOR]->(a:Artist:Active)\n" +
                "WITH aff, CASE WHEN aff.prod IS NOT NULL THEN aff.prod^2 WHEN aff.proxyProd IS NOT NULL THEN aff.proxyProd^0.1 ELSE 1.0 END AS artistAffinity\n" +
                "WHERE artistAffinity <> 1.0\n" +
                "SET aff.affinity = artistAffinity", params);
    }

    /**
     * Calculate the affinity of a user for all active artists, and then adds these relationships
     * to the graph
     *
     * @param user the user Node in question
     */
    @Procedure(value = "similarity.calculateAllEventAffinity", mode = Mode.WRITE)
    @Description("Create an affinity edge between the given user and all future events")
    public void calculateAllEventAffinity( @Name("user") Node user)
    {
        if (user == null) {
            return;
        }

        Map<String, Object> params = new HashMap<>();
        params.put( "id", user.getProperty("id") );

        db.execute("MATCH (u:User {id: {id}}), (a)-[:PLAYS {roster: 0}]->(e:Future)<-[:HOSTS]-(v)\n" +
                "WHERE not(exists((u)-[:TRACKS]->(e))) AND not(exists((u)-[:TRACKS]->(a))) AND not(exists((u)-[:TRACKS]->(v)))\n" +
                "OPTIONAL MATCH (u)-[affV:HAS_AFFINITY_FOR]->(v)\n" +
                "OPTIONAL MATCH (u)-[affA:HAS_AFFINITY_FOR]->(a)\n" +
                "WITH u, v, a, e, affV, affA,\n" +
                "\tCASE WHEN affV.affinity IS NULL THEN v.popularityNorm ELSE affV.affinity END AS venueAffinity,\n" +
                "\tCASE WHEN affA.affinity IS NULL THEN a.popularityNorm ELSE affA.affinity END AS artistAffinity\n" +
                "WITH u, e, artistAffinity, venueAffinity, 1-((1-artistAffinity)*(1-venueAffinity)) AS affinity\n" +
                "MERGE (u)-[aff:HAS_AFFINITY_FOR]->(e)\n" +
                "SET aff.artistAffinity = artistAffinity, aff.venueAffinity = venueAffinity, aff.affinity = affinity", params);
    }

    /**
     * Add PROXY_TRACKS relationships for a given user; that is, a PROXY_TRACK relationship to any Artist that PLAYS
     * or Venue that HOSTS an Event which the user TRACKS
     *
     * @param user the user Node in question
     */
    @Procedure(value = "similarity.addProxyTracks", mode = Mode.WRITE)
    @Description("Create an affinity edge between the given user and all active venues")
    public void addProxyTracks( @Name("user") Node user) {

        if (user == null) {
            return;
        }

        Map<String, Object> params = new HashMap<>();
        params.put( "id", user.getProperty("id") );

        db.execute("MATCH (u:User {id: {id}})-[:TRACKS]->(e:Event)<-[:HOSTS]-(v:Venue)\n" +
                "OPTIONAL MATCH (e)<-[:PLAYS {roster: 0}]-(a:Artist)\n" +
                "WHERE not(exists((u)-[:TRACKS]->(v))) AND not(exists((u)-[:TRACKS]->(a)))\n" +
                "WITH u, collect(a) + collect(v) AS entities\n" +
                "FOREACH (e IN entities | MERGE (u)-[:PROXY_TRACKS {proxy: true}]->(e))", params);
    }

    /**
     * Add popularity norm values to all nodes of a given label (ie. Artist or Venue) according
     * to the input pareto thresholds
     *
     * @param label the Node label to update
     * @param eightyPercentValue the threshold at wish the number of trackers should reach 80% of pareto maximum
     * @param maximumValue the maximum pareto value
     */
    @Procedure(value = "similarity.addPopularityNorm", mode = Mode.WRITE)
    @Description("Create an affinity edge between the given user and all active venues")
    public void addPopularityNorm(@Name("label") String label,
                                  @Name("eightyPercentValue") long eightyPercentValue,
                                  @Name("maximumValue") long maximumValue) {

        Map<String, Object> params = new HashMap<>();
        params.put( "eightyPercentValue", eightyPercentValue );
        params.put( "maximumValue", maximumValue );
        params.put( "maximumValueDouble", (double) maximumValue );

        db.execute("MATCH (n:" + label + ")\n" +
                "SET n.popularityNorm = 0.99999 + (0.00001 * (1 - apoc.scoring.pareto(0, {eightyPercentValue}, {maximumValue}, CASE WHEN n.trackers IS NULL THEN 0 ELSE n.trackers END) / {maximumValueDouble}))", params);
    }


    /**
     * Calculate the affinity of a user for a venue and add the appropriate relationship
     * to the graph
     *
     * @param user the user Node in question
     * @param venue the venue Node for which we need to calculate affinity
     * @param useProxyTracks whether to use PROXY_TRACKS relationships (or calculate on the fly)
     */
    @Procedure(value = "similarity.calculateVenueAffinity", mode = Mode.WRITE)
    @Description("Create an affinity edge between the given user and venue")
    public void calculateVenueAffinity( @Name("user") Node user,
                                        @Name("venue") Node venue,
                                        @Name("useProxyTracks") boolean useProxyTracks)
    {
        // see if there's already a relationship there
        Iterable<Relationship> rels = user.getRelationships(MyRelationshipTypes.HAS_AFFINITY_FOR, Direction.OUTGOING);
        Relationship affinity = null;

        for (Relationship a : rels) {
            if (a.getEndNode().equals(venue)) {
                affinity = a;
                break;
            }
        }

        // this is a new relationship
        if (affinity == null && user.hasProperty("latitude") && user.hasProperty("longitude") && venue.hasProperty("latitude") && venue.hasProperty("longitude")) {
            affinity = user.createRelationshipTo(venue, MyRelationshipTypes.HAS_AFFINITY_FOR);
            double distanceM = distance(
                    (double) user.getProperty("latitude"),
                    (double) venue.getProperty("latitude"),
                    (double) user.getProperty("longitude"),
                    (double) venue.getProperty("longitude"),
                    0.0,
                    0.0
                );
            affinity.setProperty("distanceM", distanceM);
        }

        ReachedVenueEvaluator rve = new ReachedVenueEvaluator(venue, useProxyTracks);
        TrackExpander te = new TrackExpander(useProxyTracks);

        TraversalDescription similarToTracks = db.traversalDescription()
                .depthFirst()
                .expand(te)
                .evaluator(rve)
                .uniqueness(Uniqueness.RELATIONSHIP_PATH);

        double similarity;
        long   count = 0;
        double total = 0;
        double prod = 1.0;
        double min = 1.0;
        long   proxyCount = 0;
        double proxyTotal = 0;
        double proxyProd = 1.0;
        double proxyMin = 1.0;
        boolean isProxyPath;
        for (Path path : similarToTracks.traverse(user)) {
            if ( ! useProxyTracks) {
                isProxyPath = (path.length() > 2);
            } else {
                isProxyPath = false;
            }
            for (Relationship rel : path.relationships()) {
                if (useProxyTracks && rel.isType(MyRelationshipTypes.PROXY_TRACKS)) {
                    isProxyPath = true;
                }
                if (rel.isType(MyRelationshipTypes.SIMILAR_TO)) {
                    similarity = (double) rel.getProperty("NMID");
                    if ( ! isProxyPath) {
                        count++;
                        total += similarity;
                        prod *= similarity;
                        if (similarity < min) {
                            min = similarity;
                        }
                    } else {
                        proxyCount++;
                        proxyTotal += similarity;
                        proxyProd *= similarity;
                        if (similarity < proxyMin) {
                            proxyMin = similarity;
                        }
                    }
                }
            }
        }

        if (count > 0) {
            if (affinity == null) {
                affinity = user.createRelationshipTo(venue, MyRelationshipTypes.HAS_AFFINITY_FOR);
            }
            affinity.setProperty("min", min);
            affinity.setProperty("avg", total / count);
            affinity.setProperty("prod", prod);
            affinity.setProperty("count", count);
        }

        if (proxyCount > 0) {
            if (affinity == null) {
                affinity = user.createRelationshipTo(venue, MyRelationshipTypes.HAS_AFFINITY_FOR);
            }
            affinity.setProperty("proxyMin", proxyMin);
            affinity.setProperty("proxyAvg", proxyTotal / proxyCount);
            affinity.setProperty("proxyProd", proxyProd);
            affinity.setProperty("proxyCount", proxyCount);
        }
    }

    /**
     * Calculate distance between two points in latitude and longitude taking
     * into account height difference. If you are not interested in height
     * difference pass 0.0. Uses Haversine method as its base.
     *
     * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
     * el2 End altitude in meters
     * @returns Distance in Meters
     */
    private static double distance(double lat1, double lat2, double lon1,
                                  double lon2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return Math.sqrt(distance);
    }
}
