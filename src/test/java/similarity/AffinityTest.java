package similarity;

import apoc.coll.Coll;
import apoc.scoring.Scoring;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

/**
 * Created by markwood on 23/06/2017.
 */
public class AffinityTest {
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure( Affinity.class )
            .withFunction( Similarity.class )
            .withFunction( Scoring.class )
            .withFunction( Coll.class );

    @Test
    public void shouldAddVenueAffinityRelationshipUsingProxyTracks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            session.run( "MATCH (u:User {id: 1}), (v:Venue {id: 1}) CALL similarity.calculateVenueAffinity(u, v, true) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(v:Venue {id: 1}) RETURN aff");

            checkVenueAssertions(result.single().get("aff").asMap());
        }
    }

    @Test
    public void shouldAddVenueAffinityRelationshipWithoutProxyTracks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            session.run( "MATCH (u:User {id: 1}), (v:Venue {id: 1}) CALL similarity.calculateVenueAffinity(u, v, false) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(v:Venue {id: 1}) RETURN aff");

            checkVenueAssertions(result.single().get("aff").asMap());
        }
    }

    @Test
    public void shouldAddAllVenueAffinityRelationshipCypher() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            session.run( "MATCH (u:User {id: 1}) CALL similarity.calculateAllVenueAffinity(u) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(v:Venue {id: 1}) RETURN aff");

            Map affinityProperties = result.single().get("aff").asMap();

            checkVenueAssertions(affinityProperties, true);
            assertThat(affinityProperties.get("distanceNorm"), equalTo(0.9695162061105322));
            assertThat(affinityProperties.get("affinity"), equalTo(0.8500000000000001));
        }
    }

    @Test
    public void shouldAddAllArtistAffinityRelationshipCypher() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            session.run( "MATCH (u:User {id: 1}) CALL similarity.calculateAllArtistAffinity(u) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(a:Artist {id: 1}) RETURN aff");

            Map affinityProperties = result.single().get("aff").asMap();

            checkAssertions(affinityProperties);
            assertThat(affinityProperties.get("affinity"), equalTo(0.5184000000000001));
        }
    }

    @Test
    public void shouldFallBackToPopularityWhenNoLinks() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            session.run( "MATCH (u:User {id: 1}) " +
                    "CALL similarity.calculateAllVenueAffinity(u) " +
                    "CALL similarity.calculateAllArtistAffinity(u) " +
                    "CALL similarity.calculateAllEventAffinity(u) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(e:Event:Future {id: 4}) RETURN aff");

            Map affinityProperties = result.single().get("aff").asMap();

            assertThat(affinityProperties.get("artistAffinity"), equalTo(0.9999972477966368));
            assertThat(affinityProperties.get("venueAffinity"), equalTo(0.9999972477966368));
            assertThat(affinityProperties.get("affinity"), equalTo(0.9999999999924254));
        }
    }

    @Test
    public void shouldAddAllEventAffinityRelationshipCypher() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            session.run( "MATCH (u:User {id: 1}) " +
                    "CALL similarity.calculateAllVenueAffinity(u) " +
                    "CALL similarity.calculateAllArtistAffinity(u) " +
                    "CALL similarity.calculateAllEventAffinity(u) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(e:Event:Future {id: 3}) RETURN aff");

            Map affinityProperties = result.single().get("aff").asMap();

            assertThat(affinityProperties.get("artistAffinity"), equalTo(0.5184000000000001));
            assertThat(affinityProperties.get("venueAffinity"), equalTo(0.8500000000000001));
            assertThat(affinityProperties.get("affinity"), equalTo(0.92776));
        }
    }

    @Test
    public void nullUserShouldNotThrowException() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            setupGraph(session);

            // When I use the index procedure to index a node
            StatementResult result = session.run( "OPTIONAL MATCH (u:User {id: \"DUMMY\"}) " +
                    "CALL similarity.addProxyTracks(u) " +
                    "CALL similarity.calculateAllVenueAffinity(u) " +
                    "CALL similarity.calculateAllArtistAffinity(u) " +
                    "CALL similarity.calculateAllEventAffinity(u) " +
                    "RETURN u");

            // check null user returned
            assertTrue(result.single().get("u").getClass().equals(NullValue.class));
        }
    }

    private void setupGraph(Session session) {
        // Retreive the user node
        session.run( "CREATE (u:User {id: 1, latitude: 51.093965, longitude: -3.011673}), " +
                "(v2:Venue {id: 2}), " +
                "(v3:Venue {id: 3}), " +
                "(v4:Venue {id: 4}), " +
                "(v5:Venue {id: 5}), " +
                "(v6:Venue {id: 6, trackers: 500}), " +
                "(a2:Artist {id: 2}), " +
                "(a3:Artist {id: 3}), " +
                "(a4:Artist {id: 4}), " +
                "(a5:Artist {id: 5}), " +
                "(a6:Artist {id: 6, trackers: 1000}), " +
                "(e1:Event {id: 1}), " +
                "(e2:Event {id: 2}), " +
                "(e3:Event:Future {id: 3}), " +
                "(e4:Event:Future {id: 4}) " +
                "MERGE (u)-[:TRACKS]->(v2) " +
                "MERGE (u)-[:TRACKS]->(v3) " +
                "MERGE (u)-[:TRACKS]->(a2) " +
                "MERGE (u)-[:TRACKS]->(a3) " +
                "MERGE (u)-[:TRACKS]->(e1) " +
                "MERGE (u)-[:TRACKS]->(e2) " +
                "MERGE (v4)-[:HOSTS]->(e1) " +
                "MERGE (v5)-[:HOSTS]->(e2) " +
                "MERGE (v6)-[:HOSTS]->(e4) " +
                "MERGE (a4)-[:PLAYS {roster: 0}]->(e1) " +
                "MERGE (a5)-[:PLAYS {roster: 0}]->(e2) " +
                "MERGE (a6)-[:PLAYS {roster: 0}]->(e4)");

        session.run( "CREATE (v:Venue:Active {id: 1, latitude: 51.46072, longitude: -2.609646}) " +
                "WITH v " +
                "MATCH (v2:Venue {id: 2}), (v3:Venue {id: 3}), (v4:Venue {id: 4}), (v5:Venue {id: 5}), (e3:Event {id: 3}) " +
                "MERGE (v)-[:SIMILAR_TO {NMID: 0.9}]-(v2) " +
                "MERGE (v)-[:SIMILAR_TO {NMID: 0.8}]-(v3) " +
                "MERGE (v)-[:SIMILAR_TO {NMID: 0.7}]-(v4) " +
                "MERGE (v)-[:SIMILAR_TO {NMID: 0.6}]-(v5) " +
                "MERGE (v)-[:HOSTS]->(e3)");

        session.run( "CREATE (a:Artist:Active {id: 1}) " +
                "WITH a " +
                "MATCH (a2:Artist {id: 2}), (a3:Artist {id: 3}), (a4:Artist {id: 4}), (a5:Artist {id: 5}), (e3:Event {id: 3}) " +
                "MERGE (a)-[:SIMILAR_TO {NMID: 0.9}]-(a2) " +
                "MERGE (a)-[:SIMILAR_TO {NMID: 0.8}]-(a3) " +
                "MERGE (a)-[:SIMILAR_TO {NMID: 0.7}]-(a4) " +
                "MERGE (a)-[:SIMILAR_TO {NMID: 0.6}]-(a5) " +
                "MERGE (a)-[:PLAYS {roster: 0}]->(e3)");

        // add proxy tracking edges
        session.run("MATCH (u:User {id: 1})\n" +
                "CALL similarity.addProxyTracks(u) " +
                "CALL similarity.addPopularityNorm('Artist', 5000, 50000) " +
                "CALL similarity.addPopularityNorm('Venue', 2500, 25000) " +
                "RETURN u");
    }

    private void checkVenueAssertions(Map affinityProperties)
    {
        checkVenueAssertions(affinityProperties, false);
    }

    private void checkVenueAssertions(Map affinityProperties, boolean useNativeDistanceFunctions)
    {
        long distance;
        if (useNativeDistanceFunctions) {
            distance = 49503;
        } else {
            distance = 49448;
        }
        assertThat(Math.round((double) affinityProperties.get("distanceM")), equalTo(distance));
        this.checkAssertions(affinityProperties);
    }

    private void checkAssertions(Map affinityProperties)
    {
        assertThat(affinityProperties.get("min"), equalTo(0.8));
        assertThat(affinityProperties.get("avg"), equalTo(0.8500000000000001));
        assertThat(affinityProperties.get("count"), equalTo(2l));
        assertThat(affinityProperties.get("prod"), equalTo(0.7200000000000001));
        assertThat(affinityProperties.get("proxyMin"), equalTo(0.6));
        assertThat(affinityProperties.get("proxyAvg"), equalTo(0.6499999999999999));
        assertThat(affinityProperties.get("proxyCount"), equalTo(2l));
        assertThat(affinityProperties.get("proxyProd"), equalTo(0.42));
    }

    @Test
    public void shouldNotAddRelationshipToUnrelatedVenue() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            // Retreive the user node
            session.run( "CREATE (u:User {id: 1}), (v:Venue {id: 1}) " +
                    "WITH u, v " +
                    "MATCH (u:User {id: 1}), (v:Venue {id: 1}) CALL similarity.calculateVenueAffinity(u, v, true) " +
                    "RETURN u");

            StatementResult result = session.run("MATCH (u:User {id: 1})-[aff:HAS_AFFINITY_FOR]->(v:Venue {id: 1}) RETURN aff");

            // check that no relationship has been created
            assertFalse(result.hasNext());
        }
    }
}
