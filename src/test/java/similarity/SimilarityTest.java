package similarity;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SimilarityTest
{
    // This rule starts a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the function we want to test
            .withFunction( Similarity.class );

    @Test
    public void LLRCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.LLR(1, 4, 3, 11) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.016502205534052905 ) );

            // When - no trackers in common (actually with this size data set, that counter-intuitively contains more information
            // and the test above!
            result = session.run( "RETURN similarity.LLR(0, 3, 2, 10) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 1.632274230570168 ) );

            // When - all trackers in common
            result = session.run( "RETURN similarity.LLR(3, 3, 3, 8) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 10.58501181052771 ) );
        }
    }

    @Test
    public void LLSimilarityCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.LLSimilarity(1, 4, 3, 11) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.016234303717406084 ) );

            // When - no trackers in common (actually with this size data set, that counter-intuitively contains more information
            // and the test above!
            result = session.run( "RETURN similarity.LLSimilarity(0, 3, 2, 10) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.6201003723751862 ) );

            // When - all trackers in common
            result = session.run( "RETURN similarity.LLSimilarity(3, 3, 3, 8) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.9136815726772705 ) );
        }
    }

    @Test
    public void LLDistanceCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.LLDistance(1, 4, 3, 11) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.9837656962825939 ) );

            // When - no trackers in common (actually with this size data set, that counter-intuitively contains more information
            // and the test above!
            result = session.run( "RETURN similarity.LLDistance(0, 3, 2, 10) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.3798996276248138 ) );

            // When - all trackers in common
            result = session.run( "RETURN similarity.LLDistance(3, 3, 3, 8) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.08631842732272954 ) );
        }
    }

    @Test
    public void mutualInformationCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.mutualInformation(1, 4, 3, 11) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 7.501002515478593E-4 ) );

            // When - no trackers in common (actually with this size data set, that counter-intuitively contains more information
            // and the test above!
            result = session.run( "RETURN similarity.mutualInformation(0, 3, 2, 10) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.08161371152850841 ) );

            // When - all trackers in common
            result = session.run( "RETURN similarity.mutualInformation(3, 3, 3, 8) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.6615632381579819 ) );
        }
    }

    @Test
    public void NMIDCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.NMID(1, 4, 3, 11) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.999395414082041 ) );

            // When - no trackers in common (actually with this size data set, that counter-intuitively contains more information
            // and the test above!
            result = session.run( "RETURN similarity.NMID(0, 3, 2, 10) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.9207366846756104 ) );

            // When - all trackers in common
            result = session.run( "RETURN similarity.NMID(3, 3, 3, 8) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.0 ) );
        }
    }

    @Test
    public void productCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.product([0.5, 0.4, 0.2]) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.04000000000000001 ) );
        }
    }

    @Test
    public void productComplementCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            double result;

            // When - some trackers in common
            result = session.run( "RETURN similarity.complementProduct([0.5, 0.4, 0.2]) AS result").single().get("result").asDouble();

            // Then
            assertThat( result, equalTo( 0.76 ) );
        }
    }

    @Test
    public void sublistCorrect() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();
            List result;

            // Sublist at start
            result = session.run( "RETURN similarity.sublist([1, 2, 3, 4, 5], 0, 2) AS result").single().get("result").asList();

            assertTrue(result.size() == 2);
            assertTrue(result.get(0).equals(1l));
            assertTrue(result.get(1).equals(2l));

            // Sublist in the middle
            result = session.run( "RETURN similarity.sublist([1, 2, 3, 4, 5], 2, 3) AS result").single().get("result").asList();

            assertTrue(result.size() == 3);
            assertTrue(result.get(0).equals(3l));
            assertTrue(result.get(1).equals(4l));
            assertTrue(result.get(2).equals(5l));

            // Sublist from start but too long
            result = session.run( "RETURN similarity.sublist([1, 2, 3, 4, 5], 0, 10) AS result").single().get("result").asList();

            assertTrue(result.size() == 5);
            assertTrue(result.get(0).equals(1l));
            assertTrue(result.get(1).equals(2l));
            assertTrue(result.get(2).equals(3l));
            assertTrue(result.get(3).equals(4l));
            assertTrue(result.get(4).equals(5l));

            // Sublist from middle but too long
            result = session.run( "RETURN similarity.sublist([1, 2, 3, 'hello', 5], 3, 10) AS result").single().get("result").asList();

            assertTrue(result.size() == 2);
            assertTrue(result.get(0).equals("hello"));
            assertTrue(result.get(1).equals(5l));
        }
    }
}