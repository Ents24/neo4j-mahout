package similarity;

import org.neo4j.graphdb.RelationshipType;

/**
 * Created by markwood on 23/06/2017.
 */
enum MyRelationshipTypes implements RelationshipType
{
    HAS_AFFINITY_FOR, TRACKS, PROXY_TRACKS, SIMILAR_TO, HOSTS
}