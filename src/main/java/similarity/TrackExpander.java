package similarity;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.Collections;

/**
 * Created by markwood on 26/06/2017.
 */
public class TrackExpander implements PathExpander {

    private boolean useProxyTracks;

    public TrackExpander(boolean useProxyTracks) {
        this.useProxyTracks = useProxyTracks;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState state) {
        Node lastNode = path.endNode();
        switch (path.length()) {
            case 0:
                if (useProxyTracks) {
                    //System.out.println("Traversing TRACKS and PROXY_TRACKS...");
                    return lastNode.getRelationships(Direction.OUTGOING, MyRelationshipTypes.TRACKS, MyRelationshipTypes.PROXY_TRACKS);
                } else {
                    //System.out.println("Traversing just TRACKS...");
                    return lastNode.getRelationships(Direction.OUTGOING, MyRelationshipTypes.TRACKS);
                }
            case 1:
                if (lastNode.hasLabel(Label.label("Venue"))) {
                    //System.out.println("Traversing SIMILAR_TO relationships from " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                    return lastNode.getRelationships(MyRelationshipTypes.SIMILAR_TO);
                } else if ( ! useProxyTracks && lastNode.hasLabel(Label.label("Event"))) {
                    //System.out.println("Traversing HOSTS relationships from " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                    return lastNode.getRelationships(MyRelationshipTypes.HOSTS, Direction.INCOMING);
                } else {
                    //System.out.println("NO CAN TRAVERSE - halting " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                    return Collections.emptyList();
                }
            case 2:
                if ( ! useProxyTracks && lastNode.hasLabel(Label.label("Venue"))) {
                    //System.out.println("Traversing SIMILAR_TO relationships from " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                    return lastNode.getRelationships(MyRelationshipTypes.SIMILAR_TO);
                } else {
                    //System.out.println("NO CAN TRAVERSE - halting " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                    return Collections.emptyList();
                }
            default:
                //System.out.println("NO CAN TRAVERSE - halting " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                return Collections.emptyList();
        }
    }

    @Override
    public PathExpander reverse() {
        return null;
    }
}
