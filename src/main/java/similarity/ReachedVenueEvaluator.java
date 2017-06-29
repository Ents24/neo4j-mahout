package similarity;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

/**
 * Created by markwood on 26/06/2017.
 */
public class ReachedVenueEvaluator implements Evaluator {

    private Node venue;
    private boolean useProxyTracks;

    public ReachedVenueEvaluator (Node venue, boolean useProxyTracks) {
        this.venue = venue;
        this.useProxyTracks = useProxyTracks;
    }

    @Override
    public Evaluation evaluate(Path path) {

        Node lastNode = path.endNode();

        if (path.length() == 0) {
            if (lastNode.hasLabel(Label.label("User"))) {
                //System.out.println("User found - OK");
                return Evaluation.EXCLUDE_AND_CONTINUE;
            } else {
                //System.out.println("User found - OK");
                return Evaluation.EXCLUDE_AND_PRUNE;
            }
        } else if (path.length() == 1) {
            // if it's not a directly tracked venue
            if ( ! lastNode.equals(this.venue) &&
                    (lastNode.hasLabel(Label.label("Venue")) ||
                            ( ! useProxyTracks && lastNode.hasLabel(Label.label("Event")))
                    )
            ) {
                //System.out.println("We've found a tracked Venue (or Event in no-proxy mode) - OK " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                return Evaluation.EXCLUDE_AND_CONTINUE;
            } else {
                //System.out.println("No tracked Venues (or Events) found - aborting...");
                return Evaluation.EXCLUDE_AND_PRUNE;
            }
        } else if (path.length() <= 3) {
            if (path.lastRelationship().isType(MyRelationshipTypes.HOSTS)) {
                if (lastNode.equals(this.venue)) {
                    // we got to the venue via a tracked event - exclude
                    //System.out.println("Oh... we found the venue we want, but it hosts a tracked event :/ aborting for now!");
                    return Evaluation.EXCLUDE_AND_PRUNE;
                } else {
                    //System.out.println("OK, we found a 'proxy venue', let's continue " + lastNode.getLabels() + " " + lastNode.getProperty("id"));
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }
            } else if (lastNode.equals(this.venue)) {
                //System.out.println("Success - we got to the destination venue!");
                return Evaluation.INCLUDE_AND_PRUNE;
            } else {
                //System.out.println("Nope, not the right venue. Ditch this path");
                return Evaluation.EXCLUDE_AND_PRUNE;
            }
        }

        //System.out.println("Nope, not the right venue AND we're outside of acceptable logic, Jim");
        return Evaluation.EXCLUDE_AND_PRUNE;
    }
}
