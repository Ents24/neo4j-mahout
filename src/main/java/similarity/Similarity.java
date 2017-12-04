package similarity;

import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.LoglikelihoodSimilarity;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;

/**
 * This is an example how you can create a simple user-defined function for Neo4j.
 */
public class Similarity
{
    @UserFunction
    @Description("similarity.LLR(AB, A, B, total) - return the log-likelihood ratio of A wrt B")
    public double LLR(
            @Name("both") long AB,
            @Name("all A") long A,
            @Name("all B") long B,
            @Name("total") long total) {

        return org.apache.mahout.math.stats.LogLikelihood.logLikelihoodRatio(AB, A-AB, B-AB, total-A-B+AB);
    }

    @UserFunction
    @Description("similarity.LLSimilarity(AB, A, B, total) - return the log likelihood similarity of A and B")
    public double LLSimilarity(
            @Name("both") long AB,
            @Name("all A") long A,
            @Name("all B") long B,
            @Name("total") long total) {

        LoglikelihoodSimilarity lls = new LoglikelihoodSimilarity();

        return lls.similarity(AB, A, B, (int) total);
    }

    @UserFunction
    @Description("similarity.LLDistance(AB, A, B, total) - return the log likelihood distance between A and B")
    public double LLDistance(
            @Name("both") long AB,
            @Name("all A") long A,
            @Name("all B") long B,
            @Name("total") long total) {

        return 1.0 - LLSimilarity(AB, A, B, total);
    }

    @UserFunction
    @Description("similarity.mutualInformation(AB, A, B, total) - return the mutual information of A and B")
    public double mutualInformation(
            @Name("both") long AB,
            @Name("all A") long A,
            @Name("all B") long B,
            @Name("total") long total) {

        // LLR = 2 * N * MI
        // MI  = LLR / 2 * N

        return this.LLR(AB, A, B, total) / (2.0 * total);
    }

    @UserFunction
    @Description("similarity.NMID(AB, A, B, total) - return the normalised mutual information distance between A and B")
    public double NMID(
            @Name("both") long AB,
            @Name("all A") long A,
            @Name("all B") long B,
            @Name("total") long total) {

        // NMID = 1 - MI / H

        double normalisedJointEntropy = org.apache.mahout.math.stats.LogLikelihood.entropy(AB, A-AB, B-AB, total-A-B+AB) / total;

        return 1.0 - (this.mutualInformation(AB, A, B, total) / normalisedJointEntropy);
    }

    @UserFunction
    @Description("similarity.product([0.5, 0.4, 0.2]) = 0.04 - return the product of the entries in a list")
    public double product(@Name("numbers") List<Number> list) {
        double product = 1;
        for (Number number : list) {
            product *= number.doubleValue();
        }
        return product;
    }

    @UserFunction
    @Description("similarity.complementProduct([0.5, 0.4, 0.2]) = 1 - ((1 - 0.5) * (1 - 0.4) * (1 - 0.2)) = " +
            "1 - (0.5 * 0.6 * 0.8) = 0.76 - return the complement of the product of the complement of the " +
            "entries in a list")
    public double complementProduct(@Name("numbers") List<Number> list) {
        double product = 1;
        for (Number number : list) {
            product *= (1d - number.doubleValue());
        }
        return (1d - product);
    }

    @UserFunction
    @Description("similarity.sublist([1, 2, 3, 4, 5], 0, 2) = [1, 2] - return a sublist of the input list")
    public List sublist(@Name("list") List list, @Name("index start") Number fromIndex, @Name("number of elements") Number count) {
        Number toIndex = fromIndex.intValue() + count.intValue();
        if (toIndex.intValue() > list.size()) {
            toIndex = list.size();
        }
        return list.subList(fromIndex.intValue(), toIndex.intValue());
    }
}