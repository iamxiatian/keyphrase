package ruc.irm.extractor.keyword.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 词图的边
 */
public class WordEdge {
    private String src;
    private String dest;
    private int count;
    private int score = -1;
    private Map<String, WordNode> nodeCache = new HashMap<String, WordNode>();

    public static double sum(Collection coll) {
        double total = 0;
        for (Object n : coll) {
            if (n instanceof Double)
                total += (Double) n;
            else if (n instanceof Integer)
                total += (Integer) n;
            else {
                //
            }
        }
        return total + 1; //加1平滑
    }
//
//    private double sum(Collection<Double> coll) {
//        double total = 0;
//        for (Double n : coll) {
//            total += n;
//        }
//        return total;
//    }

    public static double getEntropy(Collection<Integer> coll) {
        double total = sum(coll);
        List<Double> distribution = coll.stream().map(c -> c * 1.0 / total).collect(Collectors.toList());
        double entropy = 0.0;
        for (Double p : distribution) {
            entropy += -p * Math.log(p) / Math.log(2);
        }

        //归一化
        //entropy = entropy / (Math.log(coll.size())/Math.log(2));
        return entropy;
    }

    private int getEdgeCount() {
        int total = 0;
        for (WordNode node : nodeCache.values()) {
            for (int n : node.getAdjacentWords().values()) {
                total += n;
            }
        }
        return total / 2;
    }

    public int getScore() {
        if (score == -1) {
            //计算边的权重
            Collection<Integer> leftNeighbors = nodeCache.get(src).getLeftNeighbors().values();
            Collection<Integer> rightNeighbors = nodeCache.get(dest).getRightNeighbors().values();
//            double leftIn = sum(leftNeighbors); //加1平滑
//            double rightOut = sum(rightNeighbors);

            double leftOut = sum(nodeCache.get(src).getRightNeighbors().values()); //加1平滑
            double rightIn = sum(nodeCache.get(dest).getLeftNeighbors().values());


            double leftEntropy = 0;
            double rightEntropy = 0;
            if (leftNeighbors.size() > 0) {
                leftEntropy = getEntropy(leftNeighbors);
            }
            if (rightNeighbors.size() > 0) {
                rightEntropy = getEntropy(rightNeighbors);
            }

            double norm = Math.log(getEdgeCount()) / Math.log(2);
            double entropy = (leftEntropy + rightEntropy);
//            double entropy = Math.log(leftEntropy)/Math.log(2) + Math.log(rightEntropy)/Math.log(2);
            double jiehedu = (3 * Math.log(count) - Math.log(leftOut) - Math.log(rightIn)) - Math.log(src.length()) / Math.log(2) - Math.log(dest.length()) / Math.log(2);
//            System.out.println(src + "->" + dest + ":\t combine: " + jiehedu + "norm: " + norm + ", entropy: " + entropy + ", after:" + (entropy / norm));
            double value = jiehedu + entropy;
            //double value = (Math.log(leftIn) + Math.log(rightOut)) + leftEntropy + rightEntropy;
            score = (int) (value * 1000);
        }
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public WordEdge(Map<String, WordNode> nodeCache, String src, String dest, int count) {
        this.nodeCache = nodeCache;
        this.src = src;
        this.dest = dest;
        this.count = count;
    }

//    @Override
//    public int compareTo(WordEdge wordEdge) {
////        int a = wordEdge.src.compareTo(src);
////        return a == 0 ? wordEdge.dest.compareTo(dest) : a;
//        return wordEdge.count - count;
//    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WordEdge) {
            WordEdge o = (WordEdge) obj;
            return o.src.equals(src) && o.dest.equals(dest);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (this.src + "-" + this.dest).hashCode();
    }

    @Override
    public String toString() {
        return "WordEdge{" +
                "src='" + src + '\'' +
                ", dest='" + dest + '\'' +
                ", count=" + count +
                ", score=" + score +
                '}';
    }
}
