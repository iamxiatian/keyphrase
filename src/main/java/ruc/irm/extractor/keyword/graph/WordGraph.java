package ruc.irm.extractor.keyword.graph;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhinang.conf.Configuration;
import org.zhinang.util.ds.KeyValuePair;
import ruc.irm.extractor.commons.ChineseStopKeywords;
import ruc.irm.extractor.keyword.PhraseWeight;
import ruc.irm.extractor.keyword.RankGraph;
import ruc.irm.extractor.nlp.SegWord;
import ruc.irm.extractor.nlp.Segment;
import ruc.irm.extractor.nlp.SegmentFactory;
import ruc.irm.extractor.util.MathUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 关键词词图的基类，目前有如下实现：
 * <p>
 * 词语位置加权的词图实现WeightedPositionWordGraph, 参考：夏天. 词语位置加权TextRank的关键词抽取研究. 现代图书情报技术, 2013, 29(9): 30-34.
 *
 * @author 夏天
 * @organization 中国人民大学信息资源管理学院
 */
public abstract class WordGraph {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Segment segment = null;

    //是否在后面的词语中加上指向前面词语的链接关系
    protected boolean linkBack = true;

    /**
     * 如果读取的单词数量超过该值，则不再处理以后的内容，以避免文本过长导致计算速度过慢
     */
    private int maxReadableWordCount = 1000;// Integer.MAX_VALUE;

    /**
     * 读取的词语的数量
     */
    private int readWordCount = 0;

    protected Map<String, WordNode> wordNodeMap = new HashMap<String, WordNode>();

    public WordGraph() {
        this.segment = SegmentFactory.getSegment(new Configuration());
    }

    public Map<String, WordNode> getWordNodeMap() {
        return wordNodeMap;
    }

    public WordNode getWordNode(String word) {
        return wordNodeMap.get(word);
    }

    /**
     * 直接通过传入的词语和重要性列表构建关键词图
     *
     * @param wordsWithImportance
     */
    public void build(List<KeyValuePair<String, Double>> wordsWithImportance) {
        int lastPosition = -1;
        for (int i = 0; i < wordsWithImportance.size(); i++) {
            String word = wordsWithImportance.get(i).getKey();
            double importance = wordsWithImportance.get(i).getValue();

            WordNode wordNode = wordNodeMap.get(word);

            //如果已经读取了最大允许的单词数量，则忽略后续的内容
            readWordCount++;
            if (readWordCount > maxReadableWordCount) {
                return;
            }

            if (wordNode == null) {
                //如果额外指定了权重，则使用额外指定的权重代替函数传入的权重
                double specifiedWeight = PhraseWeight.getWeight(word, 0.0f);
                if (specifiedWeight < importance) {
                    specifiedWeight = importance;
                }
                wordNode = new WordNode(word, "IGNORE", 0, specifiedWeight);
                wordNodeMap.put(word, wordNode);
            } else if (wordNode.getImportance() < importance) {
                wordNode.setImportance(importance);
            }

            wordNode.setCount(wordNode.getCount() + 1);

            //加入邻接点
            if (lastPosition >= 0) {
                WordNode lastWordNode = wordNodeMap.get(wordsWithImportance.get(lastPosition).getKey());
                lastWordNode.addAdjacentWord(word);

                if (linkBack) {
                    //加入逆向链接
                    wordNode.addAdjacentWord(lastWordNode.getName());
                }

                if (lastPosition == i - 1) {
                    if (wordNode.getPos().startsWith("n") &&
                            (lastWordNode.getPos().equals("adj") || lastWordNode.getPos().startsWith("n"))) {
                        wordNode.addLeftNeighbor(lastWordNode.getName());
                        lastWordNode.addRightNeighbor(wordNode.getName());
                    }
                }
            }
            lastPosition = i;
        }
    }

    public Map<String, Integer> getLeftNeighbors(String word) {
        return wordNodeMap.get(word).getLeftNeighbors();
    }

    public Map<String, Integer> getRightNeighbors(String word) {
        return wordNodeMap.get(word).getRightNeighbors();
    }

    public void build(String text, float importance) {
        List<SegWord> words = segment.tag(text);

        int lastPosition = -1;
        for (int i = 0; i < words.size(); i++) {
            SegWord segWord = words.get(i);

            if ("w".equalsIgnoreCase(segWord.pos) || "null".equalsIgnoreCase(segWord.pos)) {
                continue;
            }

            if (segWord.word.length() >= 2 && (segWord.pos.startsWith("n") || segWord.pos.endsWith("n") || segWord.pos.startsWith("adj") || segWord.pos.startsWith("v"))) {
                WordNode wordNode = wordNodeMap.get(segWord.word);

                //如果已经读取了最大允许的单词数量，则忽略后续的内容
                readWordCount++;
                if (readWordCount > maxReadableWordCount) {
                    return;
                }

                if (LOG.isDebugEnabled()) {
                    System.out.print(segWord.word + "/" + segWord.pos + " ");
                }

                if (wordNode == null) {
                    //如果额外指定了权重，则使用额外指定的权重代替函数传入的权重
                    float specifiedWeight = PhraseWeight.getWeight(segWord.word, 0.0f);

                    if (specifiedWeight < importance) {
                        specifiedWeight = importance;
                    }

                    if (segWord.pos.equals("ns") || segWord.equals("nr") || segWord.equals("nz")) {
                        specifiedWeight = specifiedWeight * 1.3f;
                    } else if (segWord.pos.startsWith("v")) {
                        specifiedWeight *= 0.5f;
                    }
                    wordNode = new WordNode(segWord.word, segWord.pos, 0, specifiedWeight);
                    wordNodeMap.put(segWord.word, wordNode);
                } else if (wordNode.getImportance() < importance) {
                    wordNode.setImportance(importance);
                }

                wordNode.setCount(wordNode.getCount() + 1);

                //加入邻接点
                if (lastPosition >= 0) {
                    WordNode lastWordNode = wordNodeMap.get(words.get(lastPosition).word);
                    lastWordNode.addAdjacentWord(segWord.word);

                    if (linkBack) {
                        //加入逆向链接
                        wordNode.addAdjacentWord(lastWordNode.getName());
                    }

                    if (lastPosition == i - 1) {
//                        wordNode.addLeftNeighbor(lastWordNode.getName());
//                        lastWordNode.addRightNeighbor(wordNode.getName());
//                        if (wordNode.getPos().startsWith("n") &&
//                                (lastWordNode.getPos().equals("adj") ||
//                                        lastWordNode.getPos().startsWith("n") ||
//                                        lastWordNode.getPos().endsWith("n")
//                                )
//                        ) {
                        if (wordNode.getPos().startsWith("n") || wordNode.getPos().endsWith("n")) {
                            wordNode.addLeftNeighbor(lastWordNode.getName());
                            lastWordNode.addRightNeighbor(wordNode.getName());
                        }
                    }
                }
                lastPosition = i;
            }

//            if(segWord.features.equals("PU")) {
//                lastPosition = -1; //以句子为单位
//            }
        }

        if (LOG.isDebugEnabled()) {
            System.out.println();
        }

    }

    /**
     * 该步处理用于删除过多的WordNode，仅保留出现频次在前100的词语，以便加快处理速度
     */
    void shrink() {

    }

    protected abstract RankGraph makeRankGraph();

    public List<String> findTopKeywords(int topN, boolean findPhrase) {
        RankGraph g = makeRankGraph();
        g.iterateCalculation(20, 0.85f);
        g.quickSort();

        //如果不找短语，直接返回
        if (!findPhrase) {
            List<String> keywords = new LinkedList<>();

            int count = 0;
            for (int i = 0; i < g.labels.length && count < topN; i++) {
                String word = g.labels[i];
                if (!ChineseStopKeywords.isStopKeyword(word)) {
                    keywords.add(word);
                    count++;
                }
            }
            return keywords;
        }

        //把计算得分保存到词图中
        for (int i = 0; i < g.labels.length; i++) {
            String word = g.labels[i];
            getWordNode(word).setScore(g.V[i]);
        }

        Set<WordEdge> edges = new HashSet<>();

        //计算所有的节点对，并排序
        for (WordNode node : wordNodeMap.values()) {
            for (Map.Entry<String, Integer> entry : node.getLeftNeighbors().entrySet()) {
                WordEdge edge = new WordEdge(wordNodeMap, entry.getKey(), node.getName(), entry.getValue());
                edges.add(edge);
            }

            for (Map.Entry<String, Integer> entry : node.getRightNeighbors().entrySet()) {
                WordEdge edge = new WordEdge(wordNodeMap, node.getName(), entry.getKey(), entry.getValue());
                edges.add(edge);
            }
        }

        List<WordEdge> sortedEdges = Lists.newArrayList(edges);
        sortedEdges.sort(new Comparator<WordEdge>() {
            @Override
            public int compare(WordEdge e1, WordEdge e2) {
                return e2.getScore() - e1.getScore();
            }
        });


        List<String> keywords = new LinkedList<>();

//        int count = 0;
//        int limit = topN;
//        for (int i = 0; i < g.labels.length && count < limit; i++) {
//            String word = g.labels[i];
//            if (!ChineseStopKeywords.isStopKeyword(word)) {
//                keywords.add(word);
//                count++;
//            }
//        }

        //最少取10个结果
        int limit = Math.max(topN, 10);
        int count = 0;
        for (int i = 0; i < g.labels.length && count < limit; i++) {
            String word = g.labels[i];
            if (!ChineseStopKeywords.isStopKeyword(word)) {
                keywords.add(word);
                count++;
            }
        }

        List<BiWordPhrase> biPhrases = new ArrayList<>();
        for (int i = 0; i < sortedEdges.size() && i < limit; i++) {
            WordEdge edge = sortedEdges.get(i);
            if (keywords.contains(edge.getSrc()) && keywords.contains(edge.getDest())) {
                BiWordPhrase phrase = new BiWordPhrase(edge.getSrc(), edge.getDest(), i + 1);
                double score = (i + 1) + MathUtil.log2(keywords.indexOf(edge.getSrc()) + 1) + MathUtil.log2(keywords.indexOf(edge.getDest()) + 1);
                phrase.score = score/3;
                biPhrases.add(phrase);
            }
        }

        List<SingleWordPhrase> singleWordPhrases = new ArrayList<>();
        for (int i = 0; i < keywords.size(); i++) {
            String keyword = keywords.get(i);
            SingleWordPhrase phrase = new SingleWordPhrase(keyword, i + 1);
            double score = phrase.rank; //默认分值为当前排序
            for (int j = 0; j < biPhrases.size(); j++) {
                BiWordPhrase p = biPhrases.get(j);
                if (p.src.equals(keyword) || p.dest.equals(keyword)) {
                    //所在短语越重要，则该词语越不重要，成反比关系
                    //考虑短语长度，越长的短语，对词语权重调整影响越小。如黄南州/图书馆
                    double value = MathUtil.log2(limit - j); //分值加上所在短语的分值（分值越大，越不重要）
                    //if(score<value) score = value;
                    score += value;
                }
            }

            //score = score + phrase.rank;
            phrase.score = score;
            singleWordPhrases.add(phrase);
        }

        List<Phrase> results = new ArrayList<>();
        results.addAll(biPhrases);
        results.addAll(singleWordPhrases);

        Collections.sort(results, new Comparator<Phrase>() {
            @Override
            public int compare(Phrase phrase, Phrase t1) {
                if (phrase.score == t1.score) return 0;
                else if (phrase.score > t1.score) return 1;
                else return -1;
            }
        });

//        results.forEach(p -> {
//            System.out.println(p.getLabel() + "/" + p.score);
//        });

        return results.stream().map(p -> p.getLabel()).limit(topN).collect(Collectors.toList());
    }


    private int getNodeScore(String word) {
        //计算边的权重
        Collection<Integer> leftNeighbors = wordNodeMap.get(word).getLeftNeighbors().values();
        Collection<Integer> rightNeighbors = wordNodeMap.get(word).getRightNeighbors().values();

        int virtualEdgeCount = Math.min(leftNeighbors.size(), leftNeighbors.size());
        if (virtualEdgeCount == 0) virtualEdgeCount = 1;

        double leftIn = WordEdge.sum(leftNeighbors); //加1平滑
        double rightOut = WordEdge.sum(rightNeighbors);

        double leftEntropy = 0;
        double rightEntropy = 0;
        if (leftNeighbors.size() > 0) {
            leftEntropy = WordEdge.getEntropy(leftNeighbors);
        }
        if (rightNeighbors.size() > 0) {
            rightEntropy = WordEdge.getEntropy(rightNeighbors);
        }

        double value = (3 * Math.log(virtualEdgeCount) - Math.log(leftIn) - Math.log(rightOut)) + leftEntropy + rightEntropy;
        return (int) (value * 1000);
    }

    /**
     * 设置最大可以读取的词语数量
     *
     * @param maxReadableWordCount
     */
    public void setMaxReadableWordCount(int maxReadableWordCount) {
        this.maxReadableWordCount = maxReadableWordCount;
    }
}
