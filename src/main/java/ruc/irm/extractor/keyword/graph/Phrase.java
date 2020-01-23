package ruc.irm.extractor.keyword.graph;

public abstract class Phrase {
    public boolean valid = true;
    public int rank = 0;
    public double score = 0;

    public abstract String getLabel();
}

class BiWordPhrase extends Phrase {
    public String src, dest;

    public BiWordPhrase(String src, String dest, int rank) {
        this.src = src;
        this.dest = dest;
        this.rank = rank;
    }

    @Override
    public String getLabel() {
        return this.src + this.dest;
    }
}

class SingleWordPhrase extends Phrase {
    public String word;

    public SingleWordPhrase(String word, int rank) {
        this.word = word;
        this.rank = rank;
    }

    @Override
    public String getLabel() {
        return word;
    }
}