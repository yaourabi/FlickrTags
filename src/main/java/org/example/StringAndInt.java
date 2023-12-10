package org.example;

public class StringAndInt implements Comparable<StringAndInt>{
    int nbOccurence;
    String tag;

    public StringAndInt(String tag, int nbOccurence) {
        this.nbOccurence = nbOccurence;
        this.tag = tag;
    }

    public int getNbOccurence() {
        return nbOccurence;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public int compareTo(StringAndInt o) {
        return Integer.compare(o.nbOccurence, this.nbOccurence);
    }
}
