package benchmark;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class QueryEntry {

    public enum QueryTag {
        Q1(1), Q2(2),Q3(3),Q4(4),DUP(5), NA(6) ;

        public int numVal;

        QueryTag(int numVal) {
            this.numVal = numVal;
        }

    }
    public String queryString;
    public Set<QueryTag> tags = new HashSet<>();

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (!(o instanceof QueryEntry)) return false;
//        QueryEntry that = (QueryEntry) o;
//        return Objects.equals(queryString, that.queryString);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(queryString);
//    }
}
