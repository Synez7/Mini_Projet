package dictionary;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class Dictionary {

    // Initialisation d'une BidiMap nous permettant d'avoir un accès à une donnée dans les deux sens (Integer --> String et String --> Integer)
    public BidiMap<Integer,String> dictionary = new DualHashBidiMap<>();

    public Dictionary() {}


    public Dictionary(String filePath) throws IOException {
        parseData(filePath);
    }


    // Méthode de parsing d'un fichier contenant des données RDF
    public void parseData(String filePath) throws IOException {

        int i = 1; // clé du dictionnaire, incrémenté à chaque ajout d'un élément

        InputStream targetStream = new FileInputStream(filePath);

        try {

            GraphQueryResult res = QueryResults.parseGraphBackground(targetStream, null, RDFFormat.NTRIPLES);

            while (res.hasNext()) {
                Statement st = res.next();
                String s = st.getSubject().stringValue();
                if(!dictionary.containsKey(s)){
                    dictionary.put(i++,s); // ajout d'un sujet au dictionnaire
                }
                String p = st.getPredicate().stringValue();
                if(!dictionary.containsKey(p)){
                    dictionary.put(i++,p); // ajout d'un prédicat au dictionnaire
                }
                String o = st.getObject().stringValue();
                if(!dictionary.containsKey(o)){
                    dictionary.put(i++,o); // ajout d'un objet au dictionnaire
                }
            }


        } catch (Exception e) {
            System.out.println("An error occured while parsing file :\n\t> " + e.getMessage());
        } finally {
            targetStream.close();
        }
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for(Map.Entry<Integer, String> entry : dictionary.entrySet()) {
            builder.append("<")
                    .append(entry.getKey())
                    .append(",")
                    .append(entry.getValue())
                    .append(">\n");
        }

        return builder.toString();
    }


    public static void main(String[] args) throws IOException {
        Dictionary d = new Dictionary("data/sample_data.nt");
        System.out.println(d);

    }



}
