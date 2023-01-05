package indexes;

import dictionary.Dictionary;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Index {


    // Initialisation des 6 triplets d'Index (SPO, SOP, PSO, OPS, POS et OSP)
    public Map<Integer, Map<Integer, ArrayList<Integer>>> SPO = new HashMap<>(); // Triplet SPO
    public Map<Integer, Map<Integer, ArrayList<Integer>>> SOP = new HashMap<>(); // Triplet SOP
    public Map<Integer, Map<Integer, ArrayList<Integer>>> PSO = new HashMap<>(); // Triplet PSO
    public Map<Integer, Map<Integer, ArrayList<Integer>>> OPS = new HashMap<>(); // Triplet OPS
    public Map<Integer, Map<Integer, ArrayList<Integer>>> POS = new HashMap<>(); // Triplet POS
    public Map<Integer, Map<Integer, ArrayList<Integer>>> OSP = new HashMap<>(); // Triplet OSP



    public Index() {}

    public Index(Dictionary d, String filePath) throws IOException {
        parseData(d,filePath);
    }


    // Méthode de parsing d'un fichier contenant des données RDF
    private void parseData(Dictionary dictionary, String filePath) throws IOException {

        InputStream targetStream = new FileInputStream(filePath);

        try {

            GraphQueryResult res = QueryResults.parseGraphBackground(targetStream, null, RDFFormat.NTRIPLES);

            while (res.hasNext()) {
                Statement st = res.next();
                String s = st.getSubject().stringValue();
                String p = st.getPredicate().stringValue();
                String o = st.getObject().stringValue();

                // Construction de chaque triplet d'index via le dictionnaire construit auparavant
                addSPOTriple(dictionary.dictionary.getKey(s),dictionary.dictionary.getKey(p),dictionary.dictionary.getKey(o));

                addSOPTriple(dictionary.dictionary.getKey(s),dictionary.dictionary.getKey(o),dictionary.dictionary.getKey(p));

                addPSOTriple(dictionary.dictionary.getKey(p),dictionary.dictionary.getKey(s),dictionary.dictionary.getKey(o));

                addOPSTriple(dictionary.dictionary.getKey(o),dictionary.dictionary.getKey(p),dictionary.dictionary.getKey(s));

                addPOSTriple(dictionary.dictionary.getKey(p),dictionary.dictionary.getKey(o),dictionary.dictionary.getKey(s));

                addOSPTriple(dictionary.dictionary.getKey(o),dictionary.dictionary.getKey(s),dictionary.dictionary.getKey(p));

            }


        } catch (Exception e) {
            System.out.println("An error occured while parsing file :\n\t> " + e.getMessage());
        } finally {
            targetStream.close();
        }


    }

    // ----------------------------- Méthodes liées à la construction de chacun de nos triplets d'index -----------------------------
    public void addSPOTriple(Integer s, Integer p, Integer o) {
        if (SPO.containsKey(s)) {
            if (SPO.get(s).containsKey(p)) {
                if (!SPO.get(s).get(p).contains(o)) {
                    SPO.get(s).get(p).add(o);
                }
            } else {
                SPO.get(s).put(p, new ArrayList<Integer>(Arrays.asList(o)));
            }
        } else {
            Map<Integer, ArrayList<Integer>> t = new HashMap<Integer, ArrayList<Integer>>();
            t.put(p, new ArrayList<>(Arrays.asList(o)));
            SPO.put(s, t);
        }
    }


    public void addSOPTriple(Integer s, Integer o, Integer p) {
        if (SOP.containsKey(s)) {
            if (SOP.get(s).containsKey(o)) {
                if (!SOP.get(s).get(o).contains(p)) {
                    SOP.get(s).get(o).add(p);
                }
            } else {
                SOP.get(s).put(o, new ArrayList<Integer>(Arrays.asList(p)));
            }
        } else {
            Map<Integer, ArrayList<Integer>> t = new HashMap<Integer, ArrayList<Integer>>();
            t.put(o, new ArrayList<Integer>(Arrays.asList(p)));
            SOP.put(s, t);
        }
    }

    public void addPSOTriple(Integer p, Integer s, Integer o) {
        if (PSO.containsKey(p)) {
            if (PSO.get(p).containsKey(s)) {
                if (!PSO.get(p).get(s).contains(o)) {
                    PSO.get(p).get(s).add(o);
                }
            } else {
                PSO.get(p).put(s, new ArrayList<Integer>(Arrays.asList(o)));
            }
        } else {
            Map<Integer, ArrayList<Integer>> t = new HashMap<Integer, ArrayList<Integer>>();
            t.put(s, new ArrayList<Integer>(Arrays.asList(o)));
            PSO.put(p, t);
        }
    }

    public void addOPSTriple(Integer o, Integer p, Integer s) {
        if (OPS.containsKey(o)) {
            if (OPS.get(o).containsKey(p)) {
                if (!OPS.get(o).get(p).contains(s)) {
                    OPS.get(o).get(p).add(s);
                }
            } else {
                OPS.get(o).put(p, new ArrayList<Integer>(Arrays.asList(s)));
            }
        } else {
            Map<Integer, ArrayList<Integer>> t = new HashMap<Integer, ArrayList<Integer>>();
            t.put(p, new ArrayList<Integer>(Arrays.asList(s)));
            OPS.put(o, t);
        }
    }

    public void addPOSTriple(Integer p, Integer o, Integer s) {
        if (POS.containsKey(p)) {
            if (POS.get(p).containsKey(o)) {
                if (!POS.get(p).get(o).contains(s)) {
                    POS.get(p).get(o).add(s);
                }
            } else {
                POS.get(p).put(o, new ArrayList<Integer>(Arrays.asList(s)));
            }
        } else {
            Map<Integer, ArrayList<Integer>> t = new HashMap<Integer, ArrayList<Integer>>();
            t.put(o, new ArrayList<Integer>(Arrays.asList(s)));
            POS.put(p, t);
        }
    }

    public void addOSPTriple(Integer o, Integer s, Integer p) {
        if (OSP.containsKey(o)) {
            if (OSP.get(o).containsKey(s)) {
                if (!OSP.get(o).get(s).contains(p)) {
                    OSP.get(o).get(s).add(p);
                }
            } else {
                OSP.get(o).put(s, new ArrayList<Integer>(Arrays.asList(p)));
            }
        } else {
            Map<Integer, ArrayList<Integer>> t = new HashMap<Integer, ArrayList<Integer>>();
            t.put(s, new ArrayList<Integer>(Arrays.asList(p)));
            OSP.put(o, t);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for(Map.Entry<Integer, Map<Integer, ArrayList<Integer>>> entry : SPO.entrySet()) {
            for(Map.Entry<Integer, ArrayList<Integer>> sEntry : entry.getValue().entrySet()) {
                for (Integer third : sEntry.getValue()) {
                    builder.append("<")
                            .append(entry.getKey())
                            .append(", ")
                            .append(sEntry.getKey())
                            .append(", ")
                            .append(third)
                            .append(">\n");
                }
            }
        }

        return builder.toString();

    }
    }
