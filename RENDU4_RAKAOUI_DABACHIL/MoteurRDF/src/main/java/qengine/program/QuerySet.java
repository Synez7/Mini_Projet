package qengine.program;

import benchmark.QueryEntry;
import benchmark.QueryEntry.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


import static benchmark.QueryEntry.QueryTag.*;
import static qengine.program.Main.retrieveQuerySetFromFile;

/**
 * Classe permettant de filtrer un jeu de requête afin d'avoir l'ensemble le plus pertinent pour notre benchmark
 */
public class QuerySet {

    static Options options = new Options();

    static CommandLineParser parser = new DefaultParser();
    static CommandLine cmd;

    static final String baseURI = null;

    /**
     * Votre répertoire de travail où vont se trouver les fichiers à lire
     */
    static final String workingDir = "data\\";

    /**
     * Fichier contenant les requêtes sparql
     */
    static String queryDirectory = "Projet_NoSQL_DabachilRakkaoui\\qengine-master\\data\\STAR_ALL_workload.queryset";

    /**
     * Fichier contenant des données rdf
     */
    static String dataFile = "Projet_NoSQL_DabachilRakkaoui\\qengine-master\\data\\100K.nt";;

    /**
     * Path du repertoire contenant les fichiers produits
     */
    static String output = "outputQuerySetStats";



    static List<QueryEntry> querySet = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        options.addOption("queries", true, "NA");
        options.addOption("data", true,"NA");

        cmd = parser.parse(options, args);

        if(cmd.hasOption("queries")) {
            queryDirectory = cmd.getOptionValue("queries");
        }
        if(cmd.hasOption("data")){
            dataFile = cmd.getOptionValue("data");
        }

        querySet = retrieveQueries();
        processQueriesJena();
        System.out.println(querySet.stream().map(e->e.tags).collect(Collectors.toList()));
        printStats();
        filterTag(100, DUP);
//        equalize();
        randomRemove(40000);
        printStats();
        writeQuerySetAndStats();

    }


    /**
     * Ecris toutes les requêtes contenues dans querySet dans un fichier .queryset et produit un fichier
     * csv contenant les statistiques associées au jeu de requêtes produit
     * @throws IOException
     */
    public static void writeQuerySetAndStats()
            throws IOException {
        BufferedWriter queryWriter = new BufferedWriter(new FileWriter("qengine-master\\output\\queries500k&2mDupAdjusted.queryset"));
        for(QueryEntry qe : querySet){
            queryWriter.write(qe.queryString);
            queryWriter.newLine();
        }
        queryWriter.close();

        BufferedWriter queryStatsWriter = new BufferedWriter(new FileWriter("qengine-master\\output\\queries500k&2mDupAdjusted.csv"));
        long nbQuery = querySet.size();
        long nbQueryNA = querySet.stream().filter(e->e.tags.contains(NA)).count();
        long nbQueryDupNA = nbQueryNA - querySet.stream().filter(e-> e.tags.contains(NA) && e.tags.contains(DUP)).distinct().count();
        long nbQueryDUP = querySet.stream().filter(e->e.tags.contains(DUP)).count();
        long nbQ1 = querySet.stream().filter(e->e.tags.contains(Q1)).count();
        long nbQ2 = querySet.stream().filter(e->e.tags.contains(Q2)).count();
        long nbQ3 = querySet.stream().filter(e->e.tags.contains(Q3)).count();
        long nbQ4 = querySet.stream().filter(e->e.tags.contains(Q4)).count();

        queryStatsWriter.write("nom du fichier de données,nom du dossier des requêtes ,nombre de triplets RDF,nombre de requêtes, NB QUERY NA, NB QUERY DUP," +
                "NB QUERY BOTH NA AND DUP, NB Q1, NB Q2, NB Q3,NB Q4, PERCENTAGE NA, PERCENTAGE DUP, " +
                "PERCENTAGE BOTH NA AND DUP, PERCENTAGE Q1, PERCENTAGE Q2, PERCENTAGE Q3, PERCENTAGE Q4");
        queryStatsWriter.newLine();
        queryStatsWriter.write(dataFile);queryStatsWriter.write(",");
        queryStatsWriter.write(queryDirectory);queryStatsWriter.write(",");
        queryStatsWriter.write(Files.lines(Paths.get(dataFile)).count()+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQuery+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQueryNA+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQueryDUP+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQueryDupNA+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQ1+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQ2+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQ3+"");queryStatsWriter.write(",");
        queryStatsWriter.write(nbQ4+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQueryNA/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQueryDUP/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQueryDupNA/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQ1/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQ2/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQ3/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.write((float)nbQ4/nbQuery*100+"");queryStatsWriter.write(",");
        queryStatsWriter.close();

    }


    /**
     * Evaluation des requêtes par Jena pour déterminer si la query a une réponse
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static void processQueriesJena() throws FileNotFoundException, IOException {

        Model model = ModelFactory.createDefaultModel();
        model.read(dataFile);

        for(QueryEntry query : querySet) {

            QueryExecution execution = QueryExecutionFactory.create(query.queryString, model);

            StringBuilder sb = new StringBuilder();
            try {
                ResultSet rs = execution.execSelect();
                List<QuerySolution> solution = ResultSetFormatter.toList(rs);
                if(solution.isEmpty()){
//                    sb.append("Result for query " + queryString + " : " + "[]");
                    query.tags.add(NA);
                }
                for (QuerySolution querySolution : solution) {
                    querySolution.varNames().forEachRemaining((varName) -> {
//                        sb.append("Result for query " + queryString + " : " + querySolution.get(varName));
//                        System.out.println(("Result for query " + queryString + " : " + querySolution.get(varName)));

                    });
                }
            } finally {
                execution.close();
            }
//            System.out.println(sb);

        }
    }

    /**
     * Récupère les queries d'un seul fichier ou d'un répertoire
     * @return liste des queries
     * @throws IOException
     */
    static List<QueryEntry> retrieveQueries() throws IOException {
        List<QueryEntry> res = new ArrayList<>();
        File folder = new File(queryDirectory);
        if(!folder.exists())
            throw new NoSuchFileException("Le dossier n'existe pas !");

        if(folder.isFile()) {
            if(!FilenameUtils.getExtension(folder.getName()).equals("queryset"))
                throw new NoSuchFileException("Le fichier n'a pas la bonne extension");

            processTags(res, folder);
        } else if(folder.isDirectory() && folder.listFiles() != null) {
            for(File file : Objects.requireNonNull(folder.listFiles())) {
                if(!FilenameUtils.getExtension(file.getName()).equals("queryset")) {
                    continue;
                }
                processTags(res, file);
            }
        } else
            throw new NoSuchFileException("Le dossier n'existe pas !");
//        System.out.println(res);
        return res;
    }


    /**
     * Associe les tag DUP, Qn aux queries
     * @param res
     * @param file
     * @throws IOException
     */
    private static void processTags(List<QueryEntry> res, File file) throws IOException {

        QueryTag tag = Q1;
        if(file.getName().contains("Q_1")){
            tag = Q1;

        }
        else if(file.getName().contains("Q_2")){
            tag = Q2;
        }
        else if(file.getName().contains("Q_3")){
            tag = Q3;
        }
        else if(file.getName().contains("Q_4")){
            tag = Q4;
        }
        List<String> querySet = retrieveQuerySetFromFile(file.getAbsolutePath());
        for(String q : querySet){
            QueryEntry queryEntry = new QueryEntry();

            for(QueryEntry qe : res){
                if(qe.queryString.equals(q) && !qe.tags.contains(DUP)){
                    qe.tags.add(DUP);
                    break;
                }
            }
            queryEntry.tags.add(tag);
            queryEntry.queryString = q;
            res.add(queryEntry);

        }
    }


    /**
     * filtre les queries de façon à atteindre un pourcentage donné pour le tag donné
     * @param percentage
     * @param tag
     */
    static void filterTag(float percentage, QueryTag tag){
        Collections.shuffle(querySet);

        int i = 1;
        while(i<querySet.size() && (float)querySet.stream().filter(e -> e.tags.contains(tag)).count()/querySet.size()*100 > percentage){
            if(querySet.get(i-1).tags.contains(tag)){
                System.out.println("PERCENTAGE = " + (float)querySet.stream().filter(e -> e.tags.contains(tag)).count()/querySet.size()*100);
                querySet.remove(i-1);
                i--;
            }
            i++;
        }
        i = 1;
        while(i<querySet.size() && (float)querySet.stream().filter(e -> e.tags.contains(tag)).count()/querySet.size()*100 < percentage){
            if(!querySet.get(i-1).tags.contains(tag)){
                System.out.println("PERCENTAGE = " + (float)querySet.stream().filter(e -> e.tags.contains(tag)).count()/querySet.size()*100);
                querySet.remove(i-1);
                i--;
            }
            i++;
        }
    }


    /**
     * supprime de manière aléatoire des queries dans querySet afin d'atteindre le nombre de requêtes désiré
     * @param nbQuery
     */
    static void randomRemove(int nbQuery){
        Random random = new Random();
        int randomIndex = 0;

        while (querySet.size() > nbQuery){
            randomIndex = random.nextInt(querySet.size()-1);
            querySet.remove(randomIndex);
        }
    }


    /**
     * Equilibre la part de Qn dans le jeu de requêtes
     */
    static void equalize(){
        int i =1;

        long nbQ1 = querySet.stream().filter(e->e.tags.contains(Q1)).count();
        long nbQ2 = querySet.stream().filter(e->e.tags.contains(Q2)).count();
        long nbQ3 = querySet.stream().filter(e->e.tags.contains(Q3)).count();
        long nbQ4 = querySet.stream().filter(e->e.tags.contains(Q4)).count();

        long min = Math.min(Math.min(nbQ1,nbQ2), Math.min(nbQ3,nbQ4));
        System.out.println(min);
        while(i<querySet.size()){
            if(querySet.get(i-1).tags.contains(Q1) && nbQ1 > min){
                querySet.remove(i-1);
                nbQ1--;
                i--;
            }
            else if(querySet.get(i-1).tags.contains(Q2) && nbQ2 > min){
                querySet.remove(i-1);
                nbQ2--;
                i--;
            }
            else if(querySet.get(i-1).tags.contains(Q3) && nbQ3 > min){
                querySet.remove(i-1);
                nbQ3--;
                i--;
            }
            else if (querySet.get(i-1).tags.contains(Q4) && nbQ4 > min){
                querySet.remove(i-1);
                nbQ4--;
                i--;
            }
            System.out.print("Q1 = " + nbQ1 + " ");
            System.out.print("Q2 = " + nbQ2 + " ");
            System.out.print("Q3 = " + nbQ3 + " ");
            System.out.print("Q4 = " + nbQ4 + " ");
            System.out.println(" ");
            i++;
        }

    }

    static void printStats(){
        long nbQuery = querySet.size();
        long nbQueryNA = querySet.stream().filter(e->e.tags.contains(NA)).count();
        long nbQueryDupNA = nbQueryNA - querySet.stream().filter(e-> e.tags.contains(NA) && e.tags.contains(DUP)).distinct().count();
        long nbQueryDUP = querySet.stream().filter(e->e.tags.contains(DUP)).count();
        long nbQ1 = querySet.stream().filter(e->e.tags.contains(Q1)).count();
        long nbQ2 = querySet.stream().filter(e->e.tags.contains(Q2)).count();
        long nbQ3 = querySet.stream().filter(e->e.tags.contains(Q3)).count();
        long nbQ4 = querySet.stream().filter(e->e.tags.contains(Q4)).count();

        StringBuilder sb = new StringBuilder();
        sb.append("NB QUERY = " + nbQuery + '\n' +
                "NB QUERY NA = " + nbQueryNA + '\n' +
                "NB QUERY DUP = " + nbQueryDUP + '\n' +
                "NB QUERY BOTH NA AND DUPLICATE = " + nbQueryDupNA + '\n' +
                "NB Q1 = " + nbQ1 + '\n' +
                "NB Q2 = " + nbQ2 + '\n' +
                "NB Q3 = " + nbQ3 + '\n' +
                "NB Q4 = " + nbQ4 + '\n' +
                "PERCENTAGE QUERY NA = " + (float)nbQueryNA/nbQuery*100 + '\n' +
                "PERCENTAGE DUPLICATE QUERY = " + (float)nbQueryDUP/nbQuery*100 + '\n' +
                "PERCENTAGE QUERY BOTH NA AND DUPLICATE = " + (float)nbQueryDupNA/nbQuery*100 + '\n' +
                "PERCENTAGE Q1 = " + (float)nbQ1/nbQuery*100 + '\n' +
                "PERCENTAGE Q2 = " + (float)nbQ2/nbQuery*100 + '\n' +
                "PERCENTAGE Q3 = " + (float)nbQ3/nbQuery*100 + '\n' +
                "PERCENTAGE Q4 = " + (float)nbQ4/nbQuery*100);


        System.out.println(sb);
    }



}