package qengine.program;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dictionary.Dictionary;
import indexes.Index;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

/**
 * Programme simple lisant un fichier de requête et un fichier de données.
 *
 * <p>
 * Les entrées sont données ici de manière statique,
 * à vous de programmer les entrées par passage d'arguments en ligne de commande comme demandé dans l'énoncé.
 * </p>
 *
 * <p>
 * Le présent programme se contente de vous montrer la voie pour lire les triples et requêtes
 * depuis les fichiers ; ce sera à vous d'adapter/réécrire le code pour finalement utiliser les requêtes et interroger les données.
 * On ne s'attend pas forcémment à ce que vous gardiez la même structure de code, vous pouvez tout réécrire.
 * </p>
 *
 * @author Olivier Rodriguez <olivier.rodriguez1@umontpellier.fr>
 */
final class Main {

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
	static String queryDirectory = "testDataAndQuerySet\\STAR_ALL_workload.queryset";

	/**
	 * Fichier contenant des données rdf
	 */
	static String dataFile = "testDataAndQuerySet\\100K.nt";

	static String output = "output";

	static List<String> querySet = new ArrayList<>();



	// ========================================================================

	/**
	 * Entrée du programme
	 */
	public static void main(String[] args) throws Exception {

		long t1 = System.currentTimeMillis();


		options.addOption("queries", true, "NA");
		options.addOption("data", true,"NA");
		options.addOption("output", true,"NA");
		options.addOption("Jena", false,"NA");
		options.addOption("warm", true,"NA");
		options.addOption("shuffle", false,"NA");

		cmd = parser.parse(options, args);


		if(cmd.hasOption("queries")) {
			queryDirectory = cmd.getOptionValue("queries");
		}
		if(cmd.hasOption("data")){
			dataFile = cmd.getOptionValue("data");
		}
		if(cmd.hasOption("output")){
			output = cmd.getOptionValue("output");
		}

		if(cmd.hasOption("warm")){
			if(Integer.parseInt(cmd.getOptionValue("warm"))>100 || Integer.parseInt(cmd.getOptionValue("warm"))<0){
				throw new IllegalArgumentException("Please input a value between 0 and 100 for argument of -warm option");
			}

			int nbOfQueries = querySet.size() * Integer.parseInt(cmd.getOptionValue("warm"))/100 == 0 ? 1 : querySet.size() * Integer.parseInt(cmd.getOptionValue("warm"))/100 ;
			System.out.println(nbOfQueries);
			while (querySet.size() > nbOfQueries){
				querySet.remove((int)(Math.random() * (querySet.size())));
			}
		}
		if(cmd.hasOption("shuffle")){
			Collections.shuffle(querySet);
		}

		long tdebdic = System.currentTimeMillis();
		Dictionary d = new Dictionary(dataFile);
		long tdic = System.currentTimeMillis() - tdebdic;

		long tdebind = System.currentTimeMillis();
		Index ind = new Index(d,dataFile);
		long tind = System.currentTimeMillis() - tdebind;


		long tdebquery = System.currentTimeMillis();
		querySet = retrieveQueries();
		long tquery = System.currentTimeMillis() - tdebquery;

		long tdebdata = System.currentTimeMillis();
		parseData();
		long tdata = System.currentTimeMillis() - tdebdata;

		long tdebeval = System.currentTimeMillis();
		if(cmd.hasOption("Jena")) {
			processQueriesJena();
		}
		else{
			processQueries(d,ind);
		}
		long teval = System.currentTimeMillis()-tdebeval;
		long tempsExecTotal = System.currentTimeMillis()-t1;

		System.out.println("---------------------------------------------------------STATS---------------------------------------------------------");

		System.out.println("nom du fichier de données: " + dataFile);
		System.out.println("nom du dossier des requêtes: " + queryDirectory);
		System.out.println("nombre de triplets RDF: " + Files.lines(Paths.get(dataFile)).count());
		System.out.println("nombre de requêtes: " + querySet.size());
		System.out.println("temps de lecture des données (ms): " + tdata);
		System.out.println("temps de lecture des requêtes (ms): " + tquery);
		System.out.println("temps création dico (ms): " + tdic);
		System.out.println("nombre d’index: 6");
		System.out.println("temps de création des index (ms): " + tind);
		System.out.println("temps total d’évaluation du workload (ms): " + teval);
		System.out.println("Temps exec total : " + tempsExecTotal + "ms");



		BufferedWriter bw;
		if(cmd.hasOption("Jena")) {
			bw = new BufferedWriter(new FileWriter("StatistiquesEvaluation\\statsJena.csv"));
		}
		else {
			bw = new BufferedWriter(new FileWriter("StatistiquesEvaluation\\stats.csv"));
		}

		bw.write("nom du fichier de données,nom du dossier des requêtes ,nombre de triplets RDF,nombre de requêtes, temps de lecture des données (ms), temps de lecture des requêtes (ms)," +
				"temps création dico (ms),nombre d'index,temps de création des index (ms), temps total d'évaluation du workload (ms), Temps exec total(ms)");
		bw.newLine();
		bw.write(dataFile);bw.write(",");
		bw.write(queryDirectory);bw.write(",");
		bw.write(Files.lines(Paths.get(dataFile)).count()+"");bw.write(",");
		bw.write(querySet.size()+"");bw.write(",");
		bw.write(tdata+"");bw.write(",");
		bw.write(tquery+"");bw.write(",");
		bw.write(tdic+"");bw.write(",");
		bw.write("6");bw.write(",");
		bw.write(tind+"");bw.write(",");
		bw.write(teval+"");bw.write(",");
		bw.write(tempsExecTotal+"");bw.write(",");
		bw.close();
	}

	/**
	 * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet obtenu.
	 */
	public static List<String> processAQuery(ParsedQuery query, Dictionary d, Index ind) throws IOException {
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());

		try {
			List<List<Integer>> res = new ArrayList<>();

			for (StatementPattern pattern : patterns) {


				Value subject = pattern.getSubjectVar().getValue();
				Value predicate = pattern.getPredicateVar().getValue();
				Value object = pattern.getObjectVar().getValue();

				if (subject == null && predicate != null && object != null) {
					int P = d.dictionary.getKey(predicate.toString());
					int O = d.dictionary.getKey(object.toString());

					if (ind.POS.get(P) == null) {
						break;
					}
					if (ind.POS.get(P).get(O) == null) {

						break;
					}
					res.add(ind.OPS.get(O).get(P));


				} else if (subject != null && predicate != null && object == null) {
					int S = d.dictionary.getKey(subject.toString());
					int P = d.dictionary.getKey(predicate.toString());
					res.add(ind.SPO.get(S).get(P));

				} else if (subject != null && predicate == null && object != null) {
					int S = d.dictionary.getKey(subject.toString());
					int O = d.dictionary.getKey(object.toString());
					res.add(ind.SOP.get(S).get(O));

				}
			}
			if(res.size() > 0) {
				for(List<Integer> patternRes : res){
					res.get(0).retainAll(patternRes);
				}
			}
			if (res.size() > 0 && res.get(0).size() > 0) {
				List<String> finalRes = res.get(0).stream().map(e -> d.dictionary.get(e)).collect(Collectors.toList());
				return finalRes;
			}

		} catch(NullPointerException npe) {
			List<String> finalRes = new ArrayList<>();
			return finalRes;
		}

		return new ArrayList<>();

	}


	// ========================================================================

	/**
	 * Traite chaque requête lue dans {@link #queryDirectory} avec {@link #processAQuery(ParsedQuery, Dictionary, Index)}.
	 */
	private static void processQueries(Dictionary d, Index ind) throws FileNotFoundException, IOException {
		BufferedWriter queryStatsWriter = new BufferedWriter(new FileWriter("ResultatsEvaluation\\results.txt"));
		SPARQLParser sparqlParser = new SPARQLParser();
		for(String queryString : querySet){
			ParsedQuery query = sparqlParser.parseQuery(queryString, baseURI);
			List<String> res = processAQuery(query,d,ind);
			if (res.size() > 0) {
//					System.out.println("Result for query " + queryString + " : " + "Match found = " + res);
			} else {
//				System.out.println("Result for query " + queryString + " : " + "No match.");

			}
			queryStatsWriter.write("Query :" + queryString);
			queryStatsWriter.write("Results: " + res);
			queryStatsWriter.newLine();
			queryStatsWriter.newLine();
		}
		queryStatsWriter.close();
	}

	/**
	 * Traite chaque requête lue dans {@link #queryDirectory} avec Jena.
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void processQueriesJena() throws FileNotFoundException, IOException {
		BufferedWriter queryStatsWriter = new BufferedWriter(new FileWriter("ResultatsEvaluation\\resultsJena.txt"));
		Model model = ModelFactory.createDefaultModel();
		model.read(dataFile);
		int i = 0;
		for(String queryString : querySet) {

			QueryExecution execution = QueryExecutionFactory.create(queryString, model);

//			StringBuilder sb = new StringBuilder();
			try {
				ResultSet rs = execution.execSelect();
				List<QuerySolution> solution = ResultSetFormatter.toList(rs);
//				if(solution.isEmpty()) sb.append("Result for query " + queryString + " : " + "[]");
				for (QuerySolution querySolution : solution) {
					queryStatsWriter.write("Query :" + queryString);
					queryStatsWriter.write("Results: " + querySolution);
					queryStatsWriter.newLine();
					querySolution.varNames().forEachRemaining((varName) -> {
//						sb.append("Result for query " + queryString + " : " + querySolution.get(varName));
					});
				}
			} finally {
				i++;
				execution.close();

			}
//			System.out.println(sb);

		}
		queryStatsWriter.close();

		System.out.println(i);
	}


	/**
	 * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
	 */
	private static void parseData() throws FileNotFoundException, IOException {

		try (Reader dataReader = new FileReader(dataFile)) {
			// On va parser des données au format ntriples
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);

			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(new MainRDFHandler());

			// Parsing et traitement de chaque triple par le handler
			//rdfParser.parse(dataReader, baseURI);
		}
	}


	/**
	 * Récupère les queries d'un seul fichier ou d'un répertoire
	 * @return liste des queries
	 * @throws IOException
	 */
	static List<String> retrieveQueries() throws IOException {
		List<String> res = new ArrayList<>();

		File folder = new File(queryDirectory);

		if(!folder.exists())
			throw new NoSuchFileException("Le dossier n'existe pas !");

		if(folder.isFile()) {
			if(!FilenameUtils.getExtension(folder.getName()).equals("queryset"))
				throw new NoSuchFileException("Le fichier n'a pas la bonne extension");
			List<String> querySet = retrieveQuerySetFromFile(folder.getAbsolutePath());
			res.addAll(querySet);
		} else if(folder.isDirectory() && folder.listFiles() != null) {
			for(File file : Objects.requireNonNull(folder.listFiles())) {

				if(!FilenameUtils.getExtension(file.getName()).equals("queryset")) {
					continue;
				}
				List<String> querySet = retrieveQuerySetFromFile(file.getAbsolutePath());
				res.addAll(querySet);
			}
		} else
			throw new NoSuchFileException("Le dossier n'existe pas !");

		return res;
	}

	/**
	 * Récupère les queries d'un seul fichier
	 * @return liste des queries
	 * @throws IOException
	 */
	static List<String> retrieveQuerySetFromFile(String queryPath) throws IOException {
		List<String> res = new ArrayList<>();

		try (Stream<String> lineStream = Files.lines(Paths.get(queryPath))) {
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();

			while (lineIterator.hasNext())
				/*
				 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'
				 * On considère alors que c'est la fin d'une requête
				 */
			{
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					res.add(queryString.toString());
					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}
		}
		return res;
	}

}
