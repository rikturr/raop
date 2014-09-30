package edu.fau.socialbigdata.raop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class DatasetCreator {

	public static final String CLASS_ATTRIBUTE = "requester_received_pizza";

	public static void main(String[] args) throws Exception {
		JSONParser parser = new JSONParser();
		JSONArray json = (JSONArray) parser.parse(new FileReader(new File("src/main/resources/train.json")));

		writeOriginalResultDataSet(json);
		writeModifiedResultDataSet(json);
	}

	private static void writeOriginalResultDataSet(JSONArray json) throws IOException {
		BufferedWriter resultWriter = new BufferedWriter(new FileWriter("target/results_original.arff"));

		Set<String> resultAttributes = new LinkedHashSet<String>();
		resultAttributes.add("number_of_downvotes_of_request_at_retrieval");
		resultAttributes.add("number_of_upvotes_of_request_at_retrieval");
		resultAttributes.add("request_number_of_comments_at_retrieval");

		resultWriter.write("@RELATION raop_results\n\n");
		resultWriter.write("@ATTRIBUTE request_id STRING\n");
		resultWriter.write("@ATTRIBUTE number_of_downvotes_of_request_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE number_of_upvotes_of_request_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE request_number_of_comments_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE requester_received_pizza {received_pizza,not_received_pizza}\n\n");
		resultWriter.write("@DATA\n");

		for (Object a : json) {
			JSONObject element = (JSONObject) a;

			//add id
			String row = (String) element.get("request_id") + ",";

			for (String attribute : resultAttributes) {
				long value = (Long) element.get(attribute);
				//missing value
				if (element.get(attribute) == null) {
					row += "?,";
				} else {
					row += value + ",";
				}
			}

			//add class value
			boolean classValue = (Boolean) element.get(CLASS_ATTRIBUTE);
			if (classValue) {
				row += "received_pizza";
			} else {
				row += "not_received_pizza";
			}

			resultWriter.write(row + "\n");
		}

		resultWriter.close();
	}

	private static void writeModifiedResultDataSet(JSONArray json) throws IOException {
		BufferedWriter resultWriter = new BufferedWriter(new FileWriter("target/results_modified.arff"));

		resultWriter.write("@RELATION raop_results\n\n");
		resultWriter.write("@ATTRIBUTE request_id STRING\n");
		resultWriter.write("@ATTRIBUTE upvote_downvote_score NUMERIC\n");
		resultWriter.write("@ATTRIBUTE request_number_of_comments_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE requester_received_pizza {received_pizza,not_received_pizza}\n\n");
		resultWriter.write("@DATA\n");

		for (Object a : json) {
			JSONObject element = (JSONObject) a;

			//add id
			String row = (String) element.get("request_id") + ",";

			//add upvote-downvote score
			long upvote = (Long) element.get("number_of_upvotes_of_request_at_retrieval");
			long downvote = (Long) element.get("number_of_downvotes_of_request_at_retrieval");

			row += (upvote - downvote) + ",";

			//add number of comments
			row += element.get("request_number_of_comments_at_retrieval") + ",";

			//add class value
			boolean classValue = (Boolean) element.get(CLASS_ATTRIBUTE);
			if (classValue) {
				row += "received_pizza";
			} else {
				row += "not_received_pizza";
			}

			resultWriter.write(row + "\n");
		}

		resultWriter.close();
	}
}
