package edu.fau.socialbigdata.raop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class DatasetCreator {

	public static final String ID_ATTRIBUTE = "request_id";
	public static final String CLASS_ATTRIBUTE = "requester_received_pizza";

	private static Set<String> numericAttributes = new LinkedHashSet<String>();
	private static Set<String> timeAttributes = new LinkedHashSet<String>();
	private static Set<String> resultAttributes = new LinkedHashSet<String>();
	

	public static void main(String[] args) throws Exception {
		JSONParser parser = new JSONParser();
		JSONArray json = (JSONArray) parser.parse(new FileReader(new File("src/main/resources/train.json")));

		initialize();

		writeFullWithoutText(json);
		writePostMetaFullDataSet(json);

		writeOriginalResultDataSet(json);
		writeModifiedResultDataSet(json);
	}

	private static void writeFullWithoutText(JSONArray json) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter("target/full_no_text.arff"));

		writer.write("@RELATION raop_full_no_text\n\n");
		writer.write("@ATTRIBUTE " + ID_ATTRIBUTE + " STRING\n");
		writer.write("@ATTRIBUTE post_was_edited {true,false}\n");
		for (String attribute : numericAttributes) {
			writer.write("@ATTRIBUTE " + attribute + " NUMERIC\n");
		}
		for (String attribute : resultAttributes) {
			writer.write("@ATTRIBUTE " + attribute + " NUMERIC\n");
		}
		for (String attribute : timeAttributes) {
			writer.write("@ATTRIBUTE " + attribute + " DATE \"yyyy-MM-dd HH:mm:ss\"\n");
		}
		writer.write("@ATTRIBUTE " + CLASS_ATTRIBUTE + " {received_pizza,not_received_pizza}\n\n");
		writer.write("@DATA\n");

		for (Object a : json) {
			JSONObject element = (JSONObject) a;

			//add id
			String row = (String) element.get(ID_ATTRIBUTE) + ",";

			//add post_was_edited
			try {
				Boolean edited = (Boolean) element.get("post_was_edited");
				if (edited == null) {
					row += "?,";
				} else {
					row += edited + ",";
				}
			} catch (ClassCastException e) {
				row += "?,";
			}

			//add numeric attributes
			for (String attribute : numericAttributes) {
				if (element.get(attribute) == null) {
					row += "?,";
				} else {
					row += element.get(attribute) + ",";
				}
			}
			
			//add result attributes
			for (String attribute : resultAttributes) {
				long value = (Long) element.get(attribute);
				//missing value
				if (element.get(attribute) == null) {
					row += "?,";
				} else {
					row += value + ",";
				}
			}

			//add time attributes
			for (String attribute : timeAttributes) {
				Double time = (Double) element.get(attribute);
				if (time == null) {
					row += "?,";
				} else {
					Date date = new Date();
					date.setTime(time.longValue() * 1000);
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					row += "\"" + format.format(date) + "\",";
				}
			}

			//add class value
			boolean classValue = (Boolean) element.get(CLASS_ATTRIBUTE);
			if (classValue) {
				row += "received_pizza";
			} else {
				row += "not_received_pizza";
			}

			writer.write(row + "\n");
		}

		writer.close();
	}

	private static void initialize() {
		numericAttributes.add("requester_account_age_in_days_at_request");
		numericAttributes.add("requester_account_age_in_days_at_retrieval");
		numericAttributes.add("requester_days_since_first_post_on_raop_at_request");
		numericAttributes.add("requester_days_since_first_post_on_raop_at_retrieval");
		numericAttributes.add("requester_number_of_comments_at_request");
		numericAttributes.add("requester_number_of_comments_at_retrieval");
		numericAttributes.add("requester_number_of_comments_in_raop_at_request");
		numericAttributes.add("requester_number_of_comments_in_raop_at_retrieval");
		numericAttributes.add("requester_number_of_posts_at_request");
		numericAttributes.add("requester_number_of_posts_at_retrieval");
		numericAttributes.add("requester_number_of_posts_on_raop_at_request");
		numericAttributes.add("requester_number_of_posts_on_raop_at_retrieval");
		numericAttributes.add("requester_number_of_subreddits_at_request");
		numericAttributes.add("requester_upvotes_minus_downvotes_at_request");
		numericAttributes.add("requester_upvotes_minus_downvotes_at_retrieval");
		numericAttributes.add("requester_upvotes_plus_downvotes_at_request");
		numericAttributes.add("requester_upvotes_plus_downvotes_at_retrieval");

		timeAttributes.add("unix_timestamp_of_request");
		timeAttributes.add("unix_timestamp_of_request_utc");
		
		resultAttributes.add("number_of_downvotes_of_request_at_retrieval");
		resultAttributes.add("number_of_upvotes_of_request_at_retrieval");
		resultAttributes.add("request_number_of_comments_at_retrieval");
	}

	private static void writePostMetaFullDataSet(JSONArray json) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter("target/post_meta_full.arff"));

		writer.write("@RELATION raop_meta\n\n");
		writer.write("@ATTRIBUTE " + ID_ATTRIBUTE + " STRING\n");
		writer.write("@ATTRIBUTE post_was_edited {true,false}\n");
		for (String attribute : numericAttributes) {
			writer.write("@ATTRIBUTE " + attribute + " NUMERIC\n");
		}
		for (String attribute : timeAttributes) {
			writer.write("@ATTRIBUTE " + attribute + " DATE \"yyyy-MM-dd HH:mm:ss\"\n");
		}
		writer.write("@ATTRIBUTE " + CLASS_ATTRIBUTE + " {received_pizza,not_received_pizza}\n\n");
		writer.write("@DATA\n");

		for (Object a : json) {
			JSONObject element = (JSONObject) a;

			//add id
			String row = (String) element.get(ID_ATTRIBUTE) + ",";

			//add post_was_edited
			try {
				Boolean edited = (Boolean) element.get("post_was_edited");
				if (edited == null) {
					row += "?,";
				} else {
					row += edited + ",";
				}
			} catch (ClassCastException e) {
				row += "?,";
			}

			//add numeric attributes
			for (String attribute : numericAttributes) {
				if (element.get(attribute) == null) {
					row += "?,";
				} else {
					row += element.get(attribute) + ",";
				}
			}

			//add time attributes
			for (String attribute : timeAttributes) {
				Double time = (Double) element.get(attribute);
				if (time == null) {
					row += "?,";
				} else {
					Date date = new Date();
					date.setTime(time.longValue() * 1000);
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					row += "\"" + format.format(date) + "\",";
				}
			}

			//add class value
			boolean classValue = (Boolean) element.get(CLASS_ATTRIBUTE);
			if (classValue) {
				row += "received_pizza";
			} else {
				row += "not_received_pizza";
			}

			writer.write(row + "\n");
		}

		writer.close();
	}

	private static void writeOriginalResultDataSet(JSONArray json) throws IOException {
		BufferedWriter resultWriter = new BufferedWriter(new FileWriter("target/results_original.arff"));

		resultWriter.write("@RELATION raop_results\n\n");
		resultWriter.write("@ATTRIBUTE " + ID_ATTRIBUTE + " STRING\n");
		resultWriter.write("@ATTRIBUTE number_of_downvotes_of_request_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE number_of_upvotes_of_request_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE request_number_of_comments_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE " + CLASS_ATTRIBUTE + " {received_pizza,not_received_pizza}\n\n");
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
		resultWriter.write("@ATTRIBUTE " + ID_ATTRIBUTE + " STRING\n");
		resultWriter.write("@ATTRIBUTE upvote_downvote_score NUMERIC\n");
		resultWriter.write("@ATTRIBUTE request_number_of_comments_at_retrieval NUMERIC\n");
		resultWriter.write("@ATTRIBUTE " + CLASS_ATTRIBUTE + " {received_pizza,not_received_pizza}\n\n");
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
