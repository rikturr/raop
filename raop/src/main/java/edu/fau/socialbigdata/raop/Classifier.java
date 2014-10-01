package edu.fau.socialbigdata.raop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import weka.classifiers.meta.FilteredClassifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ConverterUtils.DataSource;

public class Classifier {
	public static final String TRAIN = "target/full_no_text_train.arff";
	public static final String TEST = "target/full_no_text_test.arff";

	public static void main(String[] args) throws Exception {
		DataSource source = new DataSource(TRAIN);
		Instances trainingInstances = source.getDataSet();
		// Make the last attribute be the class
		trainingInstances.setClassIndex(trainingInstances.numAttributes() - 1);

		FilteredClassifier classifier = new FilteredClassifier();
		classifier.setOptions(Utils.splitOptions("-F \"weka.filters.unsupervised.attribute.Remove -R 1\" -W weka.classifiers.meta.Bagging -- -P 100 -S 1 -I 50 -W weka.classifiers.trees.REPTree -- -M 2 -V 0.0010 -N 3 -S 1 -L -1"));

		Instances testInstances = new Instances(new BufferedReader(new FileReader(TEST)));
		testInstances.setClassIndex(testInstances.numAttributes() - 1);

		Instances labeled = new Instances(testInstances);

		classifier.buildClassifier(trainingInstances);
		
		BufferedWriter resultWriter = new BufferedWriter(new FileWriter("target/submission.csv"));

		for (int i = 0; i < testInstances.numInstances(); i++) {
			double classValue = classifier.classifyInstance(testInstances.instance(i));
			Instance instance = labeled.instance(i);
			instance.setClassValue(classValue);

			//not sure why positive is 0
			if(classValue == 0) {
				resultWriter.write(instance.toString(0) + ",1");
			} else {
				resultWriter.write(instance.toString(0) + ",0");
			}
			resultWriter.write("\n");
		}

		resultWriter.close();
		BufferedWriter writer = new BufferedWriter(new FileWriter("target/labeled.arff"));
		writer.write(labeled.toString());
		writer.newLine();
		writer.close();
	}
}