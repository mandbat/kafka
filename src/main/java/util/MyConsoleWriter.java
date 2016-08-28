package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class MyConsoleWriter {

	public static void write(String text, String fileName) {

		try {

			FileWriter fwr = new FileWriter(fileName, true);
			BufferedWriter out = new BufferedWriter(fwr);

			long curTime = System.currentTimeMillis();
			String curStringDate = new SimpleDateFormat(
					"dd.MM.yyyy H:mm:ss:SSS").format(curTime);

			out.write(curStringDate + " : " + text + "\n");
			out.close();

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	public static void write(String text) {

		try {

			FileWriter fwr = new FileWriter("C:\\temp\\log\\debug.txt", true);
			BufferedWriter out = new BufferedWriter(fwr);

			long curTime = System.currentTimeMillis();
			String curStringDate = new SimpleDateFormat(
					"dd.MM.yyyy H:mm:ss:SSS").format(curTime);

			out.write(curStringDate + " : " + text + "\n");
			out.close();

		} catch (IOException e) {

			e.printStackTrace();
		}
	}
}
