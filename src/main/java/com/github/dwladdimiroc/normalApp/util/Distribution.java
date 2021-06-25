package com.github.dwladdimiroc.normalApp.util;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Distribution {
    private String distribution;

    public Distribution(String distribution) {
        this.distribution = distribution;
    }

    public float[] Input() {
        float[] inputSamples = null;
        String fileName = this.distribution + ".csv";

        InputStreamReader csvReader = this.getFileFromResourceAsStream(fileName);
        CSVReader reader = new CSVReader(csvReader);
        try {
            List<String[]> csvInput = reader.readAll();
            inputSamples = new float[csvInput.size()];
            for (int i = 1; i < csvInput.size(); i++) {
                inputSamples[i] = Float.parseFloat(csvInput.get(i)[1]);
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
        return inputSamples;
    }

    private InputStreamReader getFileFromResourceAsStream(String fileName) {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            return inputStreamReader;
        }
    }
}
