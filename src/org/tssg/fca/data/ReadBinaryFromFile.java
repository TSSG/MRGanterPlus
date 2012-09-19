/**
 * Copyright (c) Waterford Institute of Technology, 2012, bxu, CEARTAS.
 * All rights reserved.
 * 
 * Licensed under the MIT License, (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit persons to
 * whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tssg.fca.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

/**
 * This class is used by Mapper to load binary data from file.
 * It must implements Value interface.
 * 
 * @author biaoxu
 * @version 1.3
 */
public class ReadBinaryFromFile implements Value
{
	private int data[][];
	private boolean dataLoaded = false;
	
	//The number of rows
	private int numRows;
	//The number of columns
	private int numColumns;
	
	public ReadBinaryFromFile(int num)
	{
		numRows = num;
		data = new int[numRows][];
	}
	
	public ReadBinaryFromFile()
	{
		numRows = 2;
		data = new int[2][];
	}
	
	public ReadBinaryFromFile(int d[])
	{
		numRows = 1;
		numColumns = d.length;
		data = new int[numRows][numColumns];
		data[numRows-1] = d;
	}
	
	public ReadBinaryFromFile(int[][] data, int numData, int vecLen)
	{
		this.data = data;
		this.numRows = numData;
		this.numColumns = vecLen;
		this.dataLoaded = true;
	}
	
	/**
	 * Serializes the ReadBinaryFromFile object
	 */
	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {

			this.numRows = din.readInt();
			this.numColumns = din.readInt();

			this.data = new int[numRows][numColumns];

			for (int i = 0; i < numRows; i++) {
				for (int j = 0; j < numColumns; j++) {
					data[i][j] = din.readInt();
				}
			}
			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		
	}

	/**
	 * Deserializes the ReadBinaryFromFile object.
	 */
	@Override
	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();

		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;

		try {
			dout.writeInt(numRows);
			dout.writeInt(numColumns);
			for (int i = 0; i < numRows; i++) {
				for (int j = 0; j < numColumns; j++) {
					dout.writeInt(data[i][j]);
				}
			}
			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream.close();
			dout.close();
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return marshalledBytes;
	}

	public int[][] getData() {
		return data;
	}
	
	public int getNumData() {
		return numRows;
	}
	
	public int getNumColumns() {
		return numColumns;
	}
	
	public int[] getLastData()
	{
		return data[numRows-1];
	}
	
	public boolean isDataLoaded() {
		return dataLoaded;
	}
	
	public int[][] loadDataFromBinFile(String fileName) throws IOException {
		File file = new File(fileName);
		BufferedInputStream bin = new BufferedInputStream(new FileInputStream(
				file));
		DataInputStream din = new DataInputStream(bin);

		numRows = din.readInt();
		numColumns = din.readInt();

		if (!(numRows > 0 && numRows <= Integer.MAX_VALUE && numColumns > 0 && numColumns <= Integer.MAX_VALUE))
		{
			bin.close();
			din.close();
			throw new IOException("Invalid number of rows or columns.");
		}

		this.data = new int[numRows][numColumns];
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numColumns; j++) {
				data[i][j] = din.readInt();
			}
		}
		
		bin.close();
		din.close();
		return this.data;
	}
	
	/**
	 * Loads data from a text file. Sample input text file is shown below. First
	 * line indicates the number of lines. Second line gives the length of the
	 * vector. 
	 * 5 
	 * 2 
	 * 1.2 2.3 
	 * 5.6 3.3 
	 * 1.0 2.5 
	 * 3.0 6.5 
	 * 5.5 6.3
	 * @param fileName String the full path and name of the text file.
	 * @return int[][] a matrix consists of elements in the file.
	 */
	public int[][] loadDataFromTextFile(String fileName) throws IOException {

		File file = new File(fileName);
		BufferedReader reader = new BufferedReader(new FileReader(file));

		String inputLine = reader.readLine();
		if (inputLine != null) {
			numRows = Integer.parseInt(inputLine);
		} else {
			reader.close();
			new IOException("First line = number of rows is null");
		}

		inputLine = reader.readLine();
		if (inputLine != null) {
			numColumns = Integer.parseInt(inputLine);
		} else {
			new IOException("Second line = size of the vector is null");
		}
		this.data = new int[numRows][numColumns];

		String[] vectorValues = null;
		int numRecords = 0;
		while ((inputLine = reader.readLine()) != null) {
			vectorValues = inputLine.split(" ");
			if (numColumns != vectorValues.length) {
				throw new IOException("Vector length did not match at line "
						+ numRecords);
			}
			for (int i = 0; i < numColumns; i++) {
				data[numRecords][i] = Integer.valueOf(vectorValues[i]);
			}
			numRecords++;
		}
		reader.close();
		return this.data;
	}

	/**
	 * Write the vector data into a binary file.
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	public void writeToBinFile(String fileName) throws IOException {
		BufferedOutputStream bout = new BufferedOutputStream(
				new FileOutputStream(fileName));
		DataOutputStream dout = new DataOutputStream(bout);

		// First two parameters are the dimensions.
		dout.writeInt(numRows);
		dout.writeInt(numColumns);
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numColumns; j++) {
				dout.writeDouble(data[i][j]);
			}
		}
		dout.flush();
		bout.flush();
		dout.close();
		bout.close();
	}

	/**
	 * Write the vector data into a text file. First two lines give numData and
	 * vecLen.
	 * 
	 * @param fileName
	 *            - Name of the file to write.
	 * @throws IOException
	 */
	public void writeToTextFile(String fileName) throws IOException {
		// Data as string
		BufferedOutputStream bout = new BufferedOutputStream(
				new FileOutputStream(fileName));
		PrintWriter writer = new PrintWriter(bout);
		writer.println(numRows);
		writer.println(numColumns);
		StringBuffer line;
		for (int i = 0; i < numRows; i++) {
			line = new StringBuffer();
			for (int j = 0; j < numColumns; j++) {
				if (j == (numColumns - 1)) {
					line.append(data[i][j]);
				} else {
					line.append(data[i][j] + " ");
				}
			}
			writer.println(line.toString());
		}
		writer.flush();
		writer.close();
		bout.flush();
		bout.close();
	}
	
}
