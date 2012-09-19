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

package org.tssg.fca.mr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import org.safehaus.uuid.UUIDGenerator;
import org.tssg.fca.intent.ShortVector;

import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;

public class MRGanterPlus {

	private static Vector<ShortVector> concepts = new Vector<ShortVector>();
	private static int NUMBEROFCONCEPTS = 0, ITERATIONS=0;
	
	public static void main(String args[]) throws IOException
	{
		File closureFile = new File("./MrGanterPlus_Concepts.txt");
		FileWriter fileWriter = new FileWriter(closureFile);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		
		if(args.length != 2)
		{
			String errorReport = "MrGanterPlus: the Correct arguments are \n"
				+ "java MrGanterPlus "
				+ "<num of map tasks> <partition file>";
			System.out.println(errorReport);
			System.exit(0);
		}
		int numMapTasks = Integer.parseInt(args[0]);

		String partitionFile = args[1];
		MRGanterPlus client;
		client = new MRGanterPlus();
		double beginTime = System.currentTimeMillis();
		try {
			client.driveMapReduce(partitionFile, numMapTasks);
			
		} catch (TwisterException e) {
			e.printStackTrace();
		}
		double endTime = System.currentTimeMillis();
		
		bufferedWriter.write("MrGanterPlus took " + (endTime - beginTime) / 1000 + " seconds." + "\n");
		bufferedWriter.write("Totally: " + NUMBEROFCONCEPTS + " concepts IR: " + ITERATIONS + "\n");

		for(ShortVector intent:concepts)
		{
			bufferedWriter.write(intent.toString() + "\n");
		}
		
		bufferedWriter.append('\n');
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
		
		System.out.println("---------------------Successful---------------------------------");
		System.exit(0);
	}
	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();
	
	/**
	 * This method configures Twister driver and executes map and reduce tasks iteratively.
	 * @param partitionFile contains the location information of data partitions.
	 * @param numMapTasks set the number of Map task.
	 * @throws TwisterException
	 * @throws IOException
	 */
	public void driveMapReduce(String partitionFile, int numMapTasks) throws TwisterException, IOException
	{
		//Need one reducer
		int numReducers = 1;
		//Configure MapReduce computation with a job ID.
		JobConf jobConf = new JobConf("Ganter-Map-Reduce "+uuidGen.generateRandomBasedUUID());
		jobConf.setMapperClass(MRGanterPlusMapTask.class);
		jobConf.setReducerClass(MRGanterPlusReduceTask.class);
		jobConf.setCombinerClass(MRGanterPlusCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		
		TwisterModel driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);
		
		ShortVector first = new ShortVector();
		//Provide an empty IntentsSet when MRGanterPlus runs first time.
		IntentsSet input = new IntentsSet(1);
		//Set the flag to true to indicate this is the first IntentsSet.
		input.setFlag(true);
		input.addConcept(first);
		TwisterMonitor monitor = null;
		boolean isCompleted = false;
		
		//Check if the computation is completed.
		while( !isCompleted )
		{					
			ITERATIONS++;
			//Broadcast the latest Intents so that every Mapper can handle them.
			monitor = driver.runMapReduceBCast(input);
			monitor.monitorTillCompletion();
					
			MRGanterPlusCombiner combiner = (MRGanterPlusCombiner)driver.getCurrentCombiner();
			//Update input with the latest found IntentsSet.
			input = combiner.getResults();
			int len = input.getSize();

			NUMBEROFCONCEPTS += len;
			if(len > 0)
			{		
				//Store the new found concepts.
				for(int i=0; i<len; i++)
				{
					concepts.add(input.getIntentsValueAt(i));
				}
			}
			else
			{
				//Set the indicator to true when coming to the final iteration, 
				//i.e., no more intents could be found
				isCompleted = true;
				break;
			}
		}
		driver.close();
		
	}
}
