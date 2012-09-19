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

import java.util.ArrayList;

import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.tssg.fca.intent.IntVector;
import org.tssg.fca.intent.ShortVector;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;

/**
 * This class accepts and handles the local intents from Mappers, produces global intents and then
 * outputs them to combiner.
 * @author Biao Xu
 * @version 1.3
 */
public class MRGanterPlusReduceTask implements ReduceTask
{
	private static Logger logger = Logger.getLogger(MRGanterPlusReduceTask.class);
	//Hodes all the found concepts.
	private TreeMap<Integer, TreeMap<Integer, ArrayList<ShortVector>>> localContainer = 
		new TreeMap<Integer, TreeMap<Integer, ArrayList<ShortVector>>>();
//	private ShortVector allProperties;
	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
//		System.out.println("Concepts " + counter);
		
	}

	@Override
	public void configure(JobConf jobConf, ReducerConf redConf)
			throws TwisterException {
		/*IntValue intValue = (IntValue) redConf.getValue();
		int len = intValue.getVal();
		short[] properties = new short[len];
		for(short i=0; i<len; i++)
		{
			prop
	 * Combine all the new intents from every mapper to get their intersection.
	 * And then store them in to collector.
	 */
	}
	public void reduce(ReduceOutputCollector collector, Key key, List<Value> values)
			throws TwisterException
	{
		logger.info("reduce starts....");
		if(values.size() <= 0)
			throw new TwisterException("Reduce input error no values");
				
		int numMapTasks = values.size();
		IntentsSet fromMap = (IntentsSet)values.get(0);
		int size = fromMap.getSize();
		
		IntentsSet toCollector;
		ShortVector intent, localIntent;
		/*Process the IntentsSet which were marked as blank in <code>driveMapReduce</code> method
		in MRGanterPlus class.*/
		if(fromMap.getFlag())
		{
			intent = new ShortVector();
			
			for(int i=0; i<numMapTasks; i++)
			{
				localIntent = ((IntentsSet)values.get(i)).getIntentsValueAt(0);
				intent.intersect(localIntent);
			}
			toCollector = new IntentsSet(1);
			intent.setElement((short)0);
			toCollector.addConcept(intent);
			toCollector.setFlag(true);
			collector.collect(key, toCollector);
		}
		else
		{
			ShortVector oldIntent = ((IntentKey)key).getKey();
			toCollector = new IntentsSet(size);
			ShortVector currentIntentValue, candidateIntent;
			short element = 0;

			boolean isExisting;
			for(int i = 0; i < size; i++)
			{
				candidateIntent = new ShortVector();
				
				for(int j=0; j<numMapTasks; j++)
				{
					currentIntentValue = ((IntentsSet)values.get(j)).getIntentsValueAt(i);
					if(currentIntentValue.getVarious() == 0)
					{
						//Ignore the intent which consists of all attributes.
//						currentIntentValue = allProperties;
					}
					else if(currentIntentValue.getVarious() == -1)
					{
						//Recover the new intent by adding the seeding intent. 
						currentIntentValue.add(oldIntent.toShortArray());
						candidateIntent.intersect(currentIntentValue);
					}
					else
						candidateIntent.intersect(currentIntentValue);
					element = currentIntentValue.getElement();
					
				}
				
				isExisting = insert(localContainer, candidateIntent);
				if(isExisting == true)
				{
					candidateIntent.setElement((short)(element+1));
					toCollector.addConcept(candidateIntent);
				}
				candidateIntent = null;
			}

			collector.collect(key, toCollector);
		}
	}

	/**
	 * Check if a new intent is already found. If not, insert it and return true.
	 * @param store TreeMap<Integer, TreeMap<Integer, ArrayList<ShortVector>>> intents container
	 * @param intent ShortVector a candidate intent
	 * @return boolean true if the candidate intent is inserted into intents container
	 */
	public boolean insert(TreeMap<Integer, TreeMap<Integer, ArrayList<ShortVector>>> store, 
			ShortVector intent)
	{
		boolean successful = false;
		short[] sh = intent.toShortArray();
		Integer first;
		if(sh.length == 0)
			first = new Integer(-1);
		else
			first = new Integer(sh[0]);
		Integer length = new Integer(intent.size());
		TreeMap<Integer, ArrayList<ShortVector>> currentIntentValues = new TreeMap<Integer, ArrayList<ShortVector>>();
//		ArrayList<ShortVector> newConcepts = new ArrayList<ShortVector>();
		if(!store.containsKey(first))
		{
			ArrayList<ShortVector> newIntentValue = new ArrayList<ShortVector>();
			newIntentValue.add(intent);
			currentIntentValues.put(length, newIntentValue);
			store.put(first, currentIntentValues);
			successful = true;
		}
		else
		{
			currentIntentValues = store.get(first);
			ArrayList<ShortVector> arrayList = new ArrayList<ShortVector>();
			if(!currentIntentValues.containsKey(length))
			{
				arrayList.add(intent);
				currentIntentValues.put(length, arrayList);
				successful = true;
			}
			else
			{
				arrayList = currentIntentValues.get(length);
				if(!arrayList.contains(intent))
				{
					arrayList.add(intent);
					currentIntentValues.put(length, arrayList);
					successful = true;
				}
			}
				
			//Update value with new ValueVector.
			store.put(first, currentIntentValues);
				
		}
		return successful;
	}
	
	public  boolean check(IntVector pIntent, IntVector oldIntent, int pElement)
	{
		boolean result = false;
		int[] intent = pIntent.toIntArray();
		IntVector tmp = new IntVector();
		int intentLen = intent.length;
		
		int[] properties = oldIntent.toIntArray();
		int subsetLen = properties.length;
		if(intentLen != 0)
			if(subsetLen != 0)
			{
				for(int i=0, j=0; i<intentLen; i++)
				{
					int str = intent[i];
					if(j < subsetLen)
					{
						if(str==properties[j])
						{
							j++;
						}
						else
							tmp.add(str);
					}
					else
					{
						tmp.add(str);
					}
				}
				/**
				 * if the smallest new element in <code>intent</code> is not smaller then element,
				 * then return true.
				 */
				if(tmp.getIntAt(0) >= pElement)
					result = true;
			}
			else
			{
				if(intent[0] >= pElement)
					result = true;
			}
		else 
			result = true;

		return result;
	}
}
