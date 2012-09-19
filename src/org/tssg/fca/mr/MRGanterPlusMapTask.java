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

import java.io.IOException;

import org.apache.log4j.Logger;
import org.tssg.fca.data.ReadBinaryFromFile;
import org.tssg.fca.intent.IntVector;
import org.tssg.fca.intent.ShortVector;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.StringKey;
/**
 * This class implements MapTask. It loads the static data from local disk and calculates
 * local intents in <code>map</code> method.
 * @author Biao Xu
 * @version 1.3
 */
public class MRGanterPlusMapTask implements MapTask
{
	private static Logger logger = Logger.getLogger(MapTask.class);
	private ReadBinaryFromFile fileReader;
	private FileData dataPartition;
	private int[][] binData;
	private int numColumns;
	private int numData;
	//This vector contains all the attributes
	private short[] mProperties;
	//Store all objects, i.e. line numbers.
	private int[] mObjects;
	/**
	 * It loads the binary data (static data) from file,
	 * and stores the data to be used in Map phase.
	 */
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException
	{
		fileReader = new ReadBinaryFromFile();
		dataPartition = (FileData)mapConf.getDataPartition();
		try {
			fileReader.loadDataFromTextFile(dataPartition.getFileName());
		} catch (IOException e) {
			throw new TwisterException(e);
		}
		
		binData = fileReader.getData();
		numData = fileReader.getNumData();
		numColumns = fileReader.getNumColumns();
		mProperties = new short[numColumns];
		for(short i = 0; i < numColumns; i++)
		{
			mProperties[i] = i;
		}
		mObjects = new int[numData];
		for(int j=0; j<numData; j++)
		{
			mObjects[j]= j;
		}
//		logger.info("numData = " + numData);
//		logger.info("propLen = " + propLen);
	}
	
	/**
	 * This method accepts the <key, value> pairs from Twister driver and 
	 * produces new concepts/intents which will be grouped by key and then be sent to Reducer(s).
	 */
	public void map(MapOutputCollector collector, Key key, Value value) throws TwisterException
	{
		logger.info("map starts....");
		ShortVector newLocalValue;
		ShortVector intent = new ShortVector();		
		IntentsSet intentsSet = (IntentsSet)value;
		int length = intentsSet.getSize();
		ShortVector currentIntent;
		short[] intentData;
		IntentsSet toReduce;
		
		if(intentsSet.getFlag())
		{
			/**
			 * When <code>MRGanterPlus</code> starts, key/value pair is empty.
			 * Only execute this block for initialization of MRGanter+ algorithm.
			 */
			ShortVector emptySet = null;
			intent = computeClosure(emptySet);
			intent.setElement((short)0);
			newLocalValue = new ShortVector(intent.toShortArray());
			toReduce = new IntentsSet(1);
			toReduce.addConcept(newLocalValue);
			toReduce.setFlag(true);
			collector.collect(new StringKey("MRGanterPlus"), toReduce);
		}
		else
		{
			IntentKey intentKey;
			short currentAttri;
			//Traverse IntentsSet
			for(int i=0; i<length; i++)
			{
				toReduce = new IntentsSet(numColumns);
				currentIntent = intentsSet.getIntentsValueAt(i);
				
				intentData = currentIntent.toShortArray();
				//Create key using current intent
				intentKey = new IntentKey(currentIntent);
				currentAttri = currentIntent.getElement();
				//Traverse properties/attributes
				for(short element=(short) (numColumns - 1); element>= currentAttri; element--)
				{
					if(!currentIntent.contain(element))
					{
//						logger.info("element="+ element);
						intent = circlePlus(intentData, element);
						toReduce.addConcept(intent);
					}			
				}
				collector.collect(intentKey, toReduce);
//				logger.info("map.toReduce.size= " + toReduce.getSize());
			}
			intentKey = null;
			toReduce = null;
		}
	}

	/**
	 * Calculate new intent by applying the circleplus operator on the current intent and an property.
	 * @param intentData short[] the intent found in previous iteration.
	 * @param pElement short a property which is not included in <code>intentData</code>.
	 * @return ShortVector candidate intent
	 */
	private ShortVector circlePlus(short[] intentData, short pElement)
	{
		ShortVector intent;
		ShortVector intersection = new ShortVector();

		int len = intentData.length;
		for (int i=0; i< len; i++)
		{
			short str = intentData[i];
			if (str < pElement)
				intersection.add(str);
			else
				break;
		}
		intersection.add(pElement);
		
		intent = computeClosure(intersection);
		if(intent.size() > len && len != 0 && intent.size() != 0)
		{
			/*
			 * Separate the latest found intent from its seeding intent to reduce
			 * intermediate data size. By giving a status '-1', the Reducer(s) could
			 * identify it and recover it.
			 */
			intent.remove(intentData);
			intent.setVarious((short)-1);
		}
	
		intent.setElement(pElement);
		return intent;
	}
	
	/**
	 * Calculate the closure of a property set.
	 * @param pProperties a set of properties
	 * @return ShortVector the minimal superset of <code>pProperties</code>
	 */
	private ShortVector computeClosure(ShortVector pProperties)
	{
		//All the results will be stored in this container.
		ShortVector closures = new ShortVector();
		
		int[] result = new int[numColumns];
		IntVector selectedObjects = computePrime(pProperties);
		int len = selectedObjects.size();
		if(len != 0)
		{
			for (int p=0; p<numColumns; p++) 
			{
				for (int q = 0; q < len; q++) 
				{
					int idx = selectedObjects.getIntAt(q);
					if(binData[idx][p] == 0)
						break;
					else
						result[p]++;
				}
				if(result[p] == len)
				{
					closures.add(mProperties[p]);
				}
			}
			selectedObjects = null;
		}
		else
		{
			/*
			 * In this case, the closure has the most elements, including all properties.
			 * To avoid transferring large arrays, this type of closures are set a status '0'
			 * and they can be given real value at reduce phase if necessary.
			 */
			closures = new ShortVector(0);
			closures.setVarious((short) 0);
		}
		return closures;
	}
	
	/**
	 * Do mapping from property set to object set.
	 * @param pProperties a set of properties
	 * @return IntVector a set of objects
	 */
	private IntVector computePrime(ShortVector pProperties)
	{
		
		IntVector selectedObjects = new IntVector();
		if(pProperties == null || pProperties.size() == 0)
			selectedObjects = new IntVector(mObjects);
		else
		{
			int intentLen = pProperties.size();		
			for(int i=0; i<numData; i++)
			{
				boolean exist = true;
				for(int j=0; j<intentLen; j++)
				{
					
					if(binData[i][pProperties.getShortAt(j)] == 0)
					{
						exist = false;
						break; //This case could save much time.
					}
				}
				
				if(exist == true)
				{
					selectedObjects.add(i);
				}
			}
		}
		return selectedObjects;
	}
	
	/**
	 * 
	 * @param pIntent
	 * @param oldIntent
	 * @param pElement
	 * @return boolean ture if 
	 */
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
	
	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
		
	}
}
