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
import java.util.Iterator;
import java.util.Map;

import org.tssg.fca.intent.ShortVector;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;

/**
 * This class collects the results from Reducer(s) and returns them to Twister driver.
 * @author Biao Xu
 * @version 1.3
 */
public class MRGanterPlusCombiner implements Combiner
{
	private IntentsSet toMain;
	//A container for holding all found intents in current iteration
	ArrayList<ShortVector> globalIntents = new ArrayList<ShortVector>();
	public MRGanterPlusCombiner()
	{
//		System.out.println("Test combine");
		
	}
	/**
	 * Receive outputs from Reducer(s) in the form of <key, value> and then pass them to Twister driver.
	 */
	public void combine(Map<Key, Value> keyValues) throws TwisterException
	{
		Iterator<Key> ite = keyValues.keySet().iterator();
		Key key;
		int size = 0;
		IntentsSet current;
		ShortVector tmp;
		while(ite.hasNext())
		{
			key = ite.next();
			current = (IntentsSet)keyValues.get(key);
			size = current.getSize();
			for(int i=0; i<size; i++)
			{
				tmp = current.getIntentsValueAt(i);
				globalIntents.add(tmp);
			}
		}
		toMain = new IntentsSet(globalIntents);
		
	}

	public void configure(JobConf arg0) throws TwisterException {
		// TODO Auto-generated method stub
	}
	
	public IntentsSet getResults()
	{
//		System.out.println("toMain.size= " +  toMain.getSize());
		return toMain;
	}

}
