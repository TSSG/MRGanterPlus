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

package org.tssg.fca.test;

import java.util.ArrayList;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;
import org.tssg.fca.intent.ShortVector;
import org.tssg.fca.mr.MRGanterPlusReduceTask;

public class MRGanterPlusReduceTaskTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testInsert() {
		TreeMap<Integer, TreeMap<Integer, ArrayList<ShortVector>>> store = new TreeMap<Integer, TreeMap<Integer, ArrayList<ShortVector>>>();
		ArrayList<ShortVector> shorts = new ArrayList<ShortVector>();
		
//		short[] s1 = {4,7};
//		short[] s2 = {6};
		short[] s1 = {0,2,4,6};
		short[] s2 = {0,2,3,6};
		short[] s3 = {2,4,5,7};
		short[] s4 = {4};
		short[] s5 = {3,4,6,7};
		short[] s6 = {2};
		short[] s7 = {1,3,4,6,7};
		short[] s8 = {0,3,4,6,7};
		
		short[] s9 = {0,1,2,3,4,5,6,7};
		short[] s10 = {0,1,2,3,4,5,6,7};
		short[] s11 = {0,1,2,3,4,5,6,7};
		short[] s12 = {0,1,2,3,4,5,6,7};
		short[] s13 = {0,1,2,3,4,5,6,7};
		
		short[] s14 = {2,4,7};
		short[] s15 = {2,6};
		short[] s16 = {2,4,5,7};
		short[] s17 = {2,4};
		short[] s18 = {2,3,4,6,7};
		
		short[] s19 = {0,1,2,3,4,5,6,7};
		
		short[] s20 = {4,7};
		short[] s21 = {4,6};
		short[] s22 = {2,4,5,7};
		
		short[] s23 = {3,4,6,7};
		
		ShortVector sv1 = new ShortVector(s1);
		ShortVector sv2 = new ShortVector(s2);
		ShortVector sv3 = new ShortVector(s3);
		ShortVector sv4 = new ShortVector(s4);
		ShortVector sv5 = new ShortVector(s5);
		ShortVector sv6 = new ShortVector(s6);
		ShortVector sv7 = new ShortVector(s7);
		ShortVector sv8 = new ShortVector(s8);
		ShortVector sv9 = new ShortVector(s9);
		ShortVector sv10 = new ShortVector(s10);
		ShortVector sv11 = new ShortVector(s11);
		ShortVector sv12 = new ShortVector(s12);
		ShortVector sv13 = new ShortVector(s13);
		ShortVector sv14 = new ShortVector(s14);
		ShortVector sv15 = new ShortVector(s15);
		ShortVector sv16 = new ShortVector(s16);
		ShortVector sv17 = new ShortVector(s17);
		ShortVector sv18 = new ShortVector(s18);
		ShortVector sv19 = new ShortVector(s19);
		ShortVector sv20 = new ShortVector(s20);
		ShortVector sv21 = new ShortVector(s21);
		ShortVector sv22 = new ShortVector(s22);
		ShortVector sv23 = new ShortVector(s23);
		
		shorts.add(sv1);
		shorts.add(sv2);
		shorts.add(sv3);
		shorts.add(sv4);
		shorts.add(sv5);
		shorts.add(sv6);
		shorts.add(sv7);
		shorts.add(sv8);
		shorts.add(sv9);
		shorts.add(sv10);
		shorts.add(sv11);
		shorts.add(sv12);
		shorts.add(sv13);
		shorts.add(sv14);
		shorts.add(sv15);
		shorts.add(sv16);
		shorts.add(sv17);
		shorts.add(sv18);
		shorts.add(sv19);
		shorts.add(sv20);
		shorts.add(sv21);
		shorts.add(sv22);
		shorts.add(sv23);
		MRGanterPlusReduceTask main = new MRGanterPlusReduceTask();
		main.insert(store, new ShortVector());
		for(ShortVector sv : shorts)
		{
			main.insert(store, sv);
			
		}
		
	}

}
