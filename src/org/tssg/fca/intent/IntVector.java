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

package org.tssg.fca.intent;

/**
 * This class defines basic data type for <code>intent</code>, 
 * which is produced in<code>map</code> and <code>reduce</code>.
 * 
 * @author biaoxu
 * @version 1.3
 */
public final class IntVector
{
	private int[] value;
	private int count;
	boolean flag;
	
	public IntVector()
	{
		this(10);
		count = 0;
		flag = true;
	}
	
	public IntVector(int initialSize)
	{
		value = new int[initialSize];
	}
	
	public IntVector(int[] p)
	{
		count = p.length;
		value = p;
	}
	
	public void merge(IntVector val)
	{
//		int len = val.size();
		if(flag == true)
		{
			value = val.toIntArray();
			count = val.toIntArray().length;
		}
		else
		{
			IntVector tmp = new IntVector();
			
			for(int i=0; i<count; i++)
			{
				int prop = value[i];
				if(val.contain(prop))
					tmp.add(prop);
			}
			value = tmp.toIntArray();
			count = tmp.toIntArray().length;
		}
		flag = false;
		
	}
	
	public void add(int str)
	{
		if(str < 0)
			return;
		int oldCapacity = value.length;
		int minCapacity = count + 1;
		if(minCapacity > oldCapacity)
			ensureCapacity(minCapacity, oldCapacity);
		value[count++] = str;
	}
	
	public void remove(int str)
	{
		if(str < 0)
			return;
		for(int i=0; i<count; i++)
		{
			if(value[i]==str)
			{
				System.arraycopy(value, i+1, value, i, count-1);
				value[--count] = -1;
				return;
			}
		}
	}
	
	private void ensureCapacity(int minCapacity, int oldCapacity)
	{
			int oldValue[] = value;
			int newCapacity = oldCapacity << 1;
			value = new int[newCapacity];
			System.arraycopy(oldValue, 0, value, 0, count);
	}
	
	public int size()
	{
		return count;
	}
	
	public boolean contain(int str)
	{
		for(int i=0; i<count; i++)
		{
			if(value[i] == str)
			{
				return true;
			}
		}
		return false;
	}
	
	public int[] toIntArray()
	{
		int[] string = new int[count];
		System.arraycopy(value, 0, string, 0, count);
		return string;
	}
	
	public final int getIntAt(int index)
	{
		if(index < 0)
			return -1;
		else if(index >count)
			return -1;
		else
			return value[index];
	}
}
