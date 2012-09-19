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
 * This class defines <code>intent</code> of a concept in the type of short.
 * Comparing to IntVector, ShortVector could save space and be processed faster.
 * @author Biao Xu
 * @version 1.0
 */
public final class ShortVector
{
	private short[] value;
	private short key;
	private int count;
	boolean flag;
	short various = 1;
	public ShortVector()
	{
		this(1);
		count = 0;
		flag = true;
	}
	
	public ShortVector(int initialSize)
	{
		value = new short[initialSize];
	}
	
	public ShortVector(short[] p)
	{
		count = p.length;
		value = p;
	}
	
	/**
	 * Calculate the intersection of two intents.
	 * @param val ShortVector an intent of a concept.
	 */
	public void intersect(ShortVector val)
	{
//		int len = val.size();
		if(flag == true)
		{
			value = val.toShortArray();
			count = val.toShortArray().length;
		}
		else
		{
			ShortVector tmp = new ShortVector();
			for(int i=0; i<count; i++)
			{
				short prop = value[i];
				if(val.contain(prop))
					tmp.add(prop);
			}
			value = tmp.toShortArray();
			count = tmp.toShortArray().length;
		}
		flag = false;
		
	}
	
	/**
	 * Calculate the union of two intents.
	 * @param val ShortVector an intent of a concept.
	 */
	public void union(ShortVector val)
	{
		if(flag == true)
		{
			value = val.toShortArray();
			count = val.toShortArray().length;
		}
		else
		{
			ShortVector tmp = new ShortVector();
			
			for(int i=0; i<count; i++)
			{
				short prop = value[i];
				if(!val.contain(prop))
					tmp.add(prop);
			}
			value = tmp.toShortArray();
			count = tmp.toShortArray().length;
		}
		flag = false;
	}
	
	public ShortVector unionWithOrder(ShortVector val)
	{
		int len = val.size();
		int size = count + len;
		ShortVector result = new ShortVector(size);
		int i=0, j=0;
		int counter1 = 0;
		int counter2 = 0;
		for(; i<count; i++)
		{
//			System.out.println("ShortVector.unionWithOrder 3 " + i);
			for(; j<len; j++)
			{
				if(this.value[i] <= val.getShortAt(j))
				{
					result.add(value[i]);
					counter1++;
					break;
				}
				else
				{
					result.add(val.getShortAt(j));
					counter2++;
				}
			}
		}
		
		while(counter1 < count )
		{
			
			result.add(value[counter1++]);
		}
		while(counter2 < len)
		{
//			System.out.println("ShortVector.unionWithOrder 4 " + counter2);
			result.add(val.getShortAt(counter2++));
		}
//		System.out.println("ShortVector.unionWithOrder 2 " + result.toString());
		
		return result;
	}
	
	public void add(short str)
	{
		if(str < 0)
			return;
		int oldCapacity = value.length;
		int minCapacity = count + 1;
		if(minCapacity > oldCapacity)
			ensureCapacity(minCapacity, oldCapacity);
		value[count++] = str;
	}
	
	/**
	 * Add array to the current object in ascending order.
	 * @param second
	 */
	public void add(short[] second)
	{
		int len = second.length;
		int index = 0, i, j;
		short[] newValue = new short[count + len];
		for(i=0, j=0; i<count && j<len; )
		{

				if(value[i] > second[j])
				{
					newValue[index++] = second[j];
					j++;
				}
				else if(value[i] < second[j])
				{
					newValue[index++] = value[i];
					i++;
				}

		}
		while(i<count)
		{
			newValue[index++] = value[i++];
		}
		while(j<len)
		{
			newValue[index++] = second[j++];
		}
		count = count + len;
		value = newValue;
	}
	
	public void remove(short str)
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
	
	public void remove(short[] subset)
	{
		int len = subset.length;
		int index = 0, j, tmp = 0;
		if(len > 0)
		{
			short[] newValue = new short[count-len];
			for(int i=0; i<count;)
			{
				for( j=tmp; j<len; )
				{
					if(value[i] > subset[j])
					{
						System.out.println("Exception when removing short!");
						j++;
					}
					else if(value[i] < subset[j])
					{
						newValue[index++] = value[i];
						i++;
						break;
					}
					else if(value[i] == subset[j])
					{
						i++;
						tmp=j + 1;
						break;
					}
				}
				if(j == len)
					newValue[index++] = value[i++];
			}
			count = count - len;
			value = newValue;
		}
		
	}
	
	private void ensureCapacity(int minCapacity, int oldCapacity)
	{
		short oldValue[] = value;
			int newCapacity = oldCapacity << 1;
			value = new short[newCapacity];
			System.arraycopy(oldValue, 0, value, 0, count);
	}
	
	public int size()
	{
		return count;
	}
	
	public boolean lectic(short str)
	{
		boolean result = false;
		if(count > 0)
			if(value[0] >= str)
				result= true;
			else
			{
				for(int i=0; i<count; i++)
				{
					if(value[i] == str)
					{
						result = true;
					}
				}
			}
		
		return result;
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
	
	public short[] toShortArray()
	{
		short[] string = new short[count];
		System.arraycopy(value, 0, string, 0, count);
		return string;
	}
	
	public byte[] toByteArray()
	{
		byte[] string = new byte[count];
		System.arraycopy(value, 0, string, 0, count);
		return string;
	}
	
	public void setElement(short element)
	{
		key = element;
	}
	
	public short getElement()
	{
		return key;
	}
	
	public final short getShortAt(int index)
	{
		if(index < 0)
			return -1;
		else if(index >count)
			return -1;
		else
			return value[index];
	}
	
	public void setVarious(short len)
	{
		various = len;
	}
	
	public short getVarious()
	{
		return various;
	}
	
	public String toString()
	{
		String str = "intent:";
		for(int i=0; i<count; i++)
		{
			str = str + " " + value[i];
		}
		return str;
	}
	
	public boolean equals(Object obj)
	{
		boolean equ = true;
		if(obj instanceof ShortVector)
		{
			short[] arg2 = ((ShortVector)obj).toShortArray();
			if(count == arg2.length)
			{
				for(int i=0; i<count; i++)
				{
					if(value[i] != arg2[i])
					{
						equ = false;
						break;
					}
				}
			}
			else
				equ = false;
		}
		else
			System.out.println("The type of the class is not compatable with ShortVector!");
		return equ;
		
	}
	
	
}
