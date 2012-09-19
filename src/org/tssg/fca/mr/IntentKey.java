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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.tssg.fca.intent.ShortVector;

import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;

/**
 * This class implements Key interface and supports intermediate data transfer
 * between Mappers and Reducer(s) in the form of key/value pair. When MRGanterPlus 
 * works on an intent X to produce more intents, X is used as Key and the generated
 * local intents based on X are Value.
 * @author Biao Xu
 * @version 1.3
 */
public class IntentKey implements Key
{
	//Use short type to store intent to save 
	private ShortVector intent;
	private int length;
	
	private int hash;
	
	public IntentKey()
	{
		super();
	}
	
	public IntentKey(ShortVector pIntent)
	{
		intent = pIntent;
		length = intent.size();
	}
	
//	public void addKey(IntVector oldIV)
//	{
//		intent[realSize++] = oldIV;
//	}
//	
	public ShortVector getKey()
	{
		return intent;
	}
	
	//Determine the equality of two keys
	public boolean equals(Object key)
	{
		boolean bool = true;
		IntentKey tmpKey = (IntentKey) key;

		int len = tmpKey.getKey().size();
		if(this.length == len)
		{
			for(int i=0; i<len; i++)
			{
				if(intent.getShortAt(i) != tmpKey.getKey().getShortAt(i))
				{
						bool = false;
						break;
				}
			}
		}
		else
			bool = false;

		return bool;
	}
	
	public int hashCode()
	{
		int h = hash;
        int len = length;
        if (h == 0 && len > 0)
        {
        	short val[] = intent.toShortArray();

            for (int i = 0; i < len; i++)
            {
                h = 31*h + val[i];
            }
        }
        return Math.abs(h);
		
	}
	
	/**
	 * Serializes the IntentKey object
	 */
	@Override
	public void fromBytes(byte[] bytes) throws SerializationException
	{
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);
		try {
			length = din.readInt();
			short[] array = new short[length];
			for(int i=0; i<length; i++)
			{
				array[i] = din.readShort();
			}
			intent = new ShortVector(array);
			din.close();
			baInputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Deserializes the IntentKey object.
	 */
	@Override
	public byte[] getBytes() throws SerializationException
	{
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;
		try {

			dout.writeInt(length);
			for(int i=0; i<length; i++)
			{
				dout.writeShort(intent.getShortAt(i));
			}
			
			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
            baOutputStream.close();
            dout.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return marshalledBytes;
	}
	
}
