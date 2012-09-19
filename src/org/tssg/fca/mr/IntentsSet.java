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
import java.util.ArrayList;
import java.util.Vector;

import org.tssg.fca.intent.ShortVector;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;
/**
 * This class packages the concepts/intents produced during Map/Reduce.
 * It must implements Value interface.
 * @author Biao Xu
 * @version 1.3
 */
public class IntentsSet implements Value
{
	private ShortVector[] intents;
	//Define the capacity of IntentsSet.
	private int capacity;
	//Indicate the current size of IntentsSet.
	private int size;
	//Indicate if InetentsSet is blank and used at initialization stage of MRGanterPlus.
	boolean flag;
	
	public IntentsSet()
	{
		super();
	}
	
	public IntentsSet(ArrayList<ShortVector> cons)
	{
		size = cons.size();
		capacity = size;
		intents = new ShortVector[size];
		int i=0;
		for(ShortVector con: cons)
		{
			intents[i++] = con;
		}
	}
	
	/**
	 * Create a blank IntentsSet.
	 * @param len
	 */
	public IntentsSet(int len)
	{
		intents = new ShortVector[len];
		capacity = len;
		size = 0;
	}
	
	/**
	 * Add concept to IntentsSet.
	 * @param pIntent ShortVector an intent
	 */
	public void addConcept(ShortVector pIntent)
	{
		if(size < capacity)
		{
			
			intents[size++] = pIntent;
//			System.out.println("         addConcept: " + con.toString());
		}
		else
			System.out.println("Can't add more intents to IntentsSet !");
	}
	
	public void addAllConcepts(Vector<ShortVector> cons)
	{
		for(ShortVector con: cons)
		{
			addConcept(con);
		}
	}
	
	public ShortVector[] getIntents()
	{
		return intents;
	}
	
	public ShortVector getIntentsValueAt(int index)
	{
		return intents[index];
	}
	
	public int getSize()
	{
		return size;
	}
	
	public void setFlag(boolean bool)
	{
		flag = bool;
	}
	
	public boolean getFlag()
	{
		return flag;
	}
	
	/**
	 * Serializes the IntentsSet object
	 */
	@Override
	public void fromBytes(byte[] bytes) throws SerializationException
	{
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);
		try{
			capacity = din.readInt();
			size = din.readInt();
			flag = din.readBoolean();
			intents = new ShortVector[size];
			for(int i=0; i<size; i++)
			{
				int len2 = din.readInt();
				short[] inten = new short[len2];
				for(int j=0; j<len2; j++)
				{
					inten[j] = din.readShort();
				}
				intents[i] = new ShortVector(inten);

				short key = din.readShort();
				intents[i].setElement(key);
				intents[i].setVarious(din.readShort());
			}
			din.close();
			baInputStream.close();
		}
		catch(IOException ioe)
		{
			throw new SerializationException(ioe);
		}
	}

	/**
	 * Deserializes the IntentsSet object.
	 */
	@Override
	public byte[] getBytes() throws SerializationException
	{
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;
		try{
			dout.writeInt(capacity);
			dout.writeInt(size);
			dout.writeBoolean(flag);
			ShortVector intent;
			int lenForIntent;
			for(int i=0; i<size; i++)
			{				
				intent = intents[i];
				lenForIntent = intent.size();	
				
				dout.writeInt(lenForIntent);
				for(int k=0; k<lenForIntent; k++)
				{
					dout.writeShort(intent.getShortAt(k));
				}
				
				short attri = intent.getElement();
				dout.writeShort(attri);
				dout.writeShort(intent.getVarious());
			}
			
			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
            baOutputStream.close();
            dout.close();
		}
		catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return marshalledBytes;
	}
	
}
