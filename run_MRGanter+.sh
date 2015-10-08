#Setting local classpath.
cp=$TWISTER_HOME/bin:.

for i in ${TWISTER_HOME}/lib/*.jar;
  do cp=$i:${cp}
done

for i in ${TWISTER_HOME}/apps/*.jar;
  do cp=$i:${cp}
done

#echo $cp
java -Xmx2048m -Xms512m -XX:SurvivorRatio=10 -classpath  $cp  MRGanterPlus $1 $2
