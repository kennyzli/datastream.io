#DataStream to power the big data with functional programming


Big data is alwasy awesome. But writing hadoop is terrible with mapper, reducer
combiner. why don't we make our distributed hadoop life easier and give the power back to 
java developers? 
Impired by twitter's scalding, 
 https://github.com/twitter/scalding

scala is nice language to work with but not highly adopted by java development community. 

datatream's purpose is to take advantage the java8 functional programing features 
and make java + hadoop life easier. 

the datastream is based on the cascading framework 
http://www.cascading.org/

for details information shoot me email at kenny.zlee@gmail.com

Here is the example to make the datastream run locally. :)
```java
DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
stream.mapTo("county", "OtherCountry", x -> x + ":newData").writeTo(new URI("data/output/mapped.dat"), ",");
```
