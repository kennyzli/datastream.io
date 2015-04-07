#DataStream to power the big data with functional programming


Big data is alwasy awesome. But writing hadoop is terrible with mapper, reducer
combiner. why don't we make our distributed hadoop life easier and give the power back to 
java developers? 
Impired by twitter's scalding, 
 https://github.com/twitter/scalding

scala is nice language to work with but not highly adopted by java development community. 

Datatream's purpose is trying to take advantage of the java8 functional programing features 
and make your big data life easier. 

the datastream is based on the cascading framework 
http://www.cascading.org/

for details information shoot me email at kenny.zlee@gmail.com
as this project is still on early stage, I need to work with team 
to make it pefect. the interface is intend to chang here. I hope we would have
first minor release in few month 

Here is the example to make the datastream run locally. :)
```java
DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
stream.mapTo("county", "OtherCountry", x -> x + ":newData").
writeTo(new URI("data/output/mapped.dat"), ",");
```

Believe me, if might need to write over 1k lines of code to do exact the samething with map reduce to make it run
and couple of hundrends of line of code in cascading as well. 

##About me 
kenny li(zhenqi li) 
[About me]https://www.linkedin.com/profile/view?id=48862722&trk=hp-identity-photo


