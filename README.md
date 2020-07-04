# 代码描述

### 输入输出数据格式：



  A    0.25   :   B,C,D 

  B    0.25   :   A,D  

  C    0.25   :   A  

  D    0.25   :   B,C  



### Map过程

```java
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PRMapper extends Mapper<LongWritable, Text, Text, Text>{
    //mapper的作用是从输入文件中获取转移矩阵的转移概率以及当前PR值
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            /*
                当前页i PRi: 可跳转页1，可跳转页2, ... 
            */
            //将输入按：切开，line[0] = 当前页 Vn  line[1] = 可跳转页1，可跳转页2, ... 
            String[] line = value.toString().split(":"); 
            String nextPage = line[1]; 
            int N = nextPage.split(",").length; //当前页有N个可跳转页
            String nowPage = line[0].split("\\s+")[0];
            String PR = line[0].split("\\s+")[1];
            context.write(new Text(nowPage), new Text("nextPage:"+nextPage));//当前页可跳转的页
            for(String subpage:nextPage.split(",")){
                //转移矩阵中从nowPage跳转到一个subpage的概率
                context.write(new Text(subpage), new Text("M:"+nowPage+" "+String.valueOf(1.0/N))); 
                //当前subpage的PR值
                context.write(new Text(subpage), new Text("PR:"+nowPage+" "+PR));
            }        
    }
}
```

在Map过程中需要处理的任务就是将转移概率和当前PR概率提取出来，并且按照键值对输出。比如第一行A 0.25：B，C，D，对于B，它可以获得从A转移到它的概率1/3，在后面对B的PR值的计算中它还需要用到A的当前PR值，所以还需要获得A的当前PR值0.25，因为PR’[B] = …+ M\[B][A]*PR[A]+…。设计键值对如下：

​      <”subPage”, “PR: nowPage PRvalueForNowPage”>

​      <”subPage”, “M: nowPage MvalueForNowPageToSubPage”>

前者意义就是从subPage 会使用到 nowPage的当前PR值。 后者意义就是从nowPage转移到subPage的转移概率是MvalueForNowPageToSubPage。对于例子中的B，第一行输出的键值对就是：

​      <"B","M: A 0.333">  （B的M键值对）

​      <"B","PR: A 0.25">     (B的PR键值对）

其他的以此类推。当然仅有这两者还不够，因为最后reduce的输出中还需要包含每个网页可以跳转到哪几个网页，所以在map中还需要将这一项原封不动的作为输出输出给reduce。比如对于第一行就可以设计如下键值对：

​      <”A”, “nextPage: B,C,D”>  （nextPage键值对）

##### 因此Map的计算过程如下：

1. 将输入（输入文本的每一行）按照冒号切开得到：

​      A. 当前页及其PR值

​      B. 当前页可跳转的页

2. 输出A的nextPage键值对

3. 将可跳转页按逗号切开，对每个可跳转页，输出该跳转页的M键值对以及PR键值对，其中M键值对中的转移概率的值可以通过可跳转页的数量得到

   

###  Reduce过程

```java
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PRReducer extends Reducer<Text, Text, Text, Text> {
    //对于一个页面KEY，计算其新的PR值
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
       
        Configuration conf = context.getConfiguration();
        double damping = Double.parseDouble(conf.get("damping"));//阻尼系数
        int num = Integer.parseInt(conf.get("num"));//网站数量
        
        Map<String, Double> PR = new HashMap<String,Double>();
        Map<String, Double> M = new HashMap<String,Double>();
        
        String nextPage = "";
        for (Text val : values) {
            String[] line = val.toString().split(":");
            if(line[0].compareTo("nextPage") == 0){
                //取出当前页可跳转的页面方便后面按照mapper输入文件格式输出
                nextPage = line[1];
            }
            else {
                String[] tmp = line[1].split(" ");
                double p = Double.parseDouble(tmp[1]);
                if(line[0].compareTo("PR") == 0){ //取出tmp的PR值
                    PR.put(tmp[0], p);
                }else if(line[0].compareTo("M") == 0){//取出从tmp转移到当前页的转移概率
                    M.put(tmp[0], p);
                }
            }
        }
        double sum=0;
        for(Map.Entry<String,Double> entry: PR.entrySet()){
            sum += M.get(entry.getKey()) * entry.getValue(); //计算PageRank基础模型 sigma(M[key]*PR[key])
        }
        double newPR = damping*sum + (1-damping)/num; //根据公式计算当前页的新PR值

        System.out.println("    ");
        System.out.println(String.valueOf(key) + "'s New PageRank is " + String.valueOf(newPR));
        /*
            按照MAPPER中的输入格式输出
            当前页 当前页的PR值：可跳转页1，可跳转页2，....
        */
        context.write(key, new Text(String.valueOf(newPR)+":"+nextPage));
    }
}
```

在reduce过程中，对于同一个键值，以A为例，它会收到以下键值对：

   <”A”, “nextPage: B,C,D”>

   <”A”, “PR: B 0.25”>

   <”A”, “PR: C 0.25”>

   <”A”, “M: B 0.5”>

   <”A”, “M: C 1”>

reduce的工作就是将上面这些键值对的value按照计算公式整合并输出。对每个键值对判断它是nextPage键值对、M键值对、PR键值对的哪一种， 对于nextPage键值对先保存该值等到最终输出再处理、对于PR键值对，和M键值对则先将它们的value分别存储在HashMap中，比如<”A”, “PR: B 0.25”>， 那么value将按<B, 0.25>的形式被存入一个名为PR的HashMap中，<”A”, “M: B 0.5”>则将value按<B, 0.25>的形式存入一个名为M的HashMap中，这样处理完之后，PR这个HashMap就包含了当前KEY （在这里就是A）计算新PR值需要用到的所有父网页(即能跳转到它的网页)的PR值。而M这个HashMap就包含所用父网页到它的转移概率(关键点)。接下来只需要将两个HashMap中相同键值的value按公式计算在合并就可以了（这一步实际上是PageRank的基本模型计算: PR[i] = sigma（M\[i][j]*PR[j]））。

即有:                  
$$
sum = \sum\limits_{key}M[key]*PR[key]
$$
最后计算优化后的PageRank模型即可:
$$
newPR=damping*sum+(1-damping)/num
$$
其中damping是阻尼系数，num是网站数量。



### 主函数

```java
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PageRank {
    
    public static void main(String[] args) throws Exception {
        double damping=0.85;
        ArrayList<String> ouputList=new ArrayList<String>();
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
            if (otherArgs.length != 3) {
                System.err.println("Usage: PageRank <num> <in> <out>");
                System.exit(2);
            }
        if (check(otherArgs[0]) == false){
            System.err.println("Please use interger as num");
            System.exit(2);
        }
        String num = otherArgs[0];
        String input = otherArgs[1];
        String OriOutput = otherArgs[2];
        String output = otherArgs[2] + "1";

        for(int i = 1; i <= 20; i++){
            Configuration conf = new Configuration();
            conf.set("num", num);
            conf.set("damping", String.valueOf(damping));
            Job job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            input = output;
            ouputList.add(output);
            output = OriOutput + String.valueOf(i+1);
            
            System.out.println("the "+i+"th step is finished");
            job.waitForCompletion(true);
        }

        for(int i=0;i<ouputList.size()-1;i++){ //删除前面的输出文件夹只保留最后一次的
            Configuration conf=new Configuration();
            Path path=new Path(ouputList.get(i));
            FileSystem fs=path.getFileSystem(conf);
            fs.delete(path,true);
        }

    }

    public static boolean check(String str) {
        for (int i = str.length();--i>=0;){  
            if (!Character.isDigit(str.charAt(i))){
                return false;
            }
        }
        return true;
    }
}
```

主函数主要功能就是设置hadoop的任务，其中有两点需要特殊处理：

1. 循环迭代，每次迭代都是一次新的hadoop任务，每次要将上一次迭代的输出路径作为下一次的输入路径。

2. 在前面的reduce过程中需要用到网站的数量如果重新用一个新的mapreduce过程实现网站数量统计会非常的繁琐，因此这里采用直接通过命令行参数传入这个网站数量，所以在主函数中还需要取出这个数量，并通过hadoop的Configuration类自定义变量来存这个值，同理还有阻尼系数值，这样在后面reduce过程中就可以同样通过hadoop的Configuration类取出这两个值来使用。