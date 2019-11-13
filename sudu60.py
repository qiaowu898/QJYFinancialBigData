from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import os
os.environ['JAVA_HOME']='D:\java_for_android\jdk'

def showresult(one):
    print(one)

profit_2=0.0225#2年期收益率
profit_3=0.0275#3年期收益率
profit_5=0.0275#5年期收益率
years=10#分析的年限

if __name__=='__main__':
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    sc = SparkContext(conf=conf)
    lines = sc.textFile('./cases.txt')
    words = lines.flatMap(lambda line:line.split("!"))
    results = words.map(lambda word:(word,handle(word)))#每种投资方案与对应的收益配对
    #result = pairword.reduceByKey(lambda v1,v2:v1+v2)
    results.foreach(lambda result:showresult(result))#对results集合进行遍历输出

def plan_rate(index,length):#获取利率值，index是每个月的投资中的第一项，当length为3时，第一项是2年其投资
    if length==3:
        if index==0:
            return profit_2
        elif index==1:
            return profit_3
        else:
            return profit_5
    else:
        if index==0:
            return profit_3
        else:
            return profit_5

def plan_period(index,length):#获取投资期限，index是每个月的投资中的第一项，当length为3时，第一项是2年其投资
    if length==3:
        if index==0:
            return 24
        elif index==1:
            return 36
        else:
            return 60
    else:
        if index==0:
            return 36
        else:
            return 60

def handle(planstr):
    profits = [years*12]#下标代表月份，值代表收回的本金和利息
    moths = planstr.split(";")
    i=0
    for moth in moths:
      plans = moth.split(",")
      j=0
      for plan in plans:
          money = plan#当前投资方案的金额
          target =i+plan_period(j,plans.length)#目标获得收益的日期
          profit =money*plan_rate(j,plans.length)#获得的利息
          profits[target]=profit+money#本金和利息存入到收回的日期位置
          j+=1
      i+=1
    for t in (years-5)*12:#从第3年开始收益并继续投资5年期
        target=t+60#目标获得收益的月份
        profit=profits[t]*profit_5#获得的利息
        profits[target]=profits[t]+profit#本金和利率存入到目标的月份
    anstr=""#返回的收益字符串
    for k in profits:
        if k!=profits.length-1:
            anstr+=profits[k]+","
        else:
            anstr+=profits[k]+""