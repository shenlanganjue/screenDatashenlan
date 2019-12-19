import pandas as  pd
import gzip
import time
import os
import sys
import threading
import configparser
# from pyhdfs import HdfsClient

confiURL = "/wocloud/opdw4_225"

cf = configparser.ConfigParser()
cf.read(confiURL + "/urlConfi.ini",encoding="utf-8-sig")


dataFileURL = cf.get("URL","dateurl")
userListURL = cf.get("URL","userlisturl")
fileCreatURL = cf.get("URL","filecreaturl")

configurationURL = cf.get("URL","urlconfiurl")
proficePathURL = cf.get("URL","provinceurl")

maxtime = float(cf.get("Setting","maxtime"))
maxthread = int(cf.get("Setting","maxthread"))

conditionone = cf.get("Setting","conditionone")
conditiontwo = cf.get("Setting","conditiontwo")


# hadoopURL = cf.get("Setting","hadoopURL")
# hadoopName = cf.get("Setting","hadoopName")

confiFile  = []
threadLock = threading.Lock()
startTicks = 0
endTicks = 0


class myThread(threading.Thread):
    def __init__(self,threadID,name,counter,chunk):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.chunk = chunk


    def run(self) :

        print("begin" + str(self.threadID))
        dispaseChunk(self.chunk)
        print("end" + str(self.threadID))


imsi_data = {}
string_data = {}


#读取配置文件
def readconfigurationFile():
    urlstatisticsArray = []
    sexManArray = []
    sexWoManArray = []



    with open(configurationURL, 'r', encoding="utf-8") as fhandler:
        for line in fhandler:
            try:
                line = line.strip('\n')
                lineArray = line.split(" ")
                if len(lineArray) < 4 :
                    continue
                urlObjc = URLClass(lineArray[0],lineArray[1],lineArray[2],lineArray[3])

                if urlObjc.type == "0" :
                    urlstatisticsArray.append(urlObjc)
                elif urlObjc.type == "1" :
                    sexManArray.append(urlObjc)
                elif urlObjc.type == "2" :
                    sexWoManArray.append(urlObjc)

            except Exception as err:
                continue

    try:
        import operator
    except ImportError:
        cmpfun = lambda x: x.count  # use a lambda if no operator module
    else:
        cmpfun = operator.attrgetter("sort")  # use operator since it's faster than lambda
    urlstatisticsArray.sort(key=cmpfun, reverse=False)

    return urlstatisticsArray,sexManArray,sexWoManArray




def readFile(file_name):
    threadArray = []
    # client = HdfsClient(hosts= hadoopURL, user_name=hadoopName)
    # inputfile = client.open(file_name)

    dataF = pd.read_csv(file_name,sep="|",names=['手机号','位置区编号','CI号码','imei','流量类型','开始时间','结束时间','持续时间','上行流量','下行流量','总流量','网络类型','终端ip','访问ip','状态码','agent','APS','IMSI','SGSNIP','GGSNIP','内容类型','源端口','目的端口','记录标识','合并记录数','url'],index_col=[conditionone,conditiontwo],chunksize=100000,compression="gzip",error_bad_lines= False)
    chunkArray = []

    for j,chunki in enumerate(dataF) :
        print(j)
        ticks = time.time()
        if ticks - startTicks >= 60 * 60 * maxtime or ticks >= endTicks :
            break
        chunkArray.append(chunki)
        if len(chunkArray) == maxthread or len(chunki) <= 99900 :
            for i,chunk in enumerate(chunkArray) :
                thread = myThread(i,"thread" + str(i), i,chunk=chunk)
                thread.start()
                threadArray.append(thread)
            for thread in  threadArray :
                thread.join()

            print("end")
            chunkArray.clear()
            threadArray.clear()

    print(file_name + "-end")
    return

def dispaseChunk(chunk) :
    levels = chunk.index.levels

    #以imei为第一基准建立用户画像
    for imsi in levels[0].values:
        imsiDic = {"imsi":imsi,"urlArray":[]}
        imsiData = chunk.loc[imsi]
        urls = list(set(imsiData.index.values))

        #以url为第二基准建立用户画像
        for url in  urls :
            # 排除url为nan的情况
            if isinstance(url,float):
                continue
            urlData = imsiData.loc[url]

            #可能两种类型 分开处理
            if isinstance(urlData, pd.core.frame.DataFrame):

                urlDic = addIfDataFrame(urlData)
                urlDic["url"] = url
                # imsiDic["deviceType"] =  urlDic["deviceType"]
                # imsiDic["netType"] =  urlDic["netType"]
                imsiDic["urlArray"].append(urlDic)
            else:
                urlDic =addIfNotDataFrame(url,urlData)
                # imsiDic["deviceType"] =  urlDic["deviceType"]
                # imsiDic["netType"] = urlDic["netType"]

                imsiDic["urlArray"].append(urlDic)

        #筛选数组
        imsiDic = screenImsi(imsiDic)

        threadLock.acquire()

        # string_data[str(imsi)] = imsiString
        if imsi_data.get(str(imsi), -1) == -1 :
            imsi_data[str(imsi)] = imsiDic
        else:
            for dic in  imsiDic["urlArray"] :
                index = 0
                for  dic2 in imsi_data[str(imsi)]["urlArray"]:
                    if dic["urlId"] == dic2["urlId"] :
                        dic2["timingNum"] = dic2["timingNum"] + dic["timingNum"]
                        break
                    else :
                        index = index + 1

                if index == len(imsi_data[str(imsi)]["urlArray"]) :
                    imsi_data[str(imsi)]["urlArray"].append(dic)

        threadLock.release()


#非dataframe类型处理
def addIfNotDataFrame(url,urlData):

    return  {"url": url, "timingNum": 1}

def addIfDataFrame(urlData):

    urlDic = {"timingNum": len(urlData)}

    return  urlDic




#配置文件类
class URLClass:
    def __init__(self,serial,url,sort,type):
        self.serial = serial
        self.url = url
        self.sort = sort
        self.type = type


#读取用户对照表配置文件
def readuserListFile():
    userListDict = {}
    for file in os.listdir(userListURL):
        with open(userListURL + "/" + file, 'r', encoding="utf-8") as fhandler:
            for line in fhandler:
                try:
                    line = line.strip('\n')
                    lineArray = line.split(" ")
                    if len(lineArray) < 2 :
                        continue
                    userListDict[lineArray[0]] = lineArray[1]
                except Exception as err:
                    continue
    return userListDict


def screenImsi(imsiDic):
    urlTureArray = []

    for urlObjc in confiFile[0]:
        for dic in imsiDic["urlArray"]:
            if dic["url"].find(urlObjc.url) != -1:

                dic["urlId"] = urlObjc.serial
                urlTureArray.append(dic)
                break


    imsiDic["urlArray"] = urlTureArray

    return imsiDic

def CombiningString(dic) :

    newString = ""
    urlString = ""
    for urlDic in  dic["urlArray"]:
        urlString = urlString + urlDic["urlId"] + ":" + (str(urlDic["timingNum"])) + ","

    newString = newString + urlString

    if len(newString) > 128 :
        newString = newString[0:128]

    return  newString


def writeToFile(dict):
    import os
    userListDict = readuserListFile()

    timeString = time.strftime("%Y-%m-%d", time.localtime())
    if not os.path.exists( fileCreatURL + "/statisticsData"):
        os.makedirs( fileCreatURL + "/statisticsData")
    fout = open( fileCreatURL + "/statisticsData/" + "shenlan" + timeString, 'wb')
    try:
        for (i, key) in enumerate(dict.keys()):
            if userListDict.get(key,-1) != -1 :
                fout.write(userListDict[key].encode())
                fout.write("|".encode())
                fout.write(dict[key].encode())
                fout.write("\n".encode())
    finally:
        fout.close()




def readProviceUrl():
    proviceArray = []
    timeArray = []
    with open(proficePathURL,"r",encoding= "utf-8") as fhandler:
        for line in  fhandler :
            try:
                line = line.strip("\n")
                lineArray = line.split(" ")
                if len(lineArray) < 2:
                    continue
                proviceArray.append(lineArray[0])
                timeArray.append(lineArray[1])

            except Exception as err :
                continue
    return proviceArray,timeArray







if __name__ == '__main__':
    ticks = time.time()
    startTicks = ticks
    endTicks = ticks
    dict = {}
    yerTicks = ticks - 60 * 60 * 24
    timeArray = time.localtime(yerTicks)
    yerTime = time.strftime("%Y%m%d", timeArray)
    confiFile = readconfigurationFile()
    array = readProviceUrl()

    for i,item in enumerate(array[0]) :
        path = dataFileURL + "/day_id=" + yerTime + "/prov_id=" + item
        endTicks = endTicks + int(array[1][i]) * 60
        for file in  os.listdir(path):
            if  file.find(".gz") != -1 :
                readFile(path + "/" + file)

    for key in imsi_data.keys() :
        imsiString = CombiningString(imsi_data[key])
        string_data[key] = imsiString

    writeToFile(string_data)

