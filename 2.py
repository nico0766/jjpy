import hashlib
import time,datetime
import http.client
import requests,json,pymysql

import ssl
ssl._create_default_https_context = ssl._create_unverified_context
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# 获取当天日期
todaydate = datetime.datetime.now().strftime('%Y-%m-%d')

appId = '1111'
appKey = '2222'
Api_url = '/open-api/api/v2/finance/salesoperations/listingAnalyzeMultiIndex'
countryID=[40, 43, 44, 42, 41, 48, 50, 51, 49, 52, 26, 29, 30, 28, 27, 4, 8, 7, 5, 6, 21, 22]

# 变量初始化
con = pymysql.connect(
    host='r2222‘,
    user='333',
    passwd='44444**',
    db='555511',
    port=3306,
    charset='utf8'
)


def get_accessToken(appId,appKey):
    url = 'https://prodopenflat.apist.gerpgo.com/open-api/api_token'
    headers = {'content-type':'application/json'}
    Postdata = {'appId':appId,'appKey':appKey}
    ret = requests.post(url,json = Postdata)
    text = json.loads(ret.text)
    accessToken = text['data']['accessToken']
    print("accessToken：", accessToken)
    return accessToken

#获取md5
def get_md5(body_all,appKey):
    body_all = body_all.replace('\n', '') + appKey
    body_all = body_all.replace('	', '')
    body_all = body_all.replace(' ', '')
    m = hashlib.md5(body_all.encode("utf-8"))
    print("MD5加密为:",m.hexdigest())
    return m.hexdigest()

# 获取商品表现
def get_info(accessToken,sign,body_all,Api_url):
    conn = http.client.HTTPSConnection("prodopenflat.apist.gerpgo.com")
    url = Api_url
    headers = {
        'Content-Type': "application/json",
        'accessToken': accessToken,
        'sign': sign
    }
    payloadData = json.loads(body_all.replace("\n","").replace("\t", "").replace("  ","").replace(" ",""))
    payloadData = str(payloadData).replace("\n","").replace("\t", "").replace("  ","").replace(" ","").replace("'",'"')
    print(payloadData)
    #payloadData='{"page":1,"pagesize":500,"sort":"addDate","order":"ascend"}'

    #print(payloadData)
    conn.request("POST", url, payloadData, headers)
    res = conn.getresponse()
    data = res.read()
    #print(data.decode("utf-8"))
    return data.decode("utf-8")


if __name__ == '__main__':
    # 获取游标
    #time.sleep(1)
    cur = con.cursor()
    query = "INSERT INTO listinganalyze (日期, 店铺名字, 国家名字, 品牌名字, 产品名字, 父ASIN, asin, sku, msku, 售价, 平均销售价格, 订单量, 销量, 销售额, 实际销售额, 促销折扣, 退款量, 退货量, 大类排名, 小类排名, 星级, 评论数, sessions, PV, 转化率, 广告曝光量, 广告点击量, 广告订单量, 广告销售额, 广告花费, FBA可售, 调拨中, 处理中, 计划入库, 已发货, 入库中, 佣金, 配送费, FBA配送费, 亚马逊税费, 亚马逊其他费用, VAT税, 销售毛利, 销售净毛利) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    Null = 0
    accessToken = str(get_accessToken(appId, appKey))
    # print(accessToken)
    time.sleep(1)
    count_day = 20  # 统计近20日的数据
    count_check = 1
    jiaoben_rsp_text="# 当前脚本:<font color=\"warning\">**销售统计表 - 近20天**</font>\n"
    while count_check <= count_day:
        time.sleep(1)
        # 获取当前时间
        today = datetime.datetime.now()
        # 计算日期偏移量
        offset = datetime.timedelta(days=-count_check)
        count_check = count_check + 1
        # 获取日期
        re_day = (today + offset).strftime('%Y-%m-%d')
        print(re_day)
        try:
            # 判断当前日期是否存在数据，如果存在数据则清除，否则继续录入
            check_date_query = "SELECT COUNT(*) FROM listinganalyze  WHERE `日期` ='{0}' LiMIT 1".format(re_day)
            dele_query = "DELETE FROM listinganalyze  WHERE `日期` ='{0}'".format(re_day)
            cur.execute(check_date_query)
            count_r = cur.fetchall()
            for a in count_r:
                countres = a[0]  # 获取Count结果数
                if countres > 0:
                    # 如果结果数>0
                    print("存在相同日期，自动删除源数据后自动处理导入")
                    cur.execute(dele_query)
                    body_all = '''{{
                                    "beginDate":"{0}",
                                    "endDate":"{1}",
                                    "groupByType":"asin",
                                    "order":"descend",
                                    "page":1,
                                    "pagesize":500,
                                    "showCurrencyType":"YUAN",
                                    "marketList": {2},
                                    "sort":"unitsOrdered"
                                }}'''.format(re_day, re_day,countryID)
                    sign = str(get_md5(body_all, appKey))
                    # print(sign)
                    #time.sleep(1)
                    listingdata = get_info(accessToken, sign, body_all,Api_url )
                    listingdata = json.loads(listingdata)
                    # print(listingdata['data']['rows'])
                    for i in listingdata['data']['rows']:
                        店铺名字 = i['marketName']
                        国家名字 = i['countryName']
                        品牌名字 = i['brand']
                        产品名字 = i['productName']
                        if 产品名字 is None:
                            产品名字 = "-"
                        else:
                            产品名字 = 产品名字
                        父ASIN = i['variationAsin']
                        asin = i['asin']
                        sku = i['sku']
                        if sku is Null:
                            sku = "-"
                        else:
                            sku = sku
                        msku = i['msku']
                        售价 = i['sellingPrice']
                        平均销售价格 = i['averagePrice']
                        订单量 = i['orders']
                        销量 = i['unitsOrdered']
                        销售额 = i['orderProductSales']
                        实际销售额 = i['discountCostSales']
                        促销折扣 = i['discountCost']
                        退款量 = i['refunds']
                        退货量 = i['returns']
                        大类排名 = i['mainSellerRank']
                        if 大类排名 is None:
                            大类排名 = 0
                        else:
                            大类排名 = 大类排名
                        小类排名 = i['sellerRank']
                        if 小类排名 is None:
                            小类排名 = 0
                        else:
                            小类排名 = 小类排名
                        星级 = i['star']
                        if 星级 is None:
                            星级 = 0
                        else:
                            星级 = 星级
                        评论数 = i['reviewQuantity']
                        if 评论数 is None:
                            评论数 = 0
                        else:
                            评论数 = 评论数
                        sessions = i['sessions']
                        if sessions is None:
                            sessions = 0
                        else:
                            sessions = sessions
                        PV = i['pageViews']
                        if PV is None:
                            PV = 0
                        else:
                            PV = PV
                        转化率 = i['unitSessionPercentage']
                        广告曝光量 = i['adsImpressions']
                        广告点击量 = i['adsClicks']
                        广告订单量 = i['adsOrders']
                        广告销售额 = i['adsSales']
                        广告花费 = i['adsSpend']
                        FBA可售 = i['fbaQuantity']
                        调拨中 = i['reservedTransfers']
                        处理中 = i['reservedTrocessing']
                        计划入库 = i['planStorageQuantity']
                        已发货 = i['shippedQuantity']
                        入库中 = i['receivingQuantity']
                        佣金 = i['commissionCost']
                        配送费 = i['shippingCost']
                        FBA配送费 = i['fbaShippingCost']
                        亚马逊税费 = i['amazonTax']
                        亚马逊其他费用 = i['othersCost']
                        VAT税 = i['vatCost']
                        销售毛利 = i['salesGrossProfit']
                        销售净毛利 = i['salesNetProfit']
                        values = (re_day, 店铺名字, 国家名字, 品牌名字, 产品名字, 父ASIN, asin, sku, msku, 售价, 平均销售价格, 订单量, 销量, 销售额, 实际销售额, 促销折扣, 退款量, 退货量, 大类排名, 小类排名, 星级, 评论数, sessions, PV, 转化率, 广告曝光量, 广告点击量, 广告订单量, 广告销售额, 广告花费, FBA可售, 调拨中, 处理中, 计划入库, 已发货, 入库中, 佣金, 配送费, FBA配送费, 亚马逊税费, 亚马逊其他费用, VAT税, 销售毛利, 销售净毛利)
                        # 执行sql语句
                        cur.execute(query, values)
                    print("导入{0}数据成功".format(re_day))
                    time.sleep(1)

                else:
                    print("没有相同数据源，开始导入")
                    body_all = '''{{
                                                        "beginDate":"{0}",
                                                        "endDate":"{1}",
                                                        "groupByType":"asin",
                                                        "order":"descend",
                                                        "page":1,
                                                        "pagesize":500,
                                                        "showCurrencyType":"YUAN",
                                                        "marketList": {2},
                                                        "sort":"unitsOrdered"
                                                    }}'''.format(re_day, re_day, countryID)
                    sign = str(get_md5(body_all, appKey))
                    # print(sign)
                    # time.sleep(1)
                    listingdata = get_info(accessToken, sign, body_all, Api_url)
                    listingdata = json.loads(listingdata)
                    # print(listingdata['data']['rows'])
                    for i in listingdata['data']['rows']:
                        店铺名字 = i['marketName']
                        国家名字 = i['countryName']
                        品牌名字 = i['brand']
                        产品名字 = i['productName']
                        if 产品名字 is None:
                            产品名字 = "-"
                        else:
                            产品名字 = 产品名字
                        父ASIN = i['variationAsin']
                        asin = i['asin']
                        sku = i['sku']
                        if sku is Null:
                            sku = "-"
                        else:
                            sku = sku
                        msku = i['msku']
                        售价 = i['sellingPrice']
                        平均销售价格 = i['averagePrice']
                        订单量 = i['orders']
                        销量 = i['unitsOrdered']
                        销售额 = i['orderProductSales']
                        实际销售额 = i['discountCostSales']
                        促销折扣 = i['discountCost']
                        退款量 = i['refunds']
                        退货量 = i['returns']
                        大类排名 = i['mainSellerRank']
                        if 大类排名 is None:
                            大类排名 = 0
                        else:
                            大类排名 = 大类排名
                        小类排名 = i['sellerRank']
                        if 小类排名 is None:
                            小类排名 = 0
                        else:
                            小类排名 = 小类排名
                        星级 = i['star']
                        if 星级 is None:
                            星级 = 0
                        else:
                            星级 = 星级
                        评论数 = i['reviewQuantity']
                        if 评论数 is None:
                            评论数 = 0
                        else:
                            评论数 = 评论数
                        sessions = i['sessions']
                        if sessions is None:
                            sessions = 0
                        else:
                            sessions = sessions
                        PV = i['pageViews']
                        if PV is None:
                            PV = 0
                        else:
                            PV = PV
                        转化率 = i['unitSessionPercentage']
                        广告曝光量 = i['adsImpressions']
                        广告点击量 = i['adsClicks']
                        广告订单量 = i['adsOrders']
                        广告销售额 = i['adsSales']
                        广告花费 = i['adsSpend']
                        FBA可售 = i['fbaQuantity']
                        调拨中 = i['reservedTransfers']
                        处理中 = i['reservedTrocessing']
                        计划入库 = i['planStorageQuantity']
                        已发货 = i['shippedQuantity']
                        入库中 = i['receivingQuantity']
                        佣金 = i['commissionCost']
                        配送费 = i['shippingCost']
                        FBA配送费 = i['fbaShippingCost']
                        亚马逊税费 = i['amazonTax']
                        亚马逊其他费用 = i['othersCost']
                        VAT税 = i['vatCost']
                        销售毛利 = i['salesGrossProfit']
                        销售净毛利 = i['salesNetProfit']
                        values = (re_day, 店铺名字, 国家名字, 品牌名字, 产品名字, 父ASIN, asin, sku, msku, 售价, 平均销售价格, 订单量, 销量, 销售额, 实际销售额, 促销折扣, 退款量, 退货量, 大类排名, 小类排名, 星级, 评论数, sessions, PV, 转化率, 广告曝光量, 广告点击量, 广告订单量, 广告销售额, 广告花费, FBA可售, 调拨中, 处理中, 计划入库, 已发货, 入库中, 佣金, 配送费, FBA配送费, 亚马逊税费, 亚马逊其他费用, VAT税, 销售毛利, 销售净毛利)
                        # 执行sql语句
                        cur.execute(query, values)
                    print("导入{0}数据成功".format(re_day))
                jiaoben_rsp_text = jiaoben_rsp_text + "><font color=\"info\">{0} ：成功</font> \n".format(re_day)

        except Exception as e:
            print('Insert error:', e)
            jiaoben_rsp_text = jiaoben_rsp_text + "><font color=\"warning\">{0} ：{1}</font>\n".format(re_day,e)
            con.rollback()
        else:
            con.commit()
    jiaoben_rsp_text = jiaoben_rsp_text +"> 时间: <font color=\"comment\">{0}</font>\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
