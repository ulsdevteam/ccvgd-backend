import json
import os
import re

import mysql.connector
from flask import Blueprint, jsonify, request, session, send_from_directory
from flask_cors import CORS
from status_code import *

advance_blueprint = Blueprint('advancesearch', __name__)
CORS(advance_blueprint)

@advance_blueprint.route("/", methods=["POST", "GET"], strict_slashes=False)
def advanceSearch():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    villageid = json_data.get("villageid")
    topic = json_data.get("topic")
    year = json_data.get("year", None)
    year_range = json_data.get("year_range", None)

    # Get connection
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        port=3307,
        database="CCVG")
    mycursor = mydb.cursor()

    topics = ["village", "gazetteerinformation", "naturaldisasters", "naturalenvironment", "military", "education",
              "economy", "familyplanning",
              "population", "ethnicgroups", "fourthlastNames", "firstavailabilityorpurchase"]
    # get every topics' index in the
    indexes = []
    for i in topic:
        if i not in topics:
            return "topics is not fullfil requirement"
        else:
            indexes.append(topics.index(i) + 1)

    dicts = {}
    table = []

    table1 = {}
    table1["field"] = ["gazetteerId", "gazetteerName", "villageId", "villageName", "province", "city", "county",
                       "category1",
                       "data", "unit"]
    table1["data"] = []
    table1["tableNameChinese"] = "村庄基本信息"
    dicts[1] = table1

    # 村志信息
    table2 = {}
    table2["field"] = ["villageId", "villageName", "gazetteerId", "gazetteerName", "publishYear", "publishType"]
    table2["data"] = []
    table2["tableNameChinese"] = "村志基本信息"
    dicts[2] = table2

    # 自然灾害
    table3 = {}
    table3["field"] = ["gazetteerName", "gazetteerId", "year", "category1"],
    table3["data"] = []
    table3["year"] = []
    table3["tableNameChinese"] = "自然灾害"
    dicts[3] = table3

    table4 = {}
    table4["field"] = ["gazetteerName", "gazetteerId", "category1", "data", "unit"]
    table4["data"] = []
    table4["tableNameChinese"] = "自然环境"
    dicts[4] = table4

    # 军事
    table5 = {}
    table5["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear", "data", "unit"]
    table5["data"] = []
    table5["year"] = []
    table5["tableNameChinese"] = "军事政治"
    dicts[5] = table5

    # 教育
    table6 = {}
    table6["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "startYear", "endYear",
                       "data",
                       "unit"]
    table6["data"] = []
    table6["tableNameChinese"] = "教育"
    table6["year"] = []
    dicts[6] = table6

    # 经济
    table7 = {}
    table7["field"] = ["gazetteerName", "gazetteerId", "category1", "category2", "category3", "startYear", "endYear",
                       "data",
                       "unit"]
    table7["data"] = []
    table7["year"] = []
    table7["tableNameChinese"] = "经济"
    dicts[7] = table7

    # 计划生育
    table8 = {}
    table8["field"] = ["gazetteerName", "gazetteerId", "category", "startYear", "endYear", "data", "unit"]
    table8["data"] = []
    table8["year"] = []
    table8["tableNameChinese"] = "计划生育"
    dicts[8] = table8

    # 人口
    table9 = {}
    table9["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
    table9["data"] = []
    table9["year"] = []
    table9["tableNameChinese"] = "人口"
    dicts[9] = table9

    # 民族
    table10 = {}
    table10["field"] = ["gazetteerName", "gazetteerId", "category1", "startYear", "endYear", "data", "unit"]
    table10["data"] = []
    table10["year"] = []
    table10["tableNameChinese"] = "民族"
    dicts[10] = table10

    table11 = {}
    table11["field"] = ["gazetteerName", "gazetteerId", "firstLastNameId", "secondLastNameId", "thirdLastNameId",
                        "fourthLastNameId",
                        "fifthLastNameId", "totalNumberOfLastNameInVillage"]
    table11["data"] = []
    table11["tableNameChinese"] = "姓氏"
    dicts[11] = table11

    # 第一次购买年份
    table12 = {}
    table12["field"] = ["gazetteerName", "gazetteerId", "category", "year"]
    table12["data"] = []
    table12["year"] = []
    table12["tableNameChinese"] = "第一次拥有或购买年份"
    dicts[12] = table12

    table2func = {}
    table2func[3] = getNaturalDisaster
    table2func[4] = getNaturalEnvironment
    table2func[5] = getMilitary
    table2func[6] = getEduaction
    table2func[7] = getEconomy
    table2func[8] = getFamilyPlanning
    table2func[9] = getPopulation
    table2func[10] = getEthnicgroups
    table2func[11] = getFourthlastName
    table2func[12] = getFirstAvailabilityorPurchase

    # For every node in the village and we want to change
    temp = {}
    for village_id in villageid:
        mycursor.execute(
            "SELECT gazetteerTitle_村志书名 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(village_id))
        gazetteerName = mycursor.fetchone()[0]

        for i in getVillage(mycursor, village_id, gazetteerName)["data"]:
            table1["data"].append(i)

        for i in getGazetteer(mycursor, village_id, gazetteerName)["data"]:
            table2["data"].append(i)

        for index in indexes:
            newTable = dicts[index]  # table3~12
            temp[index] = newTable  # {3: table3...}
            if index == 1 or index == 2:
                continue
            else:
                func = table2func[index]
                res = func(mycursor, village_id, gazetteerName, year, year_range)
                for j in res["data"]:
                    newTable["data"].append(j)  # table3~12["data"].append(i) =>
                if "year" in res.keys():
                    newTable["year"] = res["year"]

    table.append(table1)
    table.append(table2)
    for index in indexes:
        if index == 1 or index == 2:
            continue
        else:
            table.append(temp[index])
    return jsonify(table)


def getVillage(mycursor, village_id, gazetteerName):
    table = {}
    table["data"] = []
    # Get province county villageName city
    mycursor.execute(
        "SELECT p.nameChineseCharacters_省汉字, ci.nameChineseCharacters_市汉字 , co.nameChineseCharacters_县或区汉字, v.nameChineseCharacters_村名汉字  FROM villageCountyCityProvince_村县市省 vccp JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE vccp.gazetteerId_村志代码={};".format(
            village_id))
    allNames = mycursor.fetchone()
    province = allNames[0]
    city = allNames[1]
    county = allNames[2]
    villageName = allNames[3]

    mycursor.execute(
        "SELECT a.data_数据, b.name_名称, c.name_名称 FROM villageGeography_村庄地理 as a ,villageGeographyCategory_村庄地理类 as b, villageGeographyUnit_村庄地理单位 as c WHERE a.villageInnerId_村庄内部代码={} AND a.categoryId_类别代码 = b.categoryId_类别代码 AND a.unitId_单位代码=c.unitId_单位代码".format(
            village_id))
    geographyList = mycursor.fetchall()
    for item in geographyList:
        d = {}
        d["gazetteerId"] = village_id
        d["gazetteerName"] = gazetteerName
        d["villageId"] = village_id
        d["villageName"] = villageName
        d["province"] = province
        d["county"] = county
        d["city"] = city
        d["category1"] = item[1]
        d["data"] = item[0]
        d["unit"] = item[2]
        table["data"].append(d)
    return table


def getGazetteer(mycursor, village_id, gazetteerName):
    mycursor.execute(
        "SELECT yearOfPublication_出版年, publicationType_出版类型 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码={}".format(
            village_id))
    publicationList = mycursor.fetchall()
    table = {}
    table["data"] = []

    mycursor.execute("SELECT nameChineseCharacters_村名汉字 FROM village_村 WHERE gazetteerId_村志代码={}".format(village_id))
    name = mycursor.fetchone()[0]

    for item in publicationList:
        d = {}
        d["villageId"] = village_id
        d["villageName"] = name
        d["gazetteerId"] = village_id
        d["gazetteerName"] = gazetteerName

        d["publishYear"] = item[0]
        d["publishType"] = item[1]
        table["data"].append(d)
    return table


def getNaturalDisaster(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []
    result_dict["year range"] = "natural disaster doesn't have year range option"
    if year != None:
        mycursor.execute("SELECT year_年份 FROM naturalDisasters_自然灾害 WHERE villageInnerId_村庄内部代码={}".format(village_id))
        all_years = mycursor.fetchall()
        all_years = [i[0] for i in all_years]
        for i in year:
            if i not in all_years:
                result_dict["year_only_empty"].append(i)
                if all_years[closest(all_years, i)] not in year:
                    mycursor.execute(
                        "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,"
                        " naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} "
                        "AND a.categoryId_类别代码=b.categoryId_类别代码 AND a.year_年份={}".format(
                            village_id, all_years[closest(all_years, i)]))
                    disasterList = mycursor.fetchall()
                    result_dict["year_only"].append(all_years[closest(all_years, i)])
                    for item in disasterList:
                        d = {}
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["year"] = item[1]
                        d["category1"] = item[0]
                        table["data"].append(d)

            else:
                mycursor.execute(
                    "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,"
                    " naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} "
                    "AND a.categoryId_类别代码=b.categoryId_类别代码 AND a.year_年份={}".format(
                        village_id, i))
                disasterList = mycursor.fetchall()

                result_dict["year_only"].append(i)
                for item in disasterList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["year"] = item[1]
                    d["category1"] = item[0]
                    table["data"].append(d)

        table["year"].append(result_dict)

    else:
        mycursor.execute(
            "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,"
            " naturalDisastersCategory_自然灾害类 as b WHERE villageInnerId_村庄内部代码={} "
            "AND a.categoryId_类别代码=b.categoryId_类别代码".format(
                village_id))

        disasterList = mycursor.fetchall()

        for item in disasterList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["year"] = item[1]
            d["category1"] = item[0]
            table["data"].append(d)

    return table


def getNaturalEnvironment(mycursor, village_id, gazetteerName, year=None, year_range=None):
    table = {}
    table["data"] = []
    mycursor.execute(
        "SELECT a.data_数据, b.name_名称, c.name_名称 FROM naturalEnvironment_自然环境 as a, naturalEnvironmentCategory_自然环境类 as b,naturalEnvironmentUnit_自然环境单位 as c \
        WHERE villageInnerId_村庄内部代码={} AND a.categoryId_类别代码=b.categoryId_类别代码 \
        AND a.unitId_单位代码=c.unitId_单位代码".format(
            village_id))
    naturalList = mycursor.fetchall()

    for item in naturalList:
        d = {}
        d["gazetteerName"] = gazetteerName
        d["gazetteerId"] = village_id
        d["category1"] = item[0]
        d["data"] = item[1]
        d["unit"] = item[2]
        table["data"].append(d)
    return table


def closest(same_year, year):
    answer = []
    for i in same_year:
        answer.append(abs(year - i))
    return answer.index(min(answer))


def getMilitary(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append(result_dict)
            return table

    if year != None:
        mycursor.execute(
            "SELECT startYear_开始年 FROM military_军事 WHERE gazetteerId_村志代码={} AND m.startYear_开始年=m.endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute(
                        "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                        m.endYear_结束年, data_数据, mu.name_名称\
                     FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                     JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                     WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id,
                                                                                                   same_years[idx],
                                                                                                   same_years[idx]))
                    militraryList = mycursor.fetchall()
                    for i, item in enumerate(militraryList):
                        d = {}
                        if item[1] != None:
                            mycursor.execute(
                                "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                            parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                            d["category1"] = parent
                            d["category2"] = item[2]

                        else:
                            d["category1"] = item[2]
                            d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id

                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)

            else:
                result_dict["year_only"].append(i)

                mycursor.execute(
                    "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                    m.endYear_结束年, data_数据, mu.name_名称\
                 FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                 JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                 WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id, i, i))
                militraryList = mycursor.fetchall()
                for i, item in enumerate(militraryList):
                    d = {}
                    if item[1] != None:
                        mycursor.execute(
                            "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                        parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                        d["category1"] = parent
                        d["category2"] = item[2]

                    else:
                        d["category1"] = item[2]
                        d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None:
        mycursor.execute(
            "SELECT m.startYear_开始年, m.endYear_结束年 FROM  military_军事 as m WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute(
                            "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                            m.endYear_结束年, data_数据, mu.name_名称\
                         FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                         JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                         WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id,
                                                                                                       start, end))
                        militraryList = mycursor.fetchall()
                        for i, item in enumerate(militraryList):
                            d = {}
                            if item[1] != None:
                                mycursor.execute(
                                    "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                                parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                                d["category1"] = parent
                                d["category2"] = item[2]

                            else:
                                d["category1"] = item[2]
                                d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)


        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute(
                "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                m.endYear_结束年, data_数据, mu.name_名称\
             FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
             JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
             WHERE gazetteerId_村志代码={} AND m.startYear_开始年={} AND m.endYear_结束年={}".format(village_id, start_year,
                                                                                           end_year))
            militraryList = mycursor.fetchall()
            for i, item in enumerate(militraryList):
                d = {}
                if item[1] != None:
                    mycursor.execute(
                        "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                    parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                    d["category1"] = parent
                    d["category2"] = item[2]

                else:
                    d["category1"] = item[2]
                    d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id

                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range == None and year == None:
        mycursor.execute(
            "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
            m.endYear_结束年, data_数据, mu.name_名称\
         FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
         JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
         WHERE gazetteerId_村志代码={}".format(village_id))
        militraryList = mycursor.fetchall()

        for i, item in enumerate(militraryList):
            d = {}

            if item[1] != None:
                mycursor.execute(
                    "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码={}".format(item[1]))
                parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                d["category1"] = parent
                d["category2"] = item[2]

            else:
                d["category1"] = item[2]
                d["category2"] = "null"  # 没有父类代码 说明本身就是父类
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append(result_dict)
    return table


def getEduaction(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append(result_dict)
            return table

    if year != None:
        mycursor.execute(
            "SELECT startYear_开始年 FROM  education_教育 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                        ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                        ON e.unitId_单位代码=eu.unitId_单位代码 \
                        WHERE e.gazetteerId_村志代码={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(int(village_id),
                                                                                                        same_years[idx],
                                                                                                        same_years[idx]))

                    educationList = mycursor.fetchall()

                    for item in educationList:
                        d = {}
                        if item[1] != None:
                            d["category2"] = "受教育程度 Highest Level of Education"
                        else:
                            d["category2"] = "null"
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category3"] = "null"
                        d["category1"] = item[2]
                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)
            else:
                result_dict["year_only"].append(i)
                mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                        ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                        ON e.unitId_单位代码=eu.unitId_单位代码 \
                                        WHERE e.gazetteerId_村志代码={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                    int(village_id), i, i))

                educationList = mycursor.fetchall()

                for item in educationList:
                    d = {}
                    if item[1] != None:
                        d["category2"] = "受教育程度 Highest Level of Education"
                    else:
                        d["category2"] = "null"
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["category3"] = "null"
                    d["category1"] = item[2]
                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM education_教育 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                                       ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                                       ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                       WHERE e.gazetteerId_村志代码={} AND  e.startYear_开始年={} AND e.endYear_结束年={}".format(
                            int(village_id), start, end))

                        educationList = mycursor.fetchall()

                        for item in educationList:
                            d = {}
                            if item[1] != None:
                                d["category1"] = "受教育程度 Highest Level of Education"
                                d["category2"] = item[2]
                            else:
                                d["category1"] = item[2]
                                d["category2"] = "null"
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                                                   ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                                                   ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                   WHERE e.gazetteerId_村志代码={} AND  e.startYear_开始年={} AND e.endYear_结束年={}".format(
                int(village_id), start_year, end_year))

            educationList = mycursor.fetchall()

            for item in educationList:
                d = {}
                if item[1] != None:
                    d["category2"] = "受教育程度 Highest Level of Education"
                else:
                    d["category2"] = "null"
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category3"] = "null"
                d["category1"] = item[2]
                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range == None and year == None:
        mycursor.execute("SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                ON e.unitId_单位代码=eu.unitId_单位代码 \
                                WHERE e.gazetteerId_村志代码={}".format(int(village_id)))

        educationList = mycursor.fetchall()

        for item in educationList:
            d = {}
            if item[1] != None:
                d["category2"] = "受教育程度 Highest Level of Education"
            else:
                d["category2"] = "null"
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category3"] = "null"
            d["category1"] = item[2]
            d["startYear"] = item[3]
            d["endYear"] = item[4]
            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append(result_dict)

    return table


def getEconomy(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append(result_dict)
            return table

    if year != None:
        mycursor.execute(
            "SELECT startYear_开始年 FROM economy_经济 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                          ON e.unitId_单位代码=eu.unitId_单位代码 \
                          WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(village_id, same_years[idx], same_years[idx]))
                    econmialList = mycursor.fetchall()
                    for item in econmialList:
                        d = {}
                        # the category2 also has two upper layer
                        if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                            mycursor.execute(
                                "SELECT parentId_父类代码,name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(
                                    item[1]))
                            categoryList = mycursor.fetchone()
                            categoryId, d["category2"] = categoryList[0], categoryList[1]

                            mycursor.execute(
                                "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
                            d["category3"] = mycursor.fetchone()[0]

                        # the category2 has no upper layer
                        elif item[1] == None:
                            d["category3"] = "null"
                            d["category2"] = "null"

                        # the category2 has onw upper layers
                        else:
                            mycursor.execute(
                                "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                            d["category2"] = mycursor.fetchone()[0]
                            d["category3"] = "null"

                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category1"] = item[2]
                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)
            else:
                result_dict["year_only"].append(i)

                mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                          ON e.unitId_单位代码=eu.unitId_单位代码 \
                                          WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                    village_id, i, i))
                econmialList = mycursor.fetchall()
                for item in econmialList:
                    d = {}
                    # the category2 also has two upper layer
                    if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                        mycursor.execute(
                            "SELECT parentId_父类代码,name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(
                                item[1]))
                        categoryList = mycursor.fetchone()
                        categoryId, d["category2"] = categoryList[0], categoryList[1]

                        mycursor.execute(
                            "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
                        d["category3"] = mycursor.fetchone()[0]

                    # the category2 has no upper layer
                    elif item[1] == None:
                        d["category3"] = "null"
                        d["category2"] = "null"

                    # the category2 has onw upper layers
                    else:
                        mycursor.execute(
                            "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                        d["category2"] = mycursor.fetchone()[0]
                        d["category3"] = "null"

                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["category1"] = item[2]
                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM  economy_经济 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()
        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                                  e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                                  ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                                  ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                  WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                            village_id, start, end))
                        econmialList = mycursor.fetchall()
                        for item in econmialList:
                            d = {}
                            # the category2 also has two upper layer
                            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                                mycursor.execute(
                                    "SELECT parentId_父类代码,name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(
                                        item[1]))
                                categoryList = mycursor.fetchone()
                                categoryId, d["category2"] = categoryList[0], categoryList[1]

                                mycursor.execute(
                                    "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(
                                        categoryId))
                                d["category3"] = mycursor.fetchone()[0]

                            # the category2 has no upper layer
                            elif item[1] == None:
                                d["category3"] = "null"
                                d["category2"] = "null"

                            # the category2 has onw upper layers
                            else:
                                mycursor.execute(
                                    "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                                d["category2"] = mycursor.fetchone()[0]
                                d["category3"] = "null"

                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["category1"] = item[2]
                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                                              e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                                              ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                                              ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                              WHERE e.gazetteerId_村志代码 ={} AND e.startYear_开始年={} AND e.endYear_结束年={}".format(
                village_id, start_year, end_year))
            econmialList = mycursor.fetchall()
            for item in econmialList:
                d = {}
                # the category2 also has two upper layer
                if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                    mycursor.execute(
                        "SELECT parentId_父类代码,name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(
                            item[1]))
                    categoryList = mycursor.fetchone()
                    categoryId, d["category2"] = categoryList[0], categoryList[1]

                    mycursor.execute(
                        "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(
                            categoryId))
                    d["category3"] = mycursor.fetchone()[0]

                # the category2 has no upper layer
                elif item[1] == None:
                    d["category3"] = "null"
                    d["category2"] = "null"

                # the category2 has onw upper layers
                else:
                    mycursor.execute(
                        "SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                    d["category2"] = mycursor.fetchone()[0]
                    d["category3"] = "null"

                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category1"] = item[2]
                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range == None and year == None:
        mycursor.execute("SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
          ON e.unitId_单位代码=eu.unitId_单位代码 \
          WHERE e.gazetteerId_村志代码 ={}".format(village_id))
        econmialList = mycursor.fetchall()
        for item in econmialList:
            d = {}
            # the category2 also has two upper layer
            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                mycursor.execute(
                    "SELECT parentId_父类代码,name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                categoryList = mycursor.fetchone()
                categoryId, d["category2"] = categoryList[0], categoryList[1]

                mycursor.execute("SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(categoryId))
                d["category3"] = mycursor.fetchone()[0]

            # the category2 has no upper layer
            elif item[1] == None:
                d["category3"] = "null"
                d["category2"] = "null"

            # the category2 has onw upper layers
            else:
                mycursor.execute("SELECT name_名称 FROM economycategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                d["category2"] = mycursor.fetchone()[0]
                d["category3"] = "null"

            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category1"] = item[2]
            d["startYear"] = item[3]
            d["endYear"] = item[4]
            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append(result_dict)
    return table


def getFamilyPlanning(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append(result_dict)
            return table

    if year != None:
        mycursor.execute(
            "SELECT startYear_开始年 FROM  familyplanning_计划生育 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                          f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                          FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                          ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                          ON f.unitId_单位代码=fu.unitId_单位代码 \
                          WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(village_id,same_years[idx],same_years[idx] ))
                    familyplanningList = mycursor.fetchall()
                    for item in familyplanningList:
                        d = {}
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category"] = item[0]
                        d["startYear"] = item[1]
                        d["endYear"] = item[2]
                        d["data"] = item[3]
                        d["unit"] = item[4]
                        table["data"].append(d)

            else:
                result_dict["year_only"].append(i)
                mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                                          f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                          FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                          ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                          ON f.unitId_单位代码=fu.unitId_单位代码 \
                                          WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(
                    village_id, i, i))
                familyplanningList = mycursor.fetchall()
                for item in familyplanningList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["category"] = item[0]
                    d["startYear"] = item[1]
                    d["endYear"] = item[2]
                    d["data"] = item[3]
                    d["unit"] = item[4]
                    table["data"].append(d)
    if year_range != None:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM familyplanning_计划生育 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                                                                  f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                                                  FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                                                  ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                                                  ON f.unitId_单位代码=fu.unitId_单位代码 \
                                                                  WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(
                            village_id, start, end))
                        familyplanningList = mycursor.fetchall()

                        for item in familyplanningList:
                            d = {}
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["category"] = item[0]
                            d["startYear"] = item[1]
                            d["endYear"] = item[2]
                            d["data"] = item[3]
                            d["unit"] = item[4]
                            table["data"].append(d)
        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
                                                                              f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                                                              FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                                                              ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                                                              ON f.unitId_单位代码=fu.unitId_单位代码 \
                                                                              WHERE f.gazetteerId_村志代码 ={} AND f.startYear_开始年={} AND f.endYear_结束年={}".format(
                village_id, start_year, end_year))
            familyplanningList = mycursor.fetchall()

            for item in familyplanningList:
                d = {}
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category"] = item[0]
                d["startYear"] = item[1]
                d["endYear"] = item[2]
                d["data"] = item[3]
                d["unit"] = item[4]
                table["data"].append(d)
    if year_range == None and year == None:
        mycursor.execute("SELECT  fc.name_名称, f.startYear_开始年,\
          f.endYear_结束年,f.data_数据 ,fu.name_名称 \
          FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
          ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
          ON f.unitId_单位代码=fu.unitId_单位代码 \
          WHERE f.gazetteerId_村志代码 ={}".format(village_id))
        familyplanningList = mycursor.fetchall()
        for item in familyplanningList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category"] = item[0]
            d["startYear"] = item[1]
            d["endYear"] = item[2]
            d["data"] = item[3]
            d["unit"] = item[4]
            table["data"].append(d)

    table["year"].append(result_dict)
    return table


def getPopulation(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append(result_dict)
            return table

    if year != None:
        mycursor.execute(
            "SELECT startYear_开始年 FROM population_人口 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                        FROM population_人口 p JOIN populationcategory_人口类 pc \
                        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                        ON p.unitId_单位代码=pu.unitId_单位代码 \
                        WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(village_id, same_years[idx],same_years[idx]))
                    populationList = mycursor.fetchall()
                    for item in populationList:
                        d = {}
                        if item[1] != None:
                            mycursor.execute(
                                "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
                            d["category2"] = mycursor.fetchone()[0]
                        else:
                            d["category2"] = "null"

                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category1"] = item[2]
                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)
                else:
                    result_dict["year_only"].append(i)
                    mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                            p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                            FROM population_人口 p JOIN populationcategory_人口类 pc \
                                            ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                            ON p.unitId_单位代码=pu.unitId_单位代码 \
                                            WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(
                        village_id, i, i))
                    populationList = mycursor.fetchall()
                    for item in populationList:
                        d = {}
                        if item[1] != None:
                            mycursor.execute(
                                "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
                            d["category2"] = mycursor.fetchone()[0]
                        else:
                            d["category2"] = "null"

                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category1"] = item[2]
                        d["startYear"] = item[3]
                        d["endYear"] = item[4]
                        d["data"] = item[5]
                        d["unit"] = item[6]
                        table["data"].append(d)
    if year_range != None:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM population_人口 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()
        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                                                    p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                                                    FROM population_人口 p JOIN populationcategory_人口类 pc \
                                                                    ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                                                    ON p.unitId_单位代码=pu.unitId_单位代码 \
                                                                    WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(
                            village_id, start, end))
                        populationList = mycursor.fetchall()
                        for item in populationList:
                            d = {}
                            if item[1] != None:
                                mycursor.execute(
                                    "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(
                                        item[1]))
                                d["category2"] = mycursor.fetchone()[0]
                            else:
                                d["category2"] = "null"

                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["category1"] = item[2]
                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)
        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                                        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                                        FROM population_人口 p JOIN populationcategory_人口类 pc \
                                                        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                                        ON p.unitId_单位代码=pu.unitId_单位代码 \
                                                        WHERE p.gazetteerId_村志代码 ={} AND p.startYear_开始年={} AND p.endYear_结束年={}".format(
                village_id, start_year, end_year))
            populationList = mycursor.fetchall()
            for item in populationList:
                d = {}
                if item[1] != None:
                    mycursor.execute(
                        "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
                    d["category2"] = mycursor.fetchone()[0]
                else:
                    d["category2"] = "null"

                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category1"] = item[2]
                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range == None and year == None:
        mycursor.execute("SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
        FROM population_人口 p JOIN populationcategory_人口类 pc \
        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
        ON p.unitId_单位代码=pu.unitId_单位代码 \
        WHERE p.gazetteerId_村志代码 ={}".format(village_id))
        populationList = mycursor.fetchall()
        for item in populationList:
            d = {}
            if item[1] != None:
                mycursor.execute("SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码={}".format(item[1]))
                d["category2"] = mycursor.fetchone()[0]
            else:
                d["category2"] = "null"

            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category1"] = item[2]
            d["startYear"] = item[3]
            d["endYear"] = item[4]
            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append(result_dict)
    return table


def getEthnicgroups(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append(result_dict)
            return table

    if year != None:
        mycursor.execute(
            "SELECT startYear_开始年 FROM  ethnicGroups_民族 WHERE gazetteerId_村志代码={} AND startYear_开始年=endYear_结束年".format(
                village_id))
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        for i in year:
            same_years = list(same_years)
            if i not in same_years:
                result_dict["year_only_empty"].append(i)
                idx = closest(same_years, i)

                # make sure there is no duplicate years add
                if same_years[idx] not in year and same_years[idx] not in result_dict["year_only"]:
                    result_dict["year_only"].append(same_years[idx])
                    mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
                          eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                          FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                          ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                          ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                          WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(village_id, same_years[idx],same_years[idx]))
                    ethnicgroupList = mycursor.fetchall()
                    for item in ethnicgroupList:
                        d = {}
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["category1"] = item[0]
                        d["startYear"] = item[1]
                        d["endYear"] = item[2]
                        d["data"] = item[3]
                        d["unit"] = item[4]
                        table["data"].append(d)
            else:
                result_dict["year_only"].append(i)
                mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
                                         eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                         FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                         ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                         ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                         WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                    village_id, i, i))
                ethnicgroupList = mycursor.fetchall()
                for item in ethnicgroupList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["category1"] = item[0]
                    d["startYear"] = item[1]
                    d["endYear"] = item[2]
                    d["data"] = item[3]
                    d["unit"] = item[4]
                    table["data"].append(d)

    if year_range != None:
        mycursor.execute(
            "SELECT startYear_开始年, endYear_结束年 FROM ethnicGroups_民族 WHERE gazetteerId_村志代码={}".format(village_id))
        all_years = mycursor.fetchall()

        if (start_year, end_year) not in all_years:
            result_dict["year_range_empty"].append([start_year, end_year])
            for start, end in all_years:
                # make sure no duplicate
                if start == end and (start in year or start in result_dict["year_only"]):
                    continue
                else:
                    if (start <= start_year and end >= end_year) or \
                            (start <= start_year and end <= end_year and end >= start_year) or \
                            (start >= start_year and end <= end_year) or \
                            (start >= start_year and end >= end_year and start < end_year):
                        result_dict["year_range"].append([start, end])
                        mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
                                                                 eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                                                 FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                                                 ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                                                 ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                                                 WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                            village_id, start, end))
                        ethnicgroupList = mycursor.fetchall()
                        for item in ethnicgroupList:
                            d = {}
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["category1"] = item[0]
                            d["startYear"] = item[1]
                            d["endYear"] = item[2]
                            d["data"] = item[3]
                            d["unit"] = item[4]
                            table["data"].append(d)
        else:
            result_dict["year_range"].append([start_year, end_year])
            mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年, eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                                                             FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                                                             ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                                                             ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                                                             WHERE eth.gazetteerId_村志代码 ={} AND eth.startYear_开始年={} AND eth.endYear_结束年={}".format(
                village_id, start_year, end_year))
            ethnicgroupList = mycursor.fetchall()
            for item in ethnicgroupList:
                d = {}
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["category1"] = item[0]
                d["startYear"] = item[1]
                d["endYear"] = item[2]
                d["data"] = item[3]
                d["unit"] = item[4]
                table["data"].append(d)
    if year_range == None and year == None:

        mycursor.execute("SELECT ethc.name_名称, eth.startYear_开始年,\
          eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
          FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
          ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
          ON eth.unitId_单位代码=ethu.unitId_单位代码 \
          WHERE eth.gazetteerId_村志代码 ={};".format(village_id))
        ethnicgroupList = mycursor.fetchall()
        for item in ethnicgroupList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["category1"] = item[0]
            d["startYear"] = item[1]
            d["endYear"] = item[2]
            d["data"] = item[3]
            d["unit"] = item[4]
            table["data"].append(d)


    table["year"].append(result_dict)
    return table


def getFourthlastName(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    mycursor.execute("SELECT firstlastNamesId_姓氏代码, secondlastNamesId_姓氏代码, thirdlastNamesId_姓氏代码, \
        fourthlastNamesId_姓氏代码, fifthlastNamesId_姓氏代码, totalNumberofLastNamesinVillage_姓氏总数 \
        FROM lastName_姓氏 WHERE gazetteerId_村志代码={}".format(village_id))
    nameList = mycursor.fetchall()

    l = []
    for z in range(len(nameList[0]) - 1):
        if nameList[0][z] == None:
            l.append("")
        else:
            mycursor.execute(
                "SELECT nameChineseCharacters_姓氏汉字 FROM lastNameCategory_姓氏类别 WHERE categoryId_类别代码 ={}".format(
                    nameList[0][z]))
            l.append(mycursor.fetchone()[0])

    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["firstLastNameId"] = l[0]
    d["secondLastNameId"] = l[1]
    d["thirdLastNameId"] = l[2]
    d["fourthLastNameId"] = l[3]
    d["fifthlastNamesId_姓氏代码"] = l[4]
    d["totalNumberOfLastNameInVillage"] = nameList[0][-1]

    table["data"].append(d)


    return table


def getFirstAvailabilityorPurchase(mycursor, village_id, gazetteerName, year, year_range):
    table = {}
    table["data"] = []
    mycursor.execute("SELECT f.year_年份,fc.name_名称 FROM firstAvailabilityorPurchase_第一次购买或拥有年份 f JOIN firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类 fc \
         ON f.categoryId_类别代码=fc.categoryId_类别代码 WHERE f.gazetteerId_村志代码={}".format(village_id))

    firstList = mycursor.fetchall()

    for i in firstList:
        d = {}
        d["gazetteerName"] = gazetteerName
        d["gazetteerId"] = village_id
        d["category"] = i[0] if i[0] != None else None
        d["year"] = i[1] if i[1] != None else None
        table["data"].append(d)

    return table

