import json
import os
import re
import csv
import mysql.connector


import pandas as pd
import pymysql

from werkzeug.routing import BaseConverter
from flask import Blueprint, jsonify, request, session, send_from_directory, send_file
from flask_cors import CORS

from config import *
from .utils import get_dicts
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

    if villageid == None or topic == None:
        return jsonify({"code": 4001, "message": "No village id or topic please try again"})

    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)
    mycursor = conn.cursor()

    # mydb = mysql.connector.connect(
    #     host=mysql_host,
    #     user=mysql_username,
    #     password=mysql_password,
    #     port=mysql_port,
    #     database=mysql_database)
    # mycursor = mydb.cursor()

    topics = ["village", "gazetteerinformation", "naturaldisasters", "naturalenvironment", "military", "education",
              "economy", "familyplanning",
              "population", "ethnicgroups", "fourthlastNames", "firstavailabilityorpurchase", "economyunity"]
    indexes = []
    for i in topic:
        if i not in topics:
            return "topics is not fullfil requirement"
        else:
            indexes.append(topics.index(i) + 1)

    table = []
    dicts = get_dicts()

    # print("dicts",dicts)
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

    table2func = {}
    table2func[3] = getNaturalDisaster
    table2func[4] = getNaturalEnvironment
    table2func[5] = getMilitary
    table2func[6] = getEduaction
    table2func[7] = getEconomy_ori
    table2func[8] = getFamilyPlanning
    table2func[9] = getPopulation
    table2func[10] = getEthnicgroups
    table2func[11] = getFourthlastName
    table2func[12] = getFirstAvailabilityorPurchase
    table2func[13] = getEconomy

    # For every node in the village and we want to change
    temp = {}
    for village_id in villageid:
        sql = "SELECT villageId_村庄代码 FROM village_村 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
        village_id_12 = mycursor.fetchone()[0]

        sql = "SELECT gazetteerTitle_村志书名 FROM gazetteerInformation_村志信息 WHERE gazetteerId_村志代码=%s"
        mycursor.execute(sql, village_id)
        result = mycursor.fetchone()
        if result == None:
            return jsonify(
                {"code": 4004, "message": "The village id {} is not exsit! Please change.".format(village_id)})

        gazetteerName = result[0]

        for i in getVillage(mycursor, village_id, gazetteerName, village_id_12)["data"]:

            table1["data"].append(i)
        for i in getGazetteer(mycursor, village_id, gazetteerName, village_id_12)["data"]:
            table2["data"].append(i)

        village_year = []
        for index in indexes:
            # print("dicts[index]",dicts[index])
            newTable = dicts[index]  # table3~12
            temp[index] = newTable  # {3: table3...}
            if index == 1 or index == 2:
                continue
            else:
                func = table2func[index]

                # this res is table in every func's return
                res = func(mycursor, village_id, gazetteerName, year, year_range, village_id_12)
                if res == []:
                    return {}
                for j in res["data"]:
                    newTable["data"].append(j)  # table3~12["data"].append(i) =>

                if "year" in res.keys():

                    for y in res["year"]:
                        village_year = res["year"]
                        newTable["year"].append({village_id: village_year})

    # table.append(table1)
    # table.append(table2)
    for index in indexes:
        if index == 1 or index == 2:
            continue
        else:
            table.append(temp[index])

    # get current working dir
    path = os.getcwd()
    topic2chinese = {"村庄基本信息": "village",
                     "村志基本信息": "gazetteerinformation",
                     "自然灾害": "naturaldisasters",
                     "自然环境": "naturalenvironment",
                     "军事政治": "military",
                     "教育": "education",
                     "经济原始数据": "economy",
                     "计划生育": "familyplanning",
                     "人口": "population",
                     "民族": "ethnicgroups",
                     "姓氏": "fourthlastNames",
                     "第一次拥有或购买年份": "firstavailabilityorpurchase",
                     "统一单位经济": "economyunity"}

    village_id_title = ""
    # table.insert(0, table2)
    for item in topic:
        village_id_title = villageid[0]
        for i in villageid[1:]:
            village_id_title += "_" + i
        try:
            f = open(os.path.join(path, "app_func", "multiple_csv", "{}_{}.csv".format(village_id_title, item)), 'w',
                     encoding='utf-8')
        except OSError as exc:
            f = open(os.path.join(path, "app_func", "multiple_csv",
                                  "{}_{}_{}.csv".format(villageid[0], villageid[-1], item)), 'w',
                     encoding='utf-8')
            print("csv name", "{}_{}_{}.csv".format(villageid[0], villageid[-1], item))

        csv_writer = csv.writer(f)
        temp_table = []

        if item == "village":
            table.insert(0, table1)

            for j in table:
                print(j.get('data'))
                if topic2chinese[j["tableNameChinese"]] == item:

                    temp_table = j
                else:
                    continue
            title = [i for i in temp_table["field"]]
            print(title)

            if len(title) == 1:
                title = title[0]
            csv_writer.writerow(title)
            #
            # # print(temp_table["data"])
            #
            for item in temp_table["data"]:

                temp_l = []
                for ti in title:
                    temp_l.append(item[ti])
                csv_writer.writerow(temp_l)
        elif item == "gazetteerinformation":
            # print(table)
            table.insert(0, table2)

            for j in table:
                print(j.get('data'))
                if topic2chinese[j["tableNameChinese"]] == item:

                    temp_table = j
                else:
                    continue
            title = [i for i in temp_table["field"]]
            print(title)

            if len(title) == 1:
                title = title[0]
            csv_writer.writerow(title)
            #
            # # print(temp_table["data"])
            #
            for item in temp_table["data"]:

                temp_l = []
                for ti in title:
                    temp_l.append(item[ti])
                csv_writer.writerow(temp_l)

        # elif item == "gazetteerinformation":
        #     if "village" not in topic:
        #         table.insert(0, table2)
        #     else:
        #         table.insert(1, table2)
        else:
            for j in table:
                if topic2chinese[j["tableNameChinese"]] == item:
                    temp_table = j
                else:
                    continue
            title = [i for i in temp_table["field"]]

            if len(title) == 1:
                title = title[0]
            csv_writer.writerow(title)

            # print(temp_table["data"])

            for item in temp_table["data"]:

                temp_l = []
                for ti in title:
                    temp_l.append(item[ti])
                csv_writer.writerow(temp_l)

    mearge_csv(topic, village_id_title)

    # return jsonify({"code":200,"table":table})
    return jsonify(table)

@advance_blueprint.route("/search", methods=["POST"], strict_slashes=False)
def searchBylocation():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    city = json_data.get("city", None)
    province = json_data.get("province", None)
    county = json_data.get("county", None)
    village_name = json_data.get("village_name", None)

    if city is None and province is None and county is None and village_name is None:
        return jsonify({"code": 400, "message": "No data in request body please check!"})

    table = {}
    table["data"] = []
    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)
    mycursor = conn.cursor()
    nameList = []
    city_sql = "SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字,  city.nameChineseCharacters_市汉字,\
     province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
     FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON \
     county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 JOIN \
      city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province \
      ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN village_村 village \
      ON village.villageInnerId_村庄内部代码 = jointable.villageInnerId_村庄内部代码 WHERE \
      city.nameChineseCharacters_市汉字= %s;"

    province_sql = "SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, \
    city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
         FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
        JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN \
         village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 WHERE province.nameChineseCharacters_省汉字 = %s;"

    county_sql = "SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
         FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
        JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN \
         village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 WHERE county.nameChineseCharacters_县或区汉字 = %s;"

    village_sql = "SELECT village.nameChineseCharacters_村名汉字, county.nameChineseCharacters_县或区汉字, city.nameChineseCharacters_市汉字, province.nameChineseCharacters_省汉字, jointable.villageInnerId_村庄内部代码 \
         FROM county_县 county JOIN villageCountyCityProvince_村县市省 jointable ON county.countyDistrictId_县或区代码=jointable.countyDistrictId_县或区代码 \
        JOIN  city_市 city ON city.cityId_市代码 = jointable.cityId_市代码 JOIN province_省 province ON province.provinceId_省代码=jointable.provinceId_省代码 JOIN \
         village_村 village ON village.villageInnerId_村庄内部代码= jointable.villageInnerId_村庄内部代码 WHERE village.nameChineseCharacters_村名汉字 = %s;"

    location2sql = {0: city_sql, 1: province_sql, 2: county_sql, 3: village_sql}
    location_list = [city, province, county, village_name]

    for idx, item in enumerate(location_list):
        if item is not None:
            print(type(item))
            print(item)
            mycursor.execute(location2sql[idx], item)
            temp_list = mycursor.fetchall()
            nameList.append(temp_list)

    for temp_list in nameList:
        for item in temp_list:
            temp = {}
            temp["name"] = item[0]
            temp["county"] = item[1]
            temp["city"] = item[2]
            temp["province"] = item[3]
            temp["id"] = str(int(float(item[4])))
            table["data"].append(temp)

    return jsonify(table)


def mearge_csv(topics, village_id_title):
    csv_list = []
    dir_path = os.getcwd()
    mutiple_dir = os.path.join(dir_path, "app_func", "multiple_csv")
    for i in topics:
        csv_list.append(os.path.join(mutiple_dir, village_id_title + "_" + i + ".csv"))
    outputfile = os.path.join(mutiple_dir, village_id_title + ".csv")

    for inputfile in csv_list:
        try:
            f = open(inputfile, encoding="utf-8")
        except OSError as exc:
            start_id = village_id_title.split("_")[0]
            end_id = village_id_title.split("_")[-1]
            sub_title = inputfile.split("_")[-1]
            f = open(os.path.join(mutiple_dir, "{}_{}_{}".format(start_id, end_id, sub_title)), encoding='utf-8')
        try:
            data = pd.read_csv(f)
            data.to_csv(outputfile, mode='a', index=False, header=None)
        except Exception as e:
            print(e)


@advance_blueprint.route("/get_all_village_id", methods=["POST", "GET"], strict_slashes=False)
def getAllVillageId():
    if request.method == "GET":
        mydb = mysql.connector.connect(
            host=mysql_host,
            user=mysql_username,
            password=mysql_password,
            port=mysql_port,
            database=mysql_database)
        mycursor = mydb.cursor()
        mycursor.execute("SELECT gazetteerId_村志代码 FROM gazetteerInformation_村志信息 limit 100 ")
        id_list = mycursor.fetchall()
        final_list = []
        for i in id_list:
            final_list.append(i[0])
        return jsonify(final_list)
    else:
        data = request.get_data()
        data = json.loads(data.decode("utf-8"))
        begin_num = int(data.get("begin_num", None))
        num_range = int(data.get("num_range", None))
        if num_range is None:
            num_range = 10
        if begin_num is None:
            begin_num = 1
        mydb = mysql.connector.connect(
            host=mysql_host,
            user=mysql_username,
            password=mysql_password,
            port=mysql_port,
            database=mysql_database)
        mycursor = mydb.cursor()
        sql = "SELECT gazetteerId_村志代码 FROM gazetteerInformation_村志信息 limit %s,%s"

        mycursor.execute(sql, (begin_num, num_range))
        id_list = mycursor.fetchall()
        final_list = []
        for i in id_list:
            final_list.append(i[0])
        return jsonify(final_list)

def get_full_name(topic, cat):

    topic2tablename = {"economy": "economyCategory_经济类",
                     "education": "educationCategory_教育类",
                     "ethnicgroups": "ethnicGroupsCategory_民族类",
                     "familyplanning": "familyplanningcategory_计划生育类",
                     "military": "militarycategory_军事类",
                     "population": "populationcategory_人口类",
                     "naturaldisasters":"naturalDisastersCategory_自然灾害类"}

    table_name = topic2tablename[topic]
    conn = pymysql.connect(host=mysql_host,
                           user=mysql_username,
                           password=mysql_password,
                           port=mysql_port,
                           database=mysql_database)
    mycursor = conn.cursor()

    sql = "SELECT name_名称 FROM {}".format(table_name)
    mycursor.execute(sql)
    categories = mycursor.fetchall()

    for item in categories:
        if cat in item[0]:
            return item[0]
    return None



@advance_blueprint.route("/download/", methods=["GET"], strict_slashes=False)
def downloadData():

    write_in = "temp_write.csv"
    None_ = pd.DataFrame(None)
    None_.to_csv(write_in, mode="w", header=0)

    dir_path = os.getcwd()
    village_id = request.args.get("village")
    topic = request.args.get("topic", None)
    category = request.args.get("category", None)

    multiple_dir = os.path.join(dir_path, "app_func", "multiple_csv")

    village_id_list = village_id.split(",")
    first_item = village_id_list[0]
    # print("village_id_list",village_id_list)
    if len(village_id_list) == 1:
        village_id_path = first_item

    else:
        village_id_path = first_item
        for j in village_id_list[1:]:
            village_id_path += "_" + j

    if topic == None:
        path = village_id_path
    elif topic != None and category == None:
        path = village_id_path + "_" + topic
    else:
        path = village_id_path + "_" + topic
        print(os.path.join(multiple_dir, path + ".csv"))
        data = pd.read_csv(os.path.join(multiple_dir, path + ".csv"))
        data_head = list(data[0:0])
        print(data_head)
        index_list = []
        cate_list = []
        res_temp = None
        for i in data_head:
            if "category" in i:
                cate_list.append(i)
                print("we add into cate_list")
        print(cate_list)
        for cat in category.split(","):

            cat = get_full_name(topic, cat)
            print("cat is ", type(cat))
            if cat == None: return jsonify({"code": 4010, "message": "The input category is wrong please check!"})
            for subcat in cate_list:
                try:
                    print(data[subcat])
                    res_temp = data[subcat].astype(str).str.contains(cat, na=False)
                except Exception as e:
                    print(e)

                temp_list = list(data[res_temp].index)
                index_list = list(set(index_list + temp_list))

        for i in index_list:
            data[i:i + 1].to_csv(write_in, mode="a", header=0)
        return send_file(write_in, as_attachment=True)


    print("multiple_dir", os.path.join(multiple_dir, path))
    if os.path.exists(os.path.join(multiple_dir, path + ".csv")):
        return send_from_directory(multiple_dir, path + ".csv", as_attachment=True)

    return jsonify({"code": 4003, "message": "File is not exist or file can't download"})


def getVillage(mycursor, village_id, gazetteerName, village_id_12):
    table = {}
    table["data"] = []
    # Get province county villageName city
    sql = "SELECT p.nameChineseCharacters_省汉字, ci.nameChineseCharacters_市汉字 , co.nameChineseCharacters_县或区汉字,\
     v.nameChineseCharacters_村名汉字  FROM villageCountyCityProvince_村县市省 vccp \
     JOIN village_村 v ON vccp.gazetteerId_村志代码=v.gazetteerId_村志代码 JOIN city_市 ci ON\
      vccp.cityId_市代码=ci.cityId_市代码 JOIN county_县 co ON co.countyDistrictId_县或区代码=vccp.countyDistrictId_县或区代码 \
      JOIN province_省 p ON p.provinceId_省代码=vccp.provinceId_省代码 WHERE vccp.gazetteerId_村志代码= %s;"
    mycursor.execute(sql, village_id)
    allNames = mycursor.fetchone()
    if allNames is None or len(allNames) == 0:
        return table
    province = allNames[0]
    city = allNames[1]
    county = allNames[2]
    villageName = allNames[3]

    sql = "SELECT a.data_数据, b.name_名称, c.name_名称 FROM villageGeography_村庄地理 as a ,\
    villageGeographyCategory_村庄地理类 as b, villageGeographyUnit_村庄地理单位 as c WHERE a.gazetteerId_村志代码= %s\
     AND a.categoryId_类别代码 = b.categoryId_类别代码 AND a.unitId_单位代码=c.unitId_单位代码 AND a.data_数据 IS NOT NULL"
    mycursor.execute(sql, village_id)
    geographyList = mycursor.fetchall()
    for item in geographyList:
        d = {}
        d["gazetteerId"] = village_id
        d["gazetteerName"] = gazetteerName
        d["villageId"] = village_id_12
        d["villageName"] = villageName
        d["province"] = province
        d["county"] = county
        d["city"] = city
        d["category1"] = item[1]
        d["data"] = item[0]
        d["unit"] = item[2]
        table["data"].append(d)
    return table


def getGazetteer(mycursor, village_id, gazetteerName, village_id_12):
    sql = "SELECT yearOfPublication_出版年, publicationType_出版类型 FROM gazetteerInformation_村志信息 WHERE \
    gazetteerId_村志代码= %s"
    mycursor.execute(sql, village_id)
    publicationList = mycursor.fetchall()
    table = {}
    table["data"] = []

    sql = "SELECT nameChineseCharacters_村名汉字 FROM village_村 WHERE gazetteerId_村志代码= %s"
    mycursor.execute(sql, village_id)
    name = mycursor.fetchone()[0]

    for item in publicationList:
        d = {}
        d["villageId"] = village_id
        d["villageName"] = name
        d["gazetteerId"] = village_id_12
        d["gazetteerName"] = gazetteerName

        d["publishYear"] = item[0]
        d["publishType"] = item[1]
        table["data"].append(d)
    return table


def getNaturalDisaster(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []

    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []
    result_dict["year range"] = "natural disaster doesn't have year range option"
    if year != None:
        sql = "SELECT year_年份 FROM naturalDisasters_自然灾害 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
        all_years = mycursor.fetchall()
        all_years = [i[0] for i in all_years]
        for i in year:
            if i not in all_years:
                result_dict["year_only_empty"].append(i)
                if all_years[closest(all_years, i)] not in year:
                    sql = "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,\
                     naturalDisastersCategory_自然灾害类 as b WHERE gazetteerId_村志代码= %s \
                        AND a.categoryId_类别代码=b.categoryId_类别代码 AND a.year_年份= %s"

                    mycursor.execute(sql, (village_id, all_years[closest(all_years, i)]))

                    disasterList = mycursor.fetchall()
                    result_dict["year_only"].append(all_years[closest(all_years, i)])
                    for item in disasterList:
                        d = {}
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["villageId"] = village_id_12
                        d["year"] = item[1]
                        d["category1"] = item[0]
                        table["data"].append(d)

            else:
                sql = "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,\
                 naturalDisastersCategory_自然灾害类 as b WHERE gazetteerId_村志代码=%s\
                 AND a.categoryId_类别代码=b.categoryId_类别代码 AND a.year_年份=%s"

                mycursor.execute(sql, (village_id, i))
                disasterList = mycursor.fetchall()

                result_dict["year_only"].append(i)
                for item in disasterList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["year"] = item[1]
                    d["category1"] = item[0]
                    table["data"].append(d)

        table["year"].append({"naturaldisaster": result_dict})

    else:
        sql = "SELECT b.name_名称, a.year_年份 FROM naturalDisasters_自然灾害 as a,\
         naturalDisastersCategory_自然灾害类 as b WHERE gazetteerId_村志代码= %s \
         AND a.categoryId_类别代码=b.categoryId_类别代码"
        mycursor.execute(sql, village_id)

        disasterList = mycursor.fetchall()

        for item in disasterList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["year"] = item[1]
            d["category1"] = item[0]
            table["data"].append(d)

    return table


def getNaturalEnvironment(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    sql = "SELECT a.data_数据, b.name_名称, c.name_名称 FROM naturalEnvironment_自然环境 as a, \
    naturalEnvironmentCategory_自然环境类 as b,naturalEnvironmentUnit_自然环境单位 as c \
        WHERE gazetteerId_村志代码= %s AND a.categoryId_类别代码=b.categoryId_类别代码 \
        AND a.unitId_单位代码=c.unitId_单位代码 AND a.data_数据 IS NOT NULL"
    mycursor.execute(sql, village_id)
    naturalList = mycursor.fetchall()

    for item in naturalList:
        d = {}
        d["gazetteerName"] = gazetteerName
        d["gazetteerId"] = village_id
        d["villageId"] = village_id_12

        d["data"] = item[0]
        d["category1"] = item[1]
        d["unit"] = item[2]
        table["data"].append(d)
    return table


def closest(same_year, year):
    answer = []
    for i in same_year:
        answer.append(abs(year - i))
    # print("same years",same_year)

    return answer.index(min(answer))


def getMilitary(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"military": result_dict})
            return table

    if year != None and year != []:

        sql = "SELECT startYear_开始年 FROM military_军事 as m WHERE gazetteerId_村志代码= %s AND m.startYear_开始年=m.endYear_结束年"
        mycursor.execute(sql, village_id)
        same_year = mycursor.fetchall()
        same_years = set()

        for i in same_year:
            same_years.add(i[0])
        # print("same_years", same_years)

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        if overlap == []:
            result_dict["year_only_empty"] = year
            return []
        else:
            for i in overlap:
                result_dict["year_only"].append(i)
                sql = "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                    m.endYear_结束年, data_数据, mu.name_名称\
                 FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                 JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                 WHERE gazetteerId_村志代码=%s AND m.startYear_开始年=%s AND m.endYear_结束年= %s AND data_数据 IS NOT NULL"
                mycursor.execute(sql, (village_id, i, i))
                militraryList = mycursor.fetchall()
                for i, item in enumerate(militraryList):
                    d = {}
                    if item[1] != None:
                        sql = "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码= %s"
                        mycursor.execute(sql, item[1])
                        parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                        d["category1"] = parent
                        d["category2"] = item[2]

                    else:
                        d["category1"] = item[2]
                        d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        sql = "SELECT m.startYear_开始年, m.endYear_结束年 FROM  military_军事 as m WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
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
                        sql = "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                            m.endYear_结束年, data_数据, mu.name_名称\
                         FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
                         JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
                         WHERE gazetteerId_村志代码=%s AND m.startYear_开始年=%s AND m.endYear_结束年=%s AND data_数据 IS NOT NULL"
                        mycursor.execute(sql, (village_id, start, end))
                        militraryList = mycursor.fetchall()
                        for i, item in enumerate(militraryList):
                            d = {}
                            if item[1] != None:
                                sql = "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码= %s"
                                mycursor.execute(sql, item[1])
                                parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                                d["category1"] = parent
                                d["category2"] = item[2]

                            else:
                                d["category1"] = item[2]
                                d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["villageId"] = village_id_12

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)


        else:
            result_dict["year_range"].append([start_year, end_year])
            sql = "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
                m.endYear_结束年, data_数据, mu.name_名称\
             FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
             JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
             WHERE gazetteerId_村志代码= %s AND m.startYear_开始年=%s AND m.endYear_结束年= %s AND data_数据 IS NOT NULL"
            mycursor.execute(sql, (village_id, start_year, end_year))

            militraryList = mycursor.fetchall()
            for i, item in enumerate(militraryList):
                d = {}
                if item[1] != None:
                    sql = "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, item[1])
                    parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                    d["category1"] = parent
                    d["category2"] = item[2]

                else:
                    d["category1"] = item[2]
                    d["category2"] = "null"  # 没有父类代码 说明本身就是父类
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["villageId"] = village_id_12

                d["startYear"] = item[3]
                d["endYear"] = item[4]

                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)


    # when year_range and year are all None we return the list with all year
    if year_range is None and (year is None or year == []):
        sql = "SELECT mc.categoryId_类别代码 as mcid, mc.parentId_父类代码 as pid, mc.name_名称 as name,m.startYear_开始年,\
            m.endYear_结束年, data_数据, mu.name_名称\
         FROM  military_军事 as m JOIN militarycategory_军事类 as mc  ON  m.categoryId_类别代码=mc.categoryId_类别代码\
         JOIN militaryunit_军事单位 as mu on m.unitId_单位代码=mu.unitId_单位代码 \
         WHERE gazetteerId_村志代码= %s AND data_数据 IS NOT NULL"
        mycursor.execute(sql, village_id)
        militraryList = mycursor.fetchall()

        for i, item in enumerate(militraryList):
            d = {}

            if item[1] != None:
                sql = "SELECT name_名称 FROM militarycategory_军事类 WHERE categoryId_类别代码= %s"
                mycursor.execute(sql, item[1])
                parent = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                d["category1"] = parent
                d["category2"] = item[2]

            else:
                d["category1"] = item[2]
                d["category2"] = "null"  # 没有父类代码 说明本身就是父类
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])
            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"military": result_dict})
    return table


def getEduaction(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"education": result_dict})
            return table

    if year != None and year != []:
        sql = "SELECT startYear_开始年 FROM  education_教育 WHERE gazetteerId_村志代码=%s AND startYear_开始年=endYear_结束年"
        mycursor.execute(sql, village_id)

        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        if overlap == []:
            result_dict["year_only_empty"] = year
            return []

        else:
            for i in overlap:
                result_dict["year_only"].append(i)
                sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                        ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                        ON e.unitId_单位代码=eu.unitId_单位代码 \
                                        WHERE e.gazetteerId_村志代码=%s AND e.startYear_开始年=%s AND e.endYear_结束年= %s AND e.data_数据 IS NOT NULL"

                mycursor.execute(sql, (int(village_id), i, i))

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
                    d["villageId"] = village_id_12

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        sql = "SELECT startYear_开始年, endYear_结束年 FROM education_教育 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
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
                        sql = "SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                                       ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                                       ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                       WHERE e.gazetteerId_村志代码= %s AND  e.startYear_开始年=%s AND e.endYear_结束年= %s AND e.data_数据 IS NOT NULL"
                        mycursor.execute(sql, (int(village_id), start, end))

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
                            d["villageId"] = village_id_12

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])
            sql = "SELECT  e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                                                   ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                                                   ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                   WHERE e.gazetteerId_村志代码= %s AND  e.startYear_开始年=%s AND e.endYear_结束年= %s AND e.data_数据 IS NOT NULL"
            mycursor.execute(sql, (int(village_id), start_year, end_year))

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
                d["villageId"] = village_id_12

                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range is None and (year is None or year == []):
        sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年, e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM education_教育 e JOIN educationCategory_教育类 ec \
                                ON e.categoryId_类别代码= ec.categoryId_类别代码 JOIN educationUnit_教育单位 eu \
                                ON e.unitId_单位代码=eu.unitId_单位代码 \
                                WHERE e.gazetteerId_村志代码=%s AND e.data_数据 IS NOT NULL"
        mycursor.execute(sql, int(village_id))

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
            d["villageId"] = village_id_12

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"education": result_dict})

    return table

def getEconomy_ori(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"economy": result_dict})
            return table

    if year != None and year != []:
        sql = "SELECT startYear_开始年 FROM economy_经济 WHERE gazetteerId_村志代码=%s AND startYear_开始年=endYear_结束年"
        mycursor.execute(sql, village_id)
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        print("overlap", overlap)
        if overlap == []:
            result_dict["year_only_empty"] = year

        else:
            for i in overlap:
                print("we now have overlap")
                result_dict["year_only"].append(i)
                sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                  e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                  ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                  ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                  WHERE e.gazetteerId_村志代码 = %s AND e.startYear_开始年=%s AND e.endYear_结束年=%s"
                mycursor.execute(sql(village_id, i, i))
                econmialList = mycursor.fetchall()
                for item in econmialList:
                    d = {}
                    # the category2 also has two upper layer
                    if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                        d["category3"] = item[2]
                        sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                        mycursor.execute(sql, item[1])
                        categoryList = mycursor.fetchone()
                        categoryId, d["category2"] = categoryList[0], categoryList[1]

                        sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s "
                        mycursor.execute(sql, categoryId)

                        d["category1"] = mycursor.fetchone()[0]


                    # the category2 has no upper layer
                    elif item[1] == None:
                        d["category1"] = item[2]
                        d["category3"] = "null"
                        d["category2"] = "null"

                    # the category2 has onw upper layers
                    else:
                        d["category2"] = item[2]
                        sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                        mycursor.execute(sql, item[1])
                        d["category1"] = mycursor.fetchone()[0]
                        d["category3"] = "null"

                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        sql = "SELECT startYear_开始年, endYear_结束年 FROM  economy_经济 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)

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

                        sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                                          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                                          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                                          ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                          WHERE e.gazetteerId_村志代码 = %s AND e.startYear_开始年= %s AND e.endYear_结束年=%s"
                        mycursor.execute(sql, (village_id, start, end))
                        econmialList = mycursor.fetchall()
                        for item in econmialList:
                            d = {}
                            # the category2 also has two upper layer
                            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                                d["category3"] = item[2]
                                sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                                mycursor.execute(sql, item[1])
                                categoryList = mycursor.fetchone()
                                categoryId, d["category2"] = categoryList[0], categoryList[1]

                                sql = ""
                                mycursor.execute(
                                    "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                                        categoryId))
                                d["category1"] = mycursor.fetchone()[0]

                            # the category2 has no upper layer
                            elif item[1] == None:
                                d["category1"] = item[2]
                                d["category3"] = "null"
                                d["category2"] = "null"

                            # the category2 has onw upper layers
                            else:
                                d["category2"] = item[2]
                                mycursor.execute(
                                    "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                                d["category1"] = mycursor.fetchone()[0]
                                d["category3"] = "null"

                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["villageId"] = village_id_12

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])

            sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                           e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                           ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                           ON e.unitId_单位代码=eu.unitId_单位代码 \
                           WHERE e.gazetteerId_村志代码 = %s AND e.startYear_开始年= %s AND e.endYear_结束年= %s"
            mycursor.execute(sql, (village_id, start_year, end_year))
            econmialList = mycursor.fetchall()
            for item in econmialList:
                d = {}
                # the category2 also has two upper layer
                if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                    d["category3"] = item[2]
                    sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, item[1])
                    categoryList = mycursor.fetchone()
                    categoryId, d["category2"] = categoryList[0], categoryList[1]

                    sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, categoryId)
                    d["category1"] = mycursor.fetchone()[0]

                # the category2 has no upper layer
                elif item[1] == None:
                    d["category1"] = item[2]
                    d["category3"] = "null"
                    d["category2"] = "null"

                # the category2 has onw upper layers
                else:
                    d["category2"] = item[2]
                    sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, item[1])
                    d["category1"] = mycursor.fetchone()[0]
                    d["category3"] = "null"

                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["villageId"] = village_id_12

                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range is None and (year is None or year == []):
        sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                  e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                  ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                  ON e.unitId_单位代码=eu.unitId_单位代码 \
                  WHERE e.gazetteerId_村志代码 = %s"
        mycursor.execute(sql, village_id)
        econmialList = mycursor.fetchall()
        for item in econmialList:
            d = {}
            # the category2 also has two upper layer
            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                d["category3"] = item[2]
                sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                mycursor.execute(sql, item[1])
                categoryList = mycursor.fetchone()
                categoryId, d["category2"] = categoryList[0], categoryList[1]

                sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码 = %s"
                mycursor.execute(sql, categoryId)
                d["category1"] = mycursor.fetchone()[0]

            # the category2 has no upper layer
            elif item[1] == None:
                d["category1"] = item[2]
                d["category3"] = "null"
                d["category2"] = "null"

            # the category2 has onw upper layers
            else:
                d["category2"] = item[2]
                sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s "
                mycursor.execute(sql, item[1])
                d["category1"] = mycursor.fetchone()[0]
                d["category3"] = "null"

            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"economy": result_dict})
    return table

def getEconomy(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    transdata = 0
    transunit = 0
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"economy": result_dict})
            return table

    if year != None and year != []:
        sql = "SELECT startYear_开始年 FROM economy_经济 WHERE gazetteerId_村志代码=%s AND startYear_开始年=endYear_结束年"
        mycursor.execute(sql, village_id)
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        print("overlap",overlap)
        if overlap == []:
            result_dict["year_only_empty"] = year
            return []



        else:
            for i in overlap:
                print("we now have overlap")
                result_dict["year_only"].append(i)

                sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                          ON e.unitId_单位代码=eu.unitId_单位代码 \
                                          WHERE e.gazetteerId_村志代码 = %s AND e.startYear_开始年=%s AND e.endYear_结束年=%s AND e.data_数据 IS NOT NULL"
                mycursor.execute(sql(village_id, i, i))
                econmialList = mycursor.fetchall()
                for item in econmialList:
                    d = {}
                    # the category2 also has two upper layer
                    if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                        d["category3"] = item[2]
                        sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                        mycursor.execute(sql, item[1])
                        categoryList = mycursor.fetchone()
                        categoryId, d["category2"] = categoryList[0], categoryList[1]

                        sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s "
                        mycursor.execute(sql, categoryId)

                        d["category1"] = mycursor.fetchone()[0]


                    # the category2 has no upper layer
                    elif item[1] == None:
                        d["category1"] = item[2]
                        d["category3"] = "null"
                        d["category2"] = "null"

                    # the category2 has onw upper layers
                    else:
                        d["category2"] = item[2]
                        sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                        mycursor.execute(sql, item[1])
                        d["category1"] = mycursor.fetchone()[0]
                        d["category3"] = "null"

                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    if d["category1"] == "总产值 Gross Output Value":
                        if item[6] == "万元 10K yuan":
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == "元 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.0001
                            if item[6] == "百元 100 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.01
                            if item[6] == "千万元 10 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == "亿元 100 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '集体经济收入 Collective Economic Income':
                        if item[6] == "万元 10K yuan":
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == "元 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.0001
                            if item[6] == "百元 100 yuan":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 0.01
                            if item[6] == "千万元 10 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == "亿元 100 millions":
                                transunit = "万元 10K yuan"
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '耕地面积 Cultivated Area':
                        if item[6] == "亩 mu":
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '公顷 hectares':
                                transunit = "亩 mu"
                                transdata = item[5]
                                transdata = transdata * 2.470912
                            if item[6] == '平方米 square meters':
                                transunit = "亩 mu"
                                transdata = item[5]
                                transdata = transdata * 0.0002470912
                    if d["category1"] == '粮食总产量 Total Grain Output':
                        if item[6] == '公斤 kilograms':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                        else:
                            if item[6] == '斤 jin':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 0.5
                            if item[6] == '千斤 1K jin':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 1000 * 0.5
                            if item[6] == '万斤 10K jin':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 10000 * 0.5
                            if item[6] == '百公斤 100 kilograms':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 100
                            if item[6] == '万公斤 10K kilograms':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 10000
                            if item[6] == '吨 tons':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == '万吨 10K tons':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 10000000
                            if item[6] == '担 dan':
                                transunit = '公斤 kilograms'
                                transdata = item[5]
                                transdata = transdata * 100
                    if d["category1"] == '用电量 Electricity Consumption':
                        if item[6] == '千瓦时/度 kilowatt hours':
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '千度 1K kilowatt hours':
                                transunit = '千瓦时/度 kilowatt hours'
                                transdata = item[5]
                                transdata = transdata * 1000
                            if item[6] == '万千瓦时/万度 10K kilowatt hours':
                                transunit = '千瓦时/度 kilowatt hours'
                                transdata = item[5]
                                transdata = transdata * 10000
                            if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                                transunit = '千瓦时/度 kilowatt hours'
                                transdata = item[5]
                                transdata = transdata * 1000000
                    if d["category1"] == '电价 Electricity Price':
                        if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                                transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                                transdata = item[5]
                                transdata = transdata * 0.001
                    if d["category1"] == '用水量 Water Consumption':
                        if item[6] == '立方米 cubic meters':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                        else:
                            if item[6] == '万立方米 10K cubic meters':
                                transunit = '立方米 cubic meters'
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '水价 Water Price':
                        if item[6] == '元每立方米 yuan per cubic meter':
                            transunit = item[6]
                            transdata = item[5]
                            pass
                        else:
                            if item[6] == '元每吨 yuan per ton':
                                transunit = '元每立方米 yuan per cubic meter'
                                transdata = item[5]
                            if item[6] == '元每亩 yuan per mu':
                                transunit = item[6]
                                transdata = item[5]
                    if d["category1"] == '人均收入 Per Capita Income':
                        if item[6] == '元 yuan':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                        else:
                            if item[6] == '万元 10K yuan':
                                transunit = '元 yuan'
                                transdata = item[5]
                                transdata = transdata * 10000
                    if d["category1"] == '人均居住面积 Per Capita Living Space':
                        if item[6] == '平方米 square meters':
                            transdata = item[5]
                            transunit = item[6]
                            pass
                    #d["data"] = item[5]
                    #d["unit"] = item[6]
                    d["data"] = transdata
                    d["unit"] = transunit

                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        sql = "SELECT startYear_开始年, endYear_结束年 FROM  economy_经济 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)

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

                        sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                                                                  e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                                                                  ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                                                                  ON e.unitId_单位代码=eu.unitId_单位代码 \
                                                                  WHERE e.gazetteerId_村志代码 = %s AND e.startYear_开始年= %s AND e.endYear_结束年=%s AND e.data_数据 IS NOT NULL"
                        mycursor.execute(sql, (village_id, start, end))
                        econmialList = mycursor.fetchall()
                        for item in econmialList:
                            d = {}
                            # the category2 also has two upper layer
                            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                                d["category3"] = item[2]
                                sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                                mycursor.execute(sql, item[1])
                                categoryList = mycursor.fetchone()
                                categoryId, d["category2"] = categoryList[0], categoryList[1]

                                sql = ""
                                mycursor.execute(
                                    "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(
                                        categoryId))
                                d["category1"] = mycursor.fetchone()[0]

                            # the category2 has no upper layer
                            elif item[1] == None:
                                d["category1"] = item[2]
                                d["category3"] = "null"
                                d["category2"] = "null"

                            # the category2 has onw upper layers
                            else:
                                d["category2"] = item[2]
                                mycursor.execute(
                                    "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码={}".format(item[1]))
                                d["category1"] = mycursor.fetchone()[0]
                                d["category3"] = "null"

                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["villageId"] = village_id_12

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            if d["category1"] == "总产值 Gross Output Value":
                                if item[6] == "万元 10K yuan":
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == "元 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.0001
                                    if item[6] == "百元 100 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.01
                                    if item[6] == "千万元 10 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == "亿元 100 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '集体经济收入 Collective Economic Income':
                                if item[6] == "万元 10K yuan":
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == "元 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.0001
                                    if item[6] == "百元 100 yuan":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 0.01
                                    if item[6] == "千万元 10 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == "亿元 100 millions":
                                        transunit = "万元 10K yuan"
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '耕地面积 Cultivated Area':
                                if item[6] == "亩 mu":
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '公顷 hectares':
                                        transunit = "亩 mu"
                                        transdata = item[5]
                                        transdata = transdata * 2.470912
                                    if item[6] == '平方米 square meters':
                                        transunit = "亩 mu"
                                        transdata = item[5]
                                        transdata = transdata * 0.0002470912
                            if d["category1"] == '粮食总产量 Total Grain Output':
                                if item[6] == '公斤 kilograms':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                                else:
                                    if item[6] == '斤 jin':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 0.5
                                    if item[6] == '千斤 1K jin':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 1000 * 0.5
                                    if item[6] == '万斤 10K jin':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 10000 * 0.5
                                    if item[6] == '百公斤 100 kilograms':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 100
                                    if item[6] == '万公斤 10K kilograms':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                                    if item[6] == '吨 tons':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == '万吨 10K tons':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 10000000
                                    if item[6] == '担 dan':
                                        transunit = '公斤 kilograms'
                                        transdata = item[5]
                                        transdata = transdata * 100
                            if d["category1"] == '用电量 Electricity Consumption':
                                if item[6] == '千瓦时/度 kilowatt hours':
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '千度 1K kilowatt hours':
                                        transunit = '千瓦时/度 kilowatt hours'
                                        transdata = item[5]
                                        transdata = transdata * 1000
                                    if item[6] == '万千瓦时/万度 10K kilowatt hours':
                                        transunit = '千瓦时/度 kilowatt hours'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                                    if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                                        transunit = '千瓦时/度 kilowatt hours'
                                        transdata = item[5]
                                        transdata = transdata * 1000000
                            if d["category1"] == '电价 Electricity Price':
                                if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                                        transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                                        transdata = item[5]
                                        transdata = transdata * 0.001
                            if d["category1"] == '用水量 Water Consumption':
                                if item[6] == '立方米 cubic meters':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                                else:
                                    if item[6] == '万立方米 10K cubic meters':
                                        transunit = '立方米 cubic meters'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '水价 Water Price':
                                if item[6] == '元每立方米 yuan per cubic meter':
                                    transunit = item[6]
                                    transdata = item[5]
                                    pass
                                else:
                                    if item[6] == '元每吨 yuan per ton':
                                        transunit = '元每立方米 yuan per cubic meter'
                                        transdata = item[5]
                                    if item[6] == '元每亩 yuan per mu':
                                        transunit = item[6]
                                        transdata = item[5]
                            if d["category1"] == '人均收入 Per Capita Income':
                                if item[6] == '元 yuan':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                                else:
                                    if item[6] == '万元 10K yuan':
                                        transunit = '元 yuan'
                                        transdata = item[5]
                                        transdata = transdata * 10000
                            if d["category1"] == '人均居住面积 Per Capita Living Space':
                                if item[6] == '平方米 square meters':
                                    transdata = item[5]
                                    transunit = item[6]
                                    pass
                            # d["data"] = item[5]
                            # d["unit"] = item[6]
                            d["data"] = transdata
                            d["unit"] = transunit

                            table["data"].append(d)

        else:
            result_dict["year_range"].append([start_year, end_year])

            sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
                   e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
                   ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
                   ON e.unitId_单位代码=eu.unitId_单位代码 \
                   WHERE e.gazetteerId_村志代码 = %s AND e.startYear_开始年= %s AND e.endYear_结束年= %s AND e.data_数据 IS NOT NULL"
            mycursor.execute(sql, (village_id, start_year, end_year))
            econmialList = mycursor.fetchall()
            for item in econmialList:
                d = {}
                # the category2 also has two upper layer
                if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                    d["category3"] = item[2]
                    sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, item[1])
                    categoryList = mycursor.fetchone()
                    categoryId, d["category2"] = categoryList[0], categoryList[1]

                    sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, categoryId)
                    d["category1"] = mycursor.fetchone()[0]

                # the category2 has no upper layer
                elif item[1] == None:
                    d["category1"] = item[2]
                    d["category3"] = "null"
                    d["category2"] = "null"

                # the category2 has onw upper layers
                else:
                    d["category2"] = item[2]
                    sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, item[1])
                    d["category1"] = mycursor.fetchone()[0]
                    d["category3"] = "null"

                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["villageId"] = village_id_12

                d["startYear"] = item[3]
                d["endYear"] = item[4]
                if d["category1"] == "总产值 Gross Output Value":
                    if item[6] == "万元 10K yuan":
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == "元 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.0001
                        if item[6] == "百元 100 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.01
                        if item[6] == "千万元 10 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == "亿元 100 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '集体经济收入 Collective Economic Income':
                    if item[6] == "万元 10K yuan":
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == "元 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.0001
                        if item[6] == "百元 100 yuan":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 0.01
                        if item[6] == "千万元 10 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == "亿元 100 millions":
                            transunit = "万元 10K yuan"
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '耕地面积 Cultivated Area':
                    if item[6] == "亩 mu":
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '公顷 hectares':
                            transunit = "亩 mu"
                            transdata = item[5]
                            transdata = transdata * 2.470912
                        if item[6] == '平方米 square meters':
                            transunit = "亩 mu"
                            transdata = item[5]
                            transdata = transdata * 0.0002470912
                if d["category1"] == '粮食总产量 Total Grain Output':
                    if item[6] == '公斤 kilograms':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                    else:
                        if item[6] == '斤 jin':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 0.5
                        if item[6] == '千斤 1K jin':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 1000 * 0.5
                        if item[6] == '万斤 10K jin':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 10000 * 0.5
                        if item[6] == '百公斤 100 kilograms':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 100
                        if item[6] == '万公斤 10K kilograms':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 10000
                        if item[6] == '吨 tons':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == '万吨 10K tons':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 10000000
                        if item[6] == '担 dan':
                            transunit = '公斤 kilograms'
                            transdata = item[5]
                            transdata = transdata * 100
                if d["category1"] == '用电量 Electricity Consumption':
                    if item[6] == '千瓦时/度 kilowatt hours':
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '千度 1K kilowatt hours':
                            transunit = '千瓦时/度 kilowatt hours'
                            transdata = item[5]
                            transdata = transdata * 1000
                        if item[6] == '万千瓦时/万度 10K kilowatt hours':
                            transunit = '千瓦时/度 kilowatt hours'
                            transdata = item[5]
                            transdata = transdata * 10000
                        if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                            transunit = '千瓦时/度 kilowatt hours'
                            transdata = item[5]
                            transdata = transdata * 1000000
                if d["category1"] == '电价 Electricity Price':
                    if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                            transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                            transdata = item[5]
                            transdata = transdata * 0.001
                if d["category1"] == '用水量 Water Consumption':
                    if item[6] == '立方米 cubic meters':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                    else:
                        if item[6] == '万立方米 10K cubic meters':
                            transunit = '立方米 cubic meters'
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '水价 Water Price':
                    if item[6] == '元每立方米 yuan per cubic meter':
                        transunit = item[6]
                        transdata = item[5]
                        pass
                    else:
                        if item[6] == '元每吨 yuan per ton':
                            transunit = '元每立方米 yuan per cubic meter'
                            transdata = item[5]
                        if item[6] == '元每亩 yuan per mu':
                            transunit = item[6]
                            transdata = item[5]
                if d["category1"] == '人均收入 Per Capita Income':
                    if item[6] == '元 yuan':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                    else:
                        if item[6] == '万元 10K yuan':
                            transunit = '元 yuan'
                            transdata = item[5]
                            transdata = transdata * 10000
                if d["category1"] == '人均居住面积 Per Capita Living Space':
                    if item[6] == '平方米 square meters':
                        transdata = item[5]
                        transunit = item[6]
                        pass
                # d["data"] = item[5]
                # d["unit"] = item[6]
                d["data"] = transdata
                d["unit"] = transunit

                table["data"].append(d)

    if year_range is None and (year is None or year == []):
        sql = "SELECT e.categoryId_类别代码 cat1, ec.parentId_父类代码 ca2, ec.name_名称, e.startYear_开始年,\
          e.endYear_结束年,e.data_数据 ,eu.name_名称 FROM economy_经济 e JOIN economyCategory_经济类 ec \
          ON e.categoryId_类别代码=ec.categoryId_类别代码 JOIN economyUnit_经济单位 eu \
          ON e.unitId_单位代码=eu.unitId_单位代码 \
          WHERE e.gazetteerId_村志代码 = %s AND e.data_数据 IS NOT NULL"
        mycursor.execute(sql, village_id)
        econmialList = mycursor.fetchall()
        for item in econmialList:
            d = {}
            # the category2 also has two upper layer
            if item[1] not in [1, 19, 37.38, 39, 40, 41, 47, 56, 62] and item[1] != None:
                d["category3"] = item[2]
                sql = "SELECT parentId_父类代码,name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s"
                mycursor.execute(sql, item[1])
                categoryList = mycursor.fetchone()
                categoryId, d["category2"] = categoryList[0], categoryList[1]

                sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码 = %s"
                mycursor.execute(sql, categoryId)
                d["category1"] = mycursor.fetchone()[0]

            # the category2 has no upper layer
            elif item[1] == None:
                d["category1"] = item[2]
                d["category3"] = "null"
                d["category2"] = "null"

            # the category2 has onw upper layers
            else:
                d["category2"] = item[2]
                sql = "SELECT name_名称 FROM economyCategory_经济类 WHERE categoryId_类别代码= %s "
                mycursor.execute(sql, item[1])
                d["category1"] = mycursor.fetchone()[0]
                d["category3"] = "null"

            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])
            if d["category1"] == "总产值 Gross Output Value":
                if item[6] == "万元 10K yuan":
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == "元 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.0001
                    if item[6] == "百元 100 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.01
                    if item[6] == "千万元 10 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == "亿元 100 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '集体经济收入 Collective Economic Income':
                if item[6] == "万元 10K yuan":
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == "元 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.0001
                    if item[6] == "百元 100 yuan":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 0.01
                    if item[6] == "千万元 10 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == "亿元 100 millions":
                        transunit = "万元 10K yuan"
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '耕地面积 Cultivated Area':
                if item[6] == "亩 mu":
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '公顷 hectares':
                        transunit = "亩 mu"
                        transdata = item[5]
                        transdata = transdata * 2.470912
                    if item[6] == '平方米 square meters':
                        transunit = "亩 mu"
                        transdata = item[5]
                        transdata = transdata * 0.0002470912
            if d["category1"] == '粮食总产量 Total Grain Output':
                if item[6] == '公斤 kilograms':
                    transdata = item[5]
                    transunit = item[6]
                    pass
                else:
                    if item[6] == '斤 jin':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 0.5
                    if item[6] == '千斤 1K jin':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 1000 * 0.5
                    if item[6] == '万斤 10K jin':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 10000 * 0.5
                    if item[6] == '百公斤 100 kilograms':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 100
                    if item[6] == '万公斤 10K kilograms':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 10000
                    if item[6] == '吨 tons':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == '万吨 10K tons':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 10000000
                    if item[6] == '担 dan':
                        transunit = '公斤 kilograms'
                        transdata = item[5]
                        transdata = transdata * 100
            if d["category1"] == '用电量 Electricity Consumption':
                if item[6] == '千瓦时/度 kilowatt hours':
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '千度 1K kilowatt hours':
                        transunit = '千瓦时/度 kilowatt hours'
                        transdata = item[5]
                        transdata = transdata * 1000
                    if item[6] == '万千瓦时/万度 10K kilowatt hours':
                        transunit = '千瓦时/度 kilowatt hours'
                        transdata = item[5]
                        transdata = transdata * 10000
                    if item[6] == '亿千瓦时/亿度 100 million kilowatt hours':
                        transunit = '千瓦时/度 kilowatt hours'
                        transdata = item[5]
                        transdata = transdata * 1000000
            if d["category1"] == '电价 Electricity Price':
                if item[6] == '元每千瓦时/度 yuan per kilowatt-hour':
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '元每千度 yuan per thousand kilowatt-hours':
                        transunit = '元每千瓦时/度 yuan per kilowatt-hour'
                        transdata = item[5]
                        transdata = transdata * 0.001
            if d["category1"] == '用水量 Water Consumption':
                if item[6] == '立方米 cubic meters':
                    transdata = item[5]
                    transunit = item[6]
                    pass
                else:
                    if item[6] == '万立方米 10K cubic meters':
                        transunit = '立方米 cubic meters'
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '水价 Water Price':
                if item[6] == '元每立方米 yuan per cubic meter':
                    transunit = item[6]
                    transdata = item[5]
                    pass
                else:
                    if item[6] == '元每吨 yuan per ton':
                        transunit = '元每立方米 yuan per cubic meter'
                        transdata = item[5]
                    if item[6] == '元每亩 yuan per mu':
                        transunit = item[6]
                        transdata = item[5]
            if d["category1"] == '人均收入 Per Capita Income':
                if item[6] == '元 yuan':
                    transdata = item[5]
                    transunit = item[6]
                    pass
                else:
                    if item[6] == '万元 10K yuan':
                        transunit = '元 yuan'
                        transdata = item[5]
                        transdata = transdata * 10000
            if d["category1"] == '人均居住面积 Per Capita Living Space':
                if item[6] == '平方米 square meters':
                    transdata = item[5]
                    transunit = item[6]
                    pass
            # d["data"] = item[5]
            # d["unit"] = item[6]
            d["data"] = transdata
            d["unit"] = transunit

            table["data"].append(d)

    table["year"].append({"economy": result_dict})
    return table


def getFamilyPlanning(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"familyplanning": result_dict})
            return table

    if year != None and year != []:
        sql = "SELECT startYear_开始年 FROM  familyplanning_计划生育 WHERE gazetteerId_村志代码= %s AND startYear_开始年=endYear_结束年"
        mycursor.execute(sql, village_id)
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        if overlap == []:
            result_dict["year_only_empty"] = year
            return []
        else:
            for i in overlap:


                result_dict["year_only"].append(i)
                sql = "SELECT fc.name_名称, f.startYear_开始年,\
                       f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                       FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                       ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                       ON f.unitId_单位代码=fu.unitId_单位代码 \
                       WHERE f.gazetteerId_村志代码 = %s AND f.startYear_开始年=%s AND f.endYear_结束年=%s AND f.data_数据 IS NOT NULL"
                mycursor.execute(sql, (village_id, i, i))
                familyplanningList = mycursor.fetchall()
                for item in familyplanningList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["category1"] = item[0]
                    d["startYear"] = item[1]
                    d["endYear"] = item[2]
                    d["data"] = item[3]
                    d["unit"] = item[4]
                    table["data"].append(d)
    if year_range != None and len(year_range) == 2:
        sql = "SELECT startYear_开始年, endYear_结束年 FROM familyplanning_计划生育 WHERE gazetteerId_村志代码="
        mycursor.execute(sql, village_id)
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

                        sql = "SELECT  fc.name_名称, f.startYear_开始年,\
                                           f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                           FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                           ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                           ON f.unitId_单位代码=fu.unitId_单位代码 \
                                           WHERE f.gazetteerId_村志代码 = %s AND f.startYear_开始年=%s AND f.endYear_结束年=%s AND f.data_数据 IS NOT NULL"
                        mycursor.execute(sql, (village_id, start, end))
                        familyplanningList = mycursor.fetchall()

                        for item in familyplanningList:
                            d = {}
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["villageId"] = village_id_12

                            d["category1"] = item[0]
                            d["startYear"] = item[1]
                            d["endYear"] = item[2]
                            d["data"] = item[3]
                            d["unit"] = item[4]
                            table["data"].append(d)
        else:
            result_dict["year_range"].append([start_year, end_year])
            sql = "SELECT  fc.name_名称, f.startYear_开始年,\
                                  f.endYear_结束年,f.data_数据 ,fu.name_名称 \
                                  FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
                                  ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
                                  ON f.unitId_单位代码=fu.unitId_单位代码 \
                                  WHERE f.gazetteerId_村志代码 = %s AND f.startYear_开始年=%s AND f.endYear_结束年=%s AND f.data_数据 IS NOT NULL"
            mycursor.execute(sql, (village_id, start_year, end_year))
            familyplanningList = mycursor.fetchall()

            for item in familyplanningList:
                d = {}
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["villageId"] = village_id_12

                d["category1"] = item[0]
                d["startYear"] = item[1]
                d["endYear"] = item[2]
                d["data"] = item[3]
                d["unit"] = item[4]
                table["data"].append(d)
    if year_range is None and (year is None or year == []):
        sql = "SELECT  fc.name_名称, f.startYear_开始年,\
          f.endYear_结束年,f.data_数据 ,fu.name_名称 \
          FROM familyplanning_计划生育 f JOIN familyplanningcategory_计划生育类 fc \
          ON f.categoryId_类别代码= fc.categoryId_类别代码 JOIN familyplanningunit_计划生育单位 fu \
          ON f.unitId_单位代码=fu.unitId_单位代码 \
          WHERE f.gazetteerId_村志代码 =%s AND f.data_数据 IS NOT NULL"
        mycursor.execute(sql, village_id)
        familyplanningList = mycursor.fetchall()
        for item in familyplanningList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["category1"] = item[0]
            d["startYear"] = item[1]
            d["endYear"] = item[2]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[3]
            d["unit"] = item[4]
            table["data"].append(d)

    table["year"].append({"familyplanning": result_dict})
    return table


def getPopulation(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["field"] = ['gazetteerName', 'gazetteerId', 'category1', 'category2', 'startYear', 'endYear', 'data', 'unit']
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []
    # result_dict["year_only_log"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"population": result_dict})
            return table

    if year != None and year != []:
        sql = "SELECT startYear_开始年 FROM population_人口 WHERE gazetteerId_村志代码= %s AND startYear_开始年=endYear_结束年"
        mycursor.execute(sql, village_id)
        same_year = mycursor.fetchall()
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        if overlap == []:
            result_dict["year_only_empty"] = year
            return []
        else:
            for i in overlap:
      
                result_dict["year_only"].append(i)
                sql = "SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                        FROM population_人口 p JOIN populationcategory_人口类 pc \
                                        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                        ON p.unitId_单位代码=pu.unitId_单位代码 \
                                        WHERE p.gazetteerId_村志代码 = %s AND p.startYear_开始年=%s AND p.endYear_结束年=%s AND p.data_数据 IS NOT NULL"
                mycursor.execute(sql, (village_id, i, i))
                populationList = mycursor.fetchall()
                for item in populationList:
                    d = {}
                    if item[1] != None:
                        sql = "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码= %s"
                        mycursor.execute(sql, item[1])
                        d["category1"] = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                        d["category2"] = item[2]
                    else:
                        d["category1"] = item[2]
                        d["category2"] = "null"  # 没有父类代码 说明本身就是父类

                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["startYear"] = item[3]
                    d["endYear"] = item[4]
                    d["data"] = item[5]
                    d["unit"] = item[6]
                    table["data"].append(d)
    if year_range != None and len(year_range) == 2:
        sql = "SELECT startYear_开始年, endYear_结束年 FROM population_人口 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
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

                        sql = "SELECT p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                                                    p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                                                    FROM population_人口 p JOIN populationcategory_人口类 pc \
                                                                    ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                                                    ON p.unitId_单位代码=pu.unitId_单位代码 \
                                                                    WHERE p.gazetteerId_村志代码 =%s AND p.startYear_开始年=%s AND p.endYear_结束年= %s AND p.data_数据 IS NOT NULL"

                        mycursor.execute(sql, (village_id, start, end))
                        populationList = mycursor.fetchall()
                        for item in populationList:
                            d = {}
                            if item[1] != None:
                                sql = "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码= %s"
                                mycursor.execute(sql, item[1])
                                d["category1"] = mycursor.fetchone()[0]
                                d["category2"] = item[2]
                            else:
                                d["category1"] = item[2]
                                d["category2"] = "null"  # 没有父类代码 说明本身就是父类

                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["villageId"] = village_id_12

                            d["startYear"] = item[3]
                            d["endYear"] = item[4]
                            d["data"] = item[5]
                            d["unit"] = item[6]
                            table["data"].append(d)
        else:
            result_dict["year_range"].append([start_year, end_year])
            sql = "SELECT p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
                                                        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
                                                        FROM population_人口 p JOIN populationcategory_人口类 pc \
                                                        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
                                                        ON p.unitId_单位代码=pu.unitId_单位代码 \
                                                        WHERE p.gazetteerId_村志代码 = %s AND p.startYear_开始年=%s AND p.endYear_结束年=%s AND p.data_数据 IS NOT NULL"
            mycursor.execute(sql, (village_id, start_year, end_year))
            populationList = mycursor.fetchall()
            for item in populationList:
                d = {}
                if item[1] != None:
                    sql = "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码= %s"
                    mycursor.execute(sql, item[1])
                    d["category1"] = mycursor.fetchone()[0]  # 通过父类代码获得父类的名字
                    d["category2"] = item[2]
                else:
                    d["category1"] = item[2]
                    d["category2"] = "null"  # 没有父类代码 说明本身就是父类

                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["villageId"] = village_id_12

                d["startYear"] = item[3]
                d["endYear"] = item[4]
                d["data"] = item[5]
                d["unit"] = item[6]
                table["data"].append(d)

    if year_range is None and (year is None or year == []):
        sql = "SELECT  p.categoryId_类别代码 cid, pc.parentId_父类代码 pid,  pc.name_名称, p.startYear_开始年,\
        p.endYear_结束年,p.data_数据 ,pu.name_名称 \
        FROM population_人口 p JOIN populationcategory_人口类 pc \
        ON p.categoryId_类别代码= pc.categoryId_类别代码 JOIN populationunit_人口单位 pu \
        ON p.unitId_单位代码=pu.unitId_单位代码 \
        WHERE p.gazetteerId_村志代码 =%s AND p.data_数据 IS NOT NULL"
        mycursor.execute(sql, village_id)
        populationList = mycursor.fetchall()
        for item in populationList:
            d = {}
            if item[1] != None:
                sql = "SELECT name_名称 FROM populationcategory_人口类 WHERE categoryId_类别代码= %s"
                mycursor.execute(sql, item[1])
                d["category1"] = mycursor.fetchone()[0]
                d["category2"] = item[2]
            else:
                d["category1"] = item[2]
                d["category2"] = "null"  # 没有父类代码 说明本身就是父类

            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["startYear"] = item[3]
            d["endYear"] = item[4]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[5]
            d["unit"] = item[6]
            table["data"].append(d)

    table["year"].append({"population": result_dict})
    return table


def getEthnicgroups(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []

    result_dict["year_range_empty"] = []
    result_dict["year_range"] = []

    if year_range != None and len(year_range) == 2:
        start_year = year_range[0]
        end_year = year_range[1]
    else:
        start_year = end_year = None

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            table["data"] = []
            result_dict["year_range_log"] = "Start year should be smaller than end year!"
            table["year"].append({"ethnicgroups": result_dict})
            return table

    if year != None and year != []:
        sql = "SELECT startYear_开始年 FROM  ethnicGroups_民族 WHERE gazetteerId_村志代码= %s AND startYear_开始年=endYear_结束年"
        mycursor.execute(sql, village_id)
        same_year = mycursor.fetchall()
        if same_year == ():
            return table
        same_years = set()
        for i in same_year:
            same_years.add(i[0])

        same_years = list(same_years)
        overlap = [x for x in same_years if x in year]
        if overlap == []:
            result_dict["year_only_empty"] = year
            return []
        else:
            for i in overlap:

       
                result_dict["year_only"].append(i)
                sql = "SELECT ethc.name_名称, eth.startYear_开始年,\
                                         eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                         FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                         ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                         ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                         WHERE eth.gazetteerId_村志代码 =%s AND eth.startYear_开始年=%s AND eth.endYear_结束年=%s AND p.data_数据 IS NOT NULL"
                mycursor.execute(sql, (village_id, i, i))
                ethnicgroupList = mycursor.fetchall()
                for item in ethnicgroupList:
                    d = {}
                    d["gazetteerName"] = gazetteerName
                    d["gazetteerId"] = village_id
                    d["villageId"] = village_id_12

                    d["category1"] = item[0]
                    d["startYear"] = item[1]
                    d["endYear"] = item[2]
                    d["data"] = item[3]
                    d["unit"] = item[4]
                    table["data"].append(d)

    if year_range != None and len(year_range) == 2:
        sql = "SELECT startYear_开始年, endYear_结束年 FROM ethnicGroups_民族 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
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
                        sql = "SELECT ethc.name_名称, eth.startYear_开始年,\
                                                                 eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                                                 FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                                                 ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                                                 ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                                                 WHERE eth.gazetteerId_村志代码 =%s AND eth.startYear_开始年=%s AND eth.endYear_结束年= %s AND eth.data_数据 IS NOT NULL"
                        mycursor.execute(sql, (village_id, start, end))
                        ethnicgroupList = mycursor.fetchall()
                        for item in ethnicgroupList:
                            d = {}
                            d["gazetteerName"] = gazetteerName
                            d["gazetteerId"] = village_id
                            d["villageId"] = village_id_12

                            d["category1"] = item[0]
                            d["startYear"] = item[1]
                            d["endYear"] = item[2]
                            d["data"] = item[3]
                            d["unit"] = item[4]
                            table["data"].append(d)
        else:
            result_dict["year_range"].append([start_year, end_year])
            sql = "SELECT ethc.name_名称, eth.startYear_开始年, eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
                                                                             FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
                                                                             ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
                                                                             ON eth.unitId_单位代码=ethu.unitId_单位代码 \
                                                                             WHERE eth.gazetteerId_村志代码 =%s AND eth.startYear_开始年=%s AND eth.endYear_结束年= %s AND eth.data_数据 IS NOT NULL"
            mycursor.execute(sql, (village_id, start_year, end_year))
            ethnicgroupList = mycursor.fetchall()
            for item in ethnicgroupList:
                d = {}
                d["gazetteerName"] = gazetteerName
                d["gazetteerId"] = village_id
                d["villageId"] = village_id_12

                d["category1"] = item[0]
                d["startYear"] = item[1]
                d["endYear"] = item[2]
                d["data"] = item[3]
                d["unit"] = item[4]
                table["data"].append(d)
    if year_range is None and (year is None or year == []):
        sql = "SELECT ethc.name_名称, eth.startYear_开始年,\
          eth.endYear_结束年,eth.data_数据 ,ethu.name_名称 \
          FROM ethnicGroups_民族 eth JOIN ethnicGroupsCategory_民族类 ethc \
          ON eth.categoryId_类别代码= ethc.categoryId_类别代码 JOIN ethnicGroupsUnit_民族单位 ethu \
          ON eth.unitId_单位代码=ethu.unitId_单位代码 \
          WHERE eth.gazetteerId_村志代码 = %s AND eth.data_数据 IS NOT NULL"
        mycursor.execute(sql, village_id)

        ethnicgroupList = mycursor.fetchall()
        for item in ethnicgroupList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["category1"] = item[0]
            d["startYear"] = item[1]
            d["endYear"] = item[2]
            if d["startYear"] == d["endYear"] and d["startYear"] not in result_dict["year_only"]:
                result_dict["year_only"].append(d["startYear"])
            elif [d["startYear"], d["endYear"]] not in result_dict["year_range"]:
                result_dict["year_range"].append([d["startYear"], d["endYear"]])

            d["data"] = item[3]
            d["unit"] = item[4]
            table["data"].append(d)

    table["year"].append({"ethnicgroups": result_dict})
    return table


def getFourthlastName(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    sql = "SELECT firstlastNamesId_姓氏代码, secondlastNamesId_姓氏代码, thirdlastNamesId_姓氏代码, \
        fourthlastNamesId_姓氏代码, fifthlastNamesId_姓氏代码, totalNumberofLastNamesinVillage_姓氏总数 \
        FROM lastName_姓氏 WHERE gazetteerId_村志代码= %s "
    mycursor.execute(sql, village_id)
    nameList = mycursor.fetchall()

    l = []
    if len(nameList) == 0:
        print("This village id is not have fourth last name")
        return table
    for z in range(len(nameList[0]) - 1):
        if nameList[0][z] == None:
            l.append("")
        else:
            sql = "SELECT nameChineseCharacters_姓氏汉字 FROM lastNameCategory_姓氏类别 WHERE categoryId_类别代码 = %s"
            mycursor.execute(sql, nameList[0][z])
            l.append(mycursor.fetchone()[0])

    d = {}
    d["gazetteerName"] = gazetteerName
    d["gazetteerId"] = village_id
    d["villageId"] = village_id_12

    d["firstLastNameId"] = l[0]
    d["secondLastNameId"] = l[1]
    d["thirdLastNameId"] = l[2]
    d["fourthLastNameId"] = l[3]
    d["fifthLastNameId"] = l[4]
    d["totalNumberOfLastNameInVillage"] = nameList[0][-1]

    table["data"].append(d)

    return table


def getFirstAvailabilityorPurchase(mycursor, village_id, gazetteerName, year, year_range, village_id_12):
    table = {}
    table["data"] = []
    table["year"] = []
    result_dict = {}
    result_dict["year_only_empty"] = []
    result_dict["year_only"] = []
    result_dict["year range"] = "firstavailabilityorpurchase doesn't have year range option"

    if year != None:
        sql = "SELECT year_年份 FROM firstAvailabilityorPurchase_第一次购买或拥有年份 WHERE gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)
        all_years = mycursor.fetchall()
        all_years = [i[0] for i in all_years]
        for i in year:
            if i not in all_years:
                result_dict["year_only_empty"].append(i)
                if all_years[closest(all_years, i)] not in year:
                    sql = "SELECT f.year_年份,fc.name_名称 FROM firstAvailabilityorPurchase_第一次购买或拥有年份 f JOIN firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类 fc \
             ON f.categoryId_类别代码=fc.categoryId_类别代码 WHERE f.gazetteerId_村志代码= %s"
                    mycursor.execute(sql, village_id)
                    # mycursor.execute(sql,(
                    #         village_id, all_years[closest(all_years, i)]))
                    firstList = mycursor.fetchall()
                    result_dict["year_only"].append(all_years[closest(all_years, i)])
                    for i in firstList:
                        d = {}
                        d["gazetteerName"] = gazetteerName
                        d["gazetteerId"] = village_id
                        d["villageId"] = village_id_12

                        d["year"] = i[0] if i[0] != None else None
                        d["category1"] = i[1] if i[1] != None else None
                        table["data"].append(d)
    else:
        sql = "SELECT f.year_年份,fc.name_名称 FROM firstAvailabilityorPurchase_第一次购买或拥有年份 f JOIN firstAvailabilityorPurchaseCategory_第一次购买或拥有年份类 fc \
             ON f.categoryId_类别代码=fc.categoryId_类别代码 WHERE f.gazetteerId_村志代码= %s"
        mycursor.execute(sql, village_id)

        firstList = mycursor.fetchall()

        for i in firstList:
            d = {}
            d["gazetteerName"] = gazetteerName
            d["gazetteerId"] = village_id
            d["villageId"] = village_id_12

            d["year"] = i[0] if i[0] != None else None
            d["category1"] = i[1] if i[1] != None else None
            table["data"].append(d)

    return table
