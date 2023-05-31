from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request
import requests
from pymongo import ssl_support
from datagovindia import DataGovIndia
import datagovindia
from flask import request
from flask_pymongo import PyMongo
import flask_pymongo

# Scheduling the app


def sensor():
    """ Function for test purposes. """
    url = "http://127.0.0.1:5000/"
    requests.get(url)
    print("Scheduler is alive!")


sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor, 'interval', minutes=10)
sched.start()


app = Flask(__name__)

mongoClient = PyMongo(
    app, uri="URI")
db = mongoClient.db

print(db)
marketCollection = db['marketcollection']
marketList = db["marketList"]

@app.route("/")
def updateData():
    # url = "http://localhost:3000/"
    # requests.get(url)
    try:
        apiKey = "API KEY"
        datagovin = DataGovIndia(
            "API KEY")
        # datagovin.disable_multithreading()

        if datagovin.is_key_valid == True:
            print(datagovin)
            results = [datagovin.get_data("9ef84268-d588-465a-a308-a864a43d0070",
                                        fields=[
                                            "state",
                                            "district",
                                            "market",
                                            "commodity",
                                            "variety",
                                            "arrival_date",
                                            "min_price",
                                            "max_price",
                                            "modal_price"
                                        ], num_results='all').head(8833)]
            # for r in results:
            for i in range(0, 10000):
                for r in results:
                    state = r['state']
                    dist = r["district"]
                    market = r["market"]
                    comm = r["commodity"]
                    vari = r["variety"]
                    arr_date = r["arrival_date"]
                    min_price = r["min_price"]
                    max_price = r["max_price"]
                    mod_price = r["modal_price"]
                    try:
                        update = {
                            "state": state[i],
                            "district": dist[i],
                            "market": market[i],
                            "commodity": comm[i],
                            "variety": vari[i],
                            "arrival_date": arr_date[i],
                            "min_price": min_price[i],
                            "max_price": max_price[i],
                            "modal_price": mod_price[i]
                        }
                        if isFound(update) == True:
                            print(f"{i}th Data present", update)
                        else:
                            print(f"updateing {i}th record.")
                            marketCollection.insert_one(update)
                            print(f"updated {i}th record..")
                    except KeyError:
                        pass
                    # except ConnectionError:
                    #     pass
            print("Market List Update Start")
            marketListData = [datagovin.get_data("9ef84268-d588-465a-a308-a864a43d0070",fields=["state","district","market",], num_results='all').head(8833)]
            for i in range(0, 10000):
                for data in marketListData:
                    state = data['state']
                    dist = data["district"]
                    market = data["market"]
                    try:
                        marketListUpdateData = {"state": state[i], "district": dist[i], "market": market[i]}
                        if isMarketFound(marketListUpdateData) == True:
                            print("Data is prensent", marketListUpdateData)
                        else:
                            print("Updating market list")
                            marketList.insert_one(marketListUpdateData)
                    except KeyError:
                        pass
                    except ConnectionError:
                        pass   
        else:
            url = "http://127.0.0.1:5000/"
            requests.get(url)
    except AttributeError:
        pass
    return "print mesg"




def isFound(findData):
    data = marketCollection.find_one(findData)
    if data == None:
        return False
    elif findData == data:
        print("Found data - ", data)
        return True
    else:
        return True

def isMarketFound(updateMarketListData):
    data = marketList.find_one(updateMarketListData)
    if data == None:
        return False
    else:
        return True