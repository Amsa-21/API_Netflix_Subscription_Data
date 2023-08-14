import json
from bson.objectid import ObjectId
from urllib.parse import quote_plus
from flask import Flask, request, jsonify
from pymongo.mongo_client import MongoClient

username = quote_plus('amsa')
password = quote_plus('Amsat@12')

uri = "mongodb+srv://" + username + ":" + password + "@firstcluster.mefl5rg.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri)

app = Flask(__name__)

db = client["netflix_db"]
collection = db["users"]

@app.route('/api/all', methods=['GET'])
def get_subscriptions():
    result = collection.find()
    if result:
        documents = list(result)
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify({"success": True, "message": None, "result": [{"count": len(documents)}, documents]})
    else:
        return jsonify({"success": False, "message": "Empty collection", "result": None})
    

@app.route('/api/add', methods=['POST'])
def add_subscription():
    data = json.loads(request.data)
    subscriptionType = data["Subscription Type"]
    monthlyRevenue = int(data["Monthly Revenue"])
    joinDate = data["Join Date"]
    lastPaymentDate = data["Last Payment Date"]
    country = data["Country"]
    age = int(data["Age"])
    gender = data["Gender"]
    device = data["Device"]
    planDuration = data["Plan Duration"]
    user = collection.find_one({"Subscription Type": subscriptionType, "Monthly Revenue": monthlyRevenue,
                                "Join Date": joinDate, "Last Payment Date": lastPaymentDate,
                                "Country": country, "Age": age, "Gender": gender,
                                "Device": device, "Plan Duration": planDuration})
    if user:
        return jsonify({"success": False, "message": "User already exist", "result": None})
    else:
        user_id = collection.insert_one(data).inserted_id
        return jsonify({"success": True, "message": "User add successfully", "result": {"id": str(user_id)}})
    

@app.route('/api/update/<id>', methods=['PUT'])
def update_subscription(id):
    obj_id = ObjectId(id)
    data = json.loads(request.data)
    updated_user = collection.update_one({"_id": obj_id}, {"$set": data})
    if updated_user.modified_count > 0:
        return jsonify({"success": True, "message": "User updated successfully", "result": None})
    else:
        return jsonify({"success": False, "message": "Error while updating", "result": None})


@app.route('/api/delete/<id>', methods=['DELETE'])
def delete_subscription(id):
    obj_id = ObjectId(id)
    deleted_user = collection.delete_one({"_id": obj_id})
    if deleted_user.deleted_count > 0:
        return jsonify({"success": True, "message": "User deleted successfully", "result": None})
    else:
        return jsonify({"success": False, "message": "Error while deleting", "result": None})
    

@app.route('/api/getById/<id>', methods=['GET'])
def get_subscription(id):
    obj_id = ObjectId(id)
    user = collection.find_one({"_id": obj_id})
    if user:
        user["_id"] = str(user["_id"])
        return jsonify({"success": True, "message": "User found", "result": [{"count": 1}, user]})
    else:
        return jsonify({"success": False, "message": "User not found", "result": None})
    

@app.route('/api/findByDevice/<deviceName>', methods=['GET'])
def get_by_device(deviceName):
    result = collection.find({"Device": deviceName})
    documents = list(result)
    if len(documents) > 0:
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify({"success": True, "message": None, "result": [{"count": len(documents)}, documents]})
    else:
        return jsonify({"success": False, "message": "Empty", "result": None})
    

@app.route('/api/findByAge/<age>', methods=['GET'])
def get_by_age(age):
    age = int(age)
    result = collection.find({"Age": age})
    documents = list(result)
    if len(documents) > 0:
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify({"success": True, "message": None, "result": [{"count": len(documents)}, documents]})
    else:
        return jsonify({"success": False, "message": "Empty", "result": None})
        
        
@app.route('/api/findByCountry/<countryName>', methods=['GET'])
def get_by_country(countryName):
    result = collection.find({"Country": countryName})
    documents = list(result)
    if len(documents) > 0:
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify({"success": True, "message": None, "result": [{"count": len(documents)}, documents]})
    else:
        return jsonify({"success": False, "message": "Empty", "result": None})
    

@app.route('/api/findBySubscriptionType/<Subscription_Type>', methods=['GET'])
def get_by_subscriptionType(Subscription_Type):
    result = collection.find({"Subscription Type": Subscription_Type})
    documents = list(result)
    if len(documents) > 0:    
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify({"success": True, "message": None, "result": [{"count": len(documents)}, documents]})
    else:
        return jsonify({"success": False, "message": "Empty", "result": None})
    

@app.route('/api/averageRevenue', methods=['GET'])
def revenu_moyen():
    pipeline = [
        {"$group": {"_id": None, "averageRevenue": {"$avg": "$Monthly Revenue"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Average revenue per subscriber", "result": result})


@app.route('/api/abonnementsParType', methods=['GET'])
def abonnements_par_type():
    pipeline = [
        {"$group": {"_id": "$Subscription Type", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Distribution of subscribers by subscription type", "result": [{"count": len(result)}, result]})


@app.route('/api/ageMoyenParPays', methods=['GET'])
def ageMoyen_par_pays():
    pipeline = [
        {"$group": {"_id": "$Country", "averageAge": {"$avg": "$Age"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Average age of subscribers by country", "result": [{"count": len(result)}, result]})


@app.route('/api/visionnageDispositif', methods=['GET'])
def preferences_visionnage_par_dispositif():
    pipeline = [
        {"$group": {"_id": "$Device", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Distribution of subscribers by device", "result": [{"count": len(result)}, result]})


@app.route('/api/repartitionParGenre', methods=['GET'])
def repartition_abonnements_par_genre():
    pipeline = [
        {"$group": {"_id": "$Gender", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Distribution of subscribers by gender", "result": [{"count": len(result)}, result]})


@app.route('/api/revenusMensuelGenre', methods=['GET'])
def revenus_mensuels_par_genre():
    pipeline = [
        {"$group": {"_id": "$Gender", "totalRevenue": {"$sum": {"$multiply": ["$Monthly Revenue", 1]}}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Monthly revenue by gender", "result": [{"count": len(result)}, result]})


@app.route('/api/revenusMensuelsPays', methods=['GET'])
def revenus_mensuels_par_pays():
    pipeline = [
        {"$group": {"_id": "$Country", "totalRevenue": {"$sum": {"$multiply": ["$Monthly Revenue", 1]}}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Monthly revenue by country", "result": [{"count": len(result)}, result]})


@app.route('/api/revenusMensuelsTotal', methods=['GET'])
def revenus_mensuels_totals():
    pipeline = [
        {"$group": { "_id": None, "totalRevenue": {"$sum": {"$multiply": ["$Monthly Revenue", 1]}}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Total monthly revenue", "result": result})


@app.route('/api/revenusMensuelsSub', methods=['GET'])
def revenu_par_subscriptionType():
    pipeline = [
        {"$group": { "_id": "$Subscription Type", "revenue": {"$sum": "$Monthly Revenue"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Monthly revenue by subscription type", "result": result})


@app.route('/api/subTypeCountry', methods=['GET'])
def subscriptionType_par_pays():
    pipeline = [
        {"$group": {"_id": {"Country": "$Country", "SubscriptionType": "$Subscription Type"}, "TotalSubscriptions": {"$sum": 1 }}},
        {"$project": {"_id": 0, "Country": "$_id.Country", "SubscriptionType": "$_id.SubscriptionType", "TotalSubscriptions": 1}},
        {"$sort": {"Country": 1, "SubscriptionType": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Distribution of subscription by country and subscription type", "result": result})
    

@app.route('/api/deviceGender', methods=['GET'])
def device_par_genre():
    pipeline = [
        {"$group": {"_id": {"Device": "$Device", "Gender": "$Gender"}, "count": {"$sum": 1}}},
        {"$group": {"_id": "$_id.Device", "distribution": {"$push": {"Gender": "$_id.Gender", "count": "$count"}}}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify({"success": True, "message": "Distribution of subscription by gender and device", "result": result})


if __name__ == '__main__':
    app.run(port=8080, debug=True)