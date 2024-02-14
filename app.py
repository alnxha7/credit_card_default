import numpy as np
from flask import Flask, request, jsonify, render_template
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pickle
import json

app = Flask(__name__)
model = pickle.load(open('model.pkl', 'rb'))

cloud_config= {
  'secure_connect_bundle': 'secure-connect-credit-card.zip'
}
with open("credit_card-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

keyspace_name = 'collection_1'
session.set_keyspace(keyspace_name)

table_name = 'credit_card_new'

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
    gender INT,
    education INT,
    marrital_status INT,
    age INT,
    limit_balance INT,
    pay_1 INT,
    pay_2 INT,
    pay_3 INT,
    pay_4 INT,
    pay_5 INT,
    pay_6 INT,
    bill_amt1 INT,
    bill_amt2 INT,
    bill_amt3 INT,
    bill_amt4 INT,
    bill_amt5 INT,
    bill_amt6 INT,
    pay_amt1 INT,
    pay_amt2 INT,
    pay_amt3 INT,
    pay_amt4 INT,
    pay_amt5 INT,
    pay_amt6 INT,
    prediction INT,
    PRIMARY KEY (gender, education, marrital_status, age, limit_balance)
)
"""
session.execute(create_table_query)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    '''
    for rendering results on HTML
    '''
    features = [int(x) for x in request.form.values()]

    # re-arranging the list as per the data set
    feature_list = [features[4]] + features[:4] + features[5:11][::-1] + features[11:17][::-1] + features[17:][::-1]
    features_arr = [np.array(feature_list)]

    prediction = model.predict(features_arr)

    print(features_arr)
    default_payment = prediction.tolist()

    query = f"""
    INSERT INTO {table_name} (
    gender, education, marrital_status, age, limit_balance,
    pay_1, pay_2, pay_3, pay_4, pay_5, pay_6,
    bill_amt1, bill_amt2, bill_amt3, bill_amt4, bill_amt5, bill_amt6,
    pay_amt1, pay_amt2, pay_amt3, pay_amt4, pay_amt5, pay_amt6, prediction
)
VALUES (
    :gender, :education, :marrital_status, :age, :limit_balance,
    :pay_1, :pay_2, :pay_3, :pay_4, :pay_5, :pay_6,
    :bill_amt1, :bill_amt2, :bill_amt3, :bill_amt4, :bill_amt5, :bill_amt6,
    :pay_amt1, :pay_amt2, :pay_amt3, :pay_amt4, :pay_amt5, :pay_amt6, :prediction
)
"""

# Ensure that the parameters dictionary includes all required keys
    parameters = {
    'gender': features[0],
    'education': features[1],
    'marrital_status': features[2],
    'age': features[3],
    'limit_balance': features[4],
    'pay_1': features[5],
    'pay_2': features[6],
    'pay_3': features[7],
    'pay_4': features[8],
    'pay_5': features[9],
    'pay_6': features[10],
    'bill_amt1': features[11],
    'bill_amt2': features[12],
    'bill_amt3': features[13],
    'bill_amt4': features[14],
    'bill_amt5': features[15],
    'bill_amt6': features[16],
    'pay_amt1': features[17],
    'pay_amt2': features[18],
    'pay_amt3': features[19],
    'pay_amt4': features[20],
    'pay_amt5': features[21],
    'pay_amt6': features[22],
    'prediction': default_payment[0]
}

    # Bind the parameters and execute the query
    try:
        session.execute(
        f"INSERT INTO {keyspace_name}.{table_name} "
        "(gender, education, marrital_status, age, limit_balance, "
        "pay_1, pay_2, pay_3, pay_4, pay_5, pay_6, "
        "bill_amt1, bill_amt2, bill_amt3, bill_amt4, bill_amt5, bill_amt6, "
        "pay_amt1, pay_amt2, pay_amt3, pay_amt4, pay_amt5, pay_amt6, prediction) "
        "VALUES (%(gender)s, %(education)s, %(marrital_status)s, %(age)s, %(limit_balance)s, "
        "%(pay_1)s, %(pay_2)s, %(pay_3)s, %(pay_4)s, %(pay_5)s, %(pay_6)s, "
        "%(bill_amt1)s, %(bill_amt2)s, %(bill_amt3)s, %(bill_amt4)s, %(bill_amt5)s, %(bill_amt6)s, "
        "%(pay_amt1)s, %(pay_amt2)s, %(pay_amt3)s, %(pay_amt4)s, %(pay_amt5)s, %(pay_amt6)s, %(prediction)s)",
        parameters
    )
        print("Document inserted successfully.")
        # Add success message to return to the client
    except Exception as e:
        print(f"Error inserting document: {e}")

    print("prediction value:", prediction)

    result = ""
    if prediction == 1:
        result = "The credit card holder will be Defaulter in the next month"
    else:
        result = "The Credit card holder will not be Defaulter in the next month"

    return render_template('index.html', prediction_text=result)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)