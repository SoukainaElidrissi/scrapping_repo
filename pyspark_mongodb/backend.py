from flask import Flask, jsonify
from flask_cors import CORS
import csv
app = Flask(__name__)
CORS(app)

@app.route("/data")
def get_csv_data():
    data = []
    with open("C:\\Users\\DELL\\Desktop\\pyspark_mongodb\\country_per_year.csv", "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            row = {key: int(value) if value.isdigit() else value for key, value in row.items()}
            data.append(row)
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)