from flask import Flask, jsonify, request
from flask_cors import CORS
from bhavcopy import BhavCopy
from datetime import datetime

app = Flask(__name__)
CORS(app)

@app.route('/bhavcopy', methods=['POST'])
def bhavcopy():
    data = request.get_json()['date']
    date = datetime.strptime(data, "%Y-%m-%d").strftime("%d%m%Y")
    bhav = BhavCopy(date)
    data = bhav.set_bhav()
    return jsonify({"body": data})

if __name__ == '__main__':
    app.run(debug=True)