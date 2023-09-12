# < 목표 >
# 소비자가 대청DB에 있는 실시간 업데이트되는 데이터를 api를 통해 보고자함
# -> 대청DB의 실시간데이터를 올리는 api를 만들고 열어놔서 소비자가 접근하여 그 데이터를 볼 수 있게 하기

# < 파이프라인 >
# psql(DB)에서 대청의 daecheong_water_quality_real 테이블의 실시간으로 업데이트되는 맨 윗줄 데이터를 가져와서
# 소비자가 원하는 자료형식과 컬럼명으로 자료를 변환하고, 10초마다 한번씩 api에 배포할 데이터를 업데이트한다.
# flask라이브러리로 api를 만들고 host권한,내부포트,토픽메시지 등을 지정 후,
# Portforwarding으로 지정한 내부포트에 대한 외부포트포워딩을 추가한다.

# < 접근법 >
# http://외부아이피:외부포트/water_quality 로 접근 

from flask import Flask, jsonify
from flask_restful import Resource, Api
import psycopg2
import threading
import time


app = Flask(__name__) # Flask 어플리케이션을 초기화 및 생성한다. 즉, 시작점을 알린다.
api = Api(app) # app에 RESTful API 기능을 추가한다. : 추후, api.add_resource()같은 메서드로 설정할 수 있게 기능들을 부여한다.

# psql에서 데이터불러와서 형식변환
def fetch_data():
    conn = psycopg2.connect(
        dbname="daecheong_1",
        user="eco0",
        password="*********",
        host="localhost"
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM daecheong_water_quality_real ORDER BY timestamp DESC LIMIT 1;")
    row = cursor.fetchone()
    
    conn.close()
    
    # row 데이터를 소비자가 요구한 새로운 형식으로 변환
    mapped_data = {
        "MEASURED": row[1].strftime('%Y-%m-%d %H:%M:%S'),  # varchar(30)
        "LATITUDE": str(row[11]),                          # varchar(30)
        "LONGITUDE": str(row[12]),                         # varchar(30)
        "TEMPERATURE": row[2],                             # double
        "DPWT_DEPTH": -0.07,                               # double
        "PH": row[3],                                      # double
        "SP_COND": row[4],                                 # double
        "HDO": row[8],                                     # double
        "TURBIDITY": row[5],                               # double
        "BG": row[6],                                      # double
        "CHL_A": row[7]                                    # double
    }
    
    return mapped_data

# Data Update 
# 계속 db의 데이터가 업데이트되니, api로 배포할 데이터도 업데이트해야한다.
# 주기 : 10초
global_data = {}

def update_data_every_10_seconds():
    global global_data
    while True:
        global_data = fetch_data()
        time.sleep(10)

# 클래스 WaterQuality로 Flask라이브러리의 클래스인 Resource를 상속받는다.
# 이는  HTTP 메서드(예: GET, POST, PUT, DELETE 등)에 응답할 수 있는 기능을 한다.
# 추후, http로 GET을 요청하면 get메서드가 이에 응답하여 json형태의 데이터를 제공한다.
class WaterQuality(Resource):
    def get(self):
        return jsonify(global_data)

# /water_quality : api 접근할때 메시지토픽
api.add_resource(WaterQuality, '/water_quality')
# https주소창에서 /water-quality를 엔드포인트로 접근하면 WaterQuality리소스가 실행 -> 최신의 데이터가 반환된다.
# 외부에서 접근하니까 http://외부ip/외부port/엔드포인트

if __name__ == '__main__':
    thread = threading.Thread(target=update_data_every_10_seconds)
    # update_data_every_10_seconds 함수를 별도의 스레드에서 실행. 이렇게 하면 Flask 앱이 작동하는 동안에도 데이터베이스에서 물질 데이터를 주기적으로 가져올 수 있다.
    thread.start()
    app.run(host='0.0.0.0', debug=True) # flask api 내부포트 디폴트 값 = 5000
    # host = 0.0.0.0 : 모든 아이피에서 접근가능하게 설정
    # host = 127.0.0.1 : 로컬에서만 접근가능하게 설정

    # 내부포트를 직접 지정
    # app.run(host='0.0.0.0', port=8080, debug=True)
   