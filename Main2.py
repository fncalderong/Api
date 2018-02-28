import os, json, base64, logging
from flask_api import FlaskAPI
from flask import request


# from backends.big_query import BqClient
app = FlaskAPI(__name__)
# os.environ.setdefault('FLASK_APP', 'main.py')
# os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', 'backends/credentials.json')


@app.route('/')
def hello():
    print("asd")
    return 'Aplicativo Flask pub-sub'


@app.route("/pub-sub/", methods=['POST'])
def pob_sub():
    print("asd")
    if request.data.get("message"):
        print(request.data.get("message"))
        # attributes = json.loads(request.data["message"].get("attributes").get("attrs"))
        # print(attributes)
        # data = request.data["message"].get("data")
        # if not attributes:
        #     logging.warning("No attributes supplied")
        #     return "", 400

        return request.data.get("message")
    # if request.data.get("message"):
    #     attributes = json.loads(request.data["message"].get("attributes").get("attrs"))
    #     data = request.data["message"].get("data")
    #     if not attributes:
    #         logging.warning("No attributes supplied")
    #         return "", 400
    #     if not data:
    #         logging.warning("No data supplied")
    #         return "", 400
    #     if not (attributes.get("dataset") and attributes.get("table")):
    #         logging.warning("No dataset or table supplied")
    #         return "", 400
    #
    #     try:
    #         data = base64.b64decode(request.data["message"].get("data"))
    #         data = json.loads(data)
    #         table = attributes.get("table")
    #         if attributes.get("country"):
    #             data_set = "_".join((attributes.get("dataset"), attributes.get("country")))
    #         else:
    #             data_set = attributes.get("dataset")
    #     except Exception as e:
    #         logging.error("Se ha encontrado un error inesperado: %s" % e)
    #         return "", 500
    #     BqClient().insert_row(data_set, table, data)
    # else:
    #     logging.error("No se encontro mensaje pub-sub")
    return "", 400


if __name__ == '__main__':
    app.run(debug=True)
